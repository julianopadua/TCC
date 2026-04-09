# src/feature_engineering_temporal.py
# =============================================================================
# TEMPORAL FUSION FEATURE ENGINEERING
# Generates tsf_* columns (EWMA/lags, ARIMA, SARIMA, ARIMAX, SARIMAX_exog,
# Prophet, MiniROCKET, TSKMeans) for bases D/E/F calculated.
#
# Two-layer evaluation:
#   Layer A: MAE/MSE/R² of the temporal model on the continuous series z.
#   Layer B: improvement in HAS_FOCO classification (measured later in
#            train_runner via PR-AUC etc.).
#
# Multivariable rationale:
#   HAS_FOCO responds to a combination of conditions (precipitation, humidity,
#   temperature, radiation). ARIMA/SARIMA run per variable (univariate),
#   while ARIMAX/SARIMAX_exog condition one endogenous variable on the others.
#   For multi-step forecasting with exogenous variables, the last observed
#   exog row is repeated into the future horizon H (documented operacional
#   convention — not a physical forecast, just a feature engineering device).
#
# Output layouts:
#   split  (default): one folder per method under paths.data.temporal_fusion,
#          e.g. data/temporal_fusion/base_D_.../ewma_lags/
#          Registered in config.yaml as tf_D_ewma_lags etc.
#   merged (legacy):  all methods merged into a single parquet under
#          paths.data.modeling/{folder}_tsfusion (backward compatible).
#
# Reference: Balduino & Valente, "Implementation of IoT Data Fusion
# Architectures for Precipitation Forecasting", Preprints 2025.
# =============================================================================

import sys
import gc
import json
import time
import warnings
import argparse
from collections import Counter
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

warnings.filterwarnings("ignore", category=FutureWarning)

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

try:
    import src.utils as utils
except ImportError:
    print("[ERRO] Falha ao importar src.utils")
    sys.exit(1)

# Memory monitoring (psutil optional)
try:
    from src.ml.core import MemoryMonitor  # type: ignore
except Exception:
    class MemoryMonitor:  # type: ignore
        @staticmethod
        def get_usage() -> str:
            try:
                import psutil, os
                p = psutil.Process(os.getpid())
                vm = psutil.virtual_memory()
                rss_gb = p.memory_info().rss / (1024 ** 3)
                avail_gb = vm.available / (1024 ** 3)
                return f"rss={rss_gb:.2f}GB avail={avail_gb:.2f}GB used={vm.percent}%"
            except Exception:
                return "psutil_not_installed"

        @staticmethod
        def log_usage(log, ctx: str = "") -> None:
            log.info(f"[MEMORIA] {ctx}: {MemoryMonitor.get_usage()}")

# ---------------------------------------------------------------------------
# Column constants (must match feature_engineering_physics.py / parquets)
# ---------------------------------------------------------------------------
COL_PRECIP  = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
COL_TEMP    = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
COL_UMID    = "UMIDADE RELATIVA DO AR, HORARIA (%)"
COL_VENTO   = "VENTO, VELOCIDADE HORARIA (m/s)"
COL_PRESSAO = "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
COL_RAD     = "RADIACAO GLOBAL (KJ/m²)"

Z_PRIMARY = COL_PRECIP  # official series z for Layer A

# ---------------------------------------------------------------------------
# ARIMA-family variable registry
#   slug  : short identifier used in column names (tsf_arima_{slug}_pred)
#   col   : actual DataFrame column name
# Add more entries here to extend coverage; CLI --arima-vars filters by slug.
# ---------------------------------------------------------------------------
ARIMA_VARS_ALL: Dict[str, str] = {
    "precip": COL_PRECIP,
    "temp":   COL_TEMP,
    "umid":   COL_UMID,
    "rad":    COL_RAD,
}
ARIMA_VARS_DEFAULT: List[str] = list(ARIMA_VARS_ALL.keys())

ALL_METHODS: Set[str] = {
    "ewma_lags", "arima", "sarima", "arimax", "sarimax_exog",
    "prophet", "minirocket", "tskmeans",
}

# Scenarios to enrich (D, E and F calculated by default)
DEFAULT_SCENARIOS: Dict[str, str] = {
    "base_D_calculated": "base_D_with_rad_drop_rows_calculated",
    "base_E_calculated": "base_E_with_rad_knn_calculated",
    "base_F_calculated": "base_F_full_original_calculated",
}

# Primeiras N falhas por método/ano: WARNING com mensagem completa da exceção
# (logging em INFO não exibe linhas DEBUG). A primeira falha inclui traceback.
TSF_FAIL_DETAIL_LOG_CAP = 25

# ---------------------------------------------------------------------------
# Optional heavy imports (guarded so the script loads even without them)
# ---------------------------------------------------------------------------
statsmodels_available = False
try:
    from statsmodels.tsa.arima.model import ARIMA as SM_ARIMA
    from statsmodels.tsa.statespace.sarimax import SARIMAX as SM_SARIMAX
    statsmodels_available = True
except ImportError:
    pass

prophet_available = False
try:
    from prophet import Prophet as FBProphet
    prophet_available = True
except ImportError:
    pass

minirocket_available = False
try:
    from aeon.transformations.collection.convolution_based import MiniRocket
    minirocket_available = True
except ImportError:
    pass

tskmeans_available = False
try:
    from tslearn.clustering import TimeSeriesKMeans
    tskmeans_available = True
except ImportError:
    pass


# ============================================================================
# Layer A metrics helper
# ============================================================================
class LayerATracker:
    """Accumulates per-city, per-method, per-target (y_true, y_pred) and
    computes MAE / MSE / R² for the temporal model on the continuous series z.

    The ``target`` field identifies the variable slug (e.g. 'precip', 'temp')
    so metrics from different variables are never mixed in the same aggregate.

    The ``is_train`` flag lets us export a training-only summary that can be
    used to rank methods without leaking information from test years.
    """

    def __init__(self):
        self.records: List[Dict] = []

    def add(
        self,
        method: str,
        target: str,
        city: str,
        year: int,
        y_true: np.ndarray,
        y_pred: np.ndarray,
        is_train: bool = False,
    ) -> None:
        mask = np.isfinite(y_true) & np.isfinite(y_pred)
        yt, yp = y_true[mask], y_pred[mask]
        if len(yt) == 0:
            return
        mae = float(np.mean(np.abs(yt - yp)))
        mse = float(np.mean((yt - yp) ** 2))
        ss_res = float(np.sum((yt - yp) ** 2))
        ss_tot = float(np.sum((yt - np.mean(yt)) ** 2))
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else float("nan")
        self.records.append({
            "method": method,
            "target": target,
            "city": city,
            "year": year,
            "n": int(len(yt)),
            "mae": mae,
            "mse": mse,
            "r2": r2,
            "is_train": bool(is_train),
        })

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["method", "target", "year"])
            .agg(
                n_cities=("city", "nunique"),
                n_total=("n", "sum"),
                mae_mean=("mae", "mean"),
                mse_mean=("mse", "mean"),
                r2_mean=("r2", "mean"),
            )
            .reset_index()
        )

    def summary_df(self) -> pd.DataFrame:
        if not self.records:
            return pd.DataFrame()
        return self._aggregate(pd.DataFrame(self.records))

    def summary_train_df(self) -> pd.DataFrame:
        """Aggregate only over training years (is_train=True)."""
        if not self.records:
            return pd.DataFrame()
        df = pd.DataFrame(self.records)
        train = df[df["is_train"] == True]  # noqa: E712
        if train.empty:
            return pd.DataFrame()
        return self._aggregate(train)

    def method_ranking_df(self) -> pd.DataFrame:
        """One row per (method, target) ranked by mae_mean ascending (train only)."""
        summ = self.summary_train_df()
        if summ.empty:
            return pd.DataFrame()
        return (
            summ.groupby(["method", "target"])
            .agg(
                n_cities_total=("n_cities", "sum"),
                n_obs_total=("n_total", "sum"),
                mae_mean=("mae_mean", "mean"),
                mse_mean=("mse_mean", "mean"),
                r2_mean=("r2_mean", "mean"),
            )
            .reset_index()
            .sort_values(["target", "mae_mean"])
            .reset_index(drop=True)
        )

    def save(self, path: Path) -> None:
        utils.ensure_dir(path.parent)
        detail = pd.DataFrame(self.records)
        if len(detail):
            detail.to_csv(path.parent / "layer_a_detail.csv", index=False)

        summary = self.summary_df()
        if len(summary):
            summary.to_csv(path, index=False)

        summary_train = self.summary_train_df()
        if len(summary_train):
            summary_train.to_csv(path.parent / "layer_a_summary_train.csv", index=False)

        ranking = self.method_ranking_df()
        if len(ranking):
            ranking.to_csv(path.parent / "method_ranking_train.csv", index=False)


# ============================================================================
# Main class
# ============================================================================
class TemporalFusionEngineer:

    def __init__(
        self,
        scenarios: Optional[Dict[str, str]] = None,
        methods: Optional[Set[str]] = None,
        refit_hours: int = 168,
        window_hours: int = 720,
        sarima_m: int = 24,
        arima_order: Tuple[int, int, int] = (2, 1, 2),
        arima_vars: Optional[List[str]] = None,
        arimax_endog: str = "precip",
        minirocket_window: int = 24,
        minirocket_kernels: int = 84,
        tskmeans_k: int = 8,
        tskmeans_period: int = 168,
        test_size_years: int = 2,
        overwrite: bool = False,
        filter_years: Optional[List[int]] = None,
        output_layout: str = "split",
    ):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger(
            "feature_eng.temporal", kind="temporal", per_run_file=True
        )
        self.modeling_dir       = Path(self.cfg["paths"]["data"]["modeling"])
        self.temporal_fusion_dir = Path(self.cfg["paths"]["data"]["temporal_fusion"])

        self.scenarios   = scenarios or DEFAULT_SCENARIOS
        self.methods     = methods if methods is not None else ALL_METHODS
        self.refit_hours = refit_hours
        self.window_hours = window_hours
        self.sarima_m    = sarima_m
        self.arima_order = arima_order
        self.arima_vars  = arima_vars if arima_vars is not None else ARIMA_VARS_DEFAULT
        self.arimax_endog = arimax_endog

        self.minirocket_window   = minirocket_window
        self.minirocket_kernels  = minirocket_kernels
        self.tskmeans_k          = tskmeans_k
        self.tskmeans_period     = tskmeans_period
        self.test_size_years     = test_size_years
        self.overwrite           = overwrite
        self.filter_years        = filter_years
        self.output_layout       = output_layout.lower().strip()

        if self.output_layout not in ("split", "merged"):
            self.log.warning(
                f"[INIT] output_layout='{output_layout}' desconhecido; usando 'split'"
            )
            self.output_layout = "split"

        self.layer_a = LayerATracker()

        # Fitted transform objects keyed by (scenario_key, method)
        self._minirocket_models: Dict[str, object] = {}
        self._tskmeans_models:   Dict[str, object] = {}

        # EDA output directory for Layer A and run metrics
        eda_dir = Path(self.cfg["paths"]["data"].get("dataset", "data/dataset"))
        self._eda_tsf_dir    = eda_dir.parent / "eda" / "temporal_fusion"
        self._run_metrics_path = self._eda_tsf_dir / "tsf_run_metrics.csv"

        # Orçamento de falhas com detalhe em WARNING (ver TSF_FAIL_DETAIL_LOG_CAP)
        self._fail_detail_log_remaining = 0

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
    def _reset_fail_detail_budget(self) -> None:
        """Reinicia contador de falhas logadas em WARNING para este método/ano."""
        self._fail_detail_log_remaining = TSF_FAIL_DETAIL_LOG_CAP

    def _log_tsf_block_failure(
        self,
        method_tag: str,
        year: int,
        detail: str,
        exc: BaseException,
    ) -> None:
        """DEBUG: toda falha. WARNING: primeiras N com texto completo; 1ª com traceback."""
        msg_body = f"{detail} | {exc.__class__.__name__}: {exc}"
        self.log.debug(f"[{method_tag}] {msg_body}")
        if self._fail_detail_log_remaining <= 0:
            return
        idx = TSF_FAIL_DETAIL_LOG_CAP - self._fail_detail_log_remaining + 1
        self._fail_detail_log_remaining -= 1
        self.log.warning(
            f"[{method_tag}] falha {idx}/{TSF_FAIL_DETAIL_LOG_CAP} (year={year}) {msg_body}",
            exc_info=(idx == 1),
        )

    def _model_key(self, scenario_key: str, method: str) -> str:
        return f"{scenario_key}__{method}"

    def _log_memory(self, ctx: str) -> None:
        try:
            import psutil, os
            p = psutil.Process(os.getpid())
            vm = psutil.virtual_memory()
            rss_gb = p.memory_info().rss / (1024 ** 3)
            avail_gb = vm.available / (1024 ** 3)
            self.log.info(
                f"[MEMORIA] {ctx} | rss={rss_gb:.2f}GB "
                f"avail={avail_gb:.2f}GB used={vm.percent}%"
            )
        except Exception:
            pass

    def _discover_years(self, folder: str) -> List[Tuple[int, Path]]:
        d = self.modeling_dir / folder
        files = sorted(d.glob("inmet_bdq_*_cerrado.parquet"))
        out = []
        for f in files:
            try:
                y = int(f.stem.split("_")[2])
                if self.filter_years and y not in self.filter_years:
                    continue
                out.append((y, f))
            except Exception:
                pass
        return out

    @staticmethod
    def _parse_ts(df: pd.DataFrame) -> pd.DataFrame:
        """Ensure ts_hour is datetime and sort."""
        df = df.copy()
        df["_ts"] = pd.to_datetime(df["ts_hour"])
        return df.sort_values(["cidade_norm", "_ts"])

    @staticmethod
    def _aggregate_series(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """One row per (cidade_norm, _ts) with mean of numeric cols."""
        grp = df.groupby(["cidade_norm", "_ts"], sort=True)
        agg = grp[cols].mean().reset_index()
        return agg

    def _active_arima_vars(self, df_agg: pd.DataFrame) -> Dict[str, str]:
        """Return {slug: col} for arima_vars slugs actually present in df_agg."""
        return {
            slug: col
            for slug, col in ARIMA_VARS_ALL.items()
            if slug in self.arima_vars and col in df_agg.columns
        }

    def _log_nan_stats(
        self, df_agg: pd.DataFrame, scenario_key: str, year: int
    ) -> None:
        """Log INFO line with NaN% per meteorological column before methods run."""
        col_slugs = [
            (COL_PRECIP,  "precip"),
            (COL_TEMP,    "temp"),
            (COL_UMID,    "umid"),
            (COL_RAD,     "rad"),
            (COL_VENTO,   "vento"),
            (COL_PRESSAO, "pressao"),
        ]
        n = len(df_agg)
        parts = []
        for col, slug in col_slugs:
            if col in df_agg.columns:
                pct = df_agg[col].isna().mean() * 100
                parts.append(f"{slug}={pct:.1f}%")
        self.log.info(
            f"[NAN] {scenario_key} year={year} n_agg={n} | "
            + (" ".join(parts) if parts else "sem colunas meteorológicas")
        )

    def _append_run_metrics(
        self,
        scenario_key: str,
        folder_name: str,
        year: int,
        method: str,
        is_train: bool,
        stats: Dict,
    ) -> None:
        """Append one row to tsf_run_metrics.csv (creates file if absent)."""
        row = {
            "scenario_key":    scenario_key,
            "folder_name":     folder_name,
            "year":            year,
            "method":          method,
            "is_train":        is_train,
            "blocks_ok":       stats.get("blocks_ok", ""),
            "blocks_fail":     stats.get("blocks_fail", ""),
            "blocks_skipped":  stats.get("blocks_skipped", ""),
            "pct_finite_pred": stats.get("pct_finite_pred", ""),
            "fail_types":      json.dumps(stats.get("fail_types", {})),
            "ts":              pd.Timestamp.now().isoformat(),
        }
        utils.ensure_dir(self._run_metrics_path.parent)
        df_row = pd.DataFrame([row])
        write_header = not self._run_metrics_path.exists()
        df_row.to_csv(self._run_metrics_path, mode="a", header=write_header, index=False)

    # ------------------------------------------------------------------
    # Rolling forecast core (shared by ARIMA and SARIMA univariate loops)
    # ------------------------------------------------------------------
    def _rolling_forecast_univariate(
        self,
        z: np.ndarray,
        min_train: int,
        H: int,
        W: int,
        fit_forecast_fn,
        method_tag: str,
        slug: str,
        city: str,
        year: int,
    ) -> Tuple[np.ndarray, int, int, int, Counter]:
        """Slide a window over z, refit every H steps, forecast H steps ahead.

        Args:
            fit_forecast_fn: callable(train_z, steps) → np.ndarray of `steps` preds.
            method_tag:      label used in DEBUG logs (e.g. 'ARIMA', 'SARIMA').

        Returns:
            (preds, blocks_ok, blocks_fail, blocks_skipped, fail_types_counter)
        """
        n = len(z)
        preds = np.full(n, np.nan)
        ok = fail = skipped = 0
        fail_types: Counter = Counter()

        for start in range(0, n, H):
            end         = min(start + H, n)
            train_start = max(0, start - W)
            train_z     = z[train_start:start]

            if len(train_z) < min_train:
                skipped += 1
                continue

            try:
                fc = fit_forecast_fn(train_z, end - start)
                preds[start:end] = fc
                ok += 1
            except Exception as exc:
                fail += 1
                fail_types[exc.__class__.__name__] += 1
                nan_in_train = int(np.isnan(train_z).sum())
                detail = (
                    f"city={city} slug={slug} bloco=[{start}:{end}] "
                    f"train_n={len(train_z)} train_nan={nan_in_train}"
                )
                self._log_tsf_block_failure(method_tag, year, detail, exc)

        return preds, ok, fail, skipped, fail_types

    def _log_method_summary(
        self,
        tag: str,
        year: int,
        slugs: List[str],
        ok: int,
        fail: int,
        skipped: int,
        pct_finite: float,
        fail_types: Counter,
    ) -> None:
        breakdown = ""
        if fail > 0:
            breakdown = " | fail_types: " + ", ".join(
                f"{k}:{v}" for k, v in fail_types.most_common()
            )
        self.log.info(
            f"[{tag} {year}] slugs={slugs} ok={ok} fail={fail} "
            f"skipped={skipped} pct_finite={pct_finite:.1f}%{breakdown}"
        )

    # ==================================================================
    # METHOD 1: EWMA multiple + lags  (fast, vectorized)
    # ==================================================================
    def _generate_ewma_lags(self, df_agg: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        result = df_agg[["cidade_norm", "_ts"]].copy()

        alphas = {"a01": 0.1, "a03": 0.3, "a08": 0.8}
        targets = {
            "precip": COL_PRECIP,
            "temp":   COL_TEMP,
            "umid":   COL_UMID,
            "rad":    COL_RAD,
        }

        for tname, col in targets.items():
            if col not in df_agg.columns:
                continue
            series = df_agg.groupby("cidade_norm")[col]

            for aname, alpha in alphas.items():
                colname = f"tsf_ewma_{tname}_{aname}"
                result[colname] = series.transform(
                    lambda x: x.ewm(alpha=alpha, adjust=False).mean()
                )

            for lag_h in [1, 24, 168]:
                colname = f"tsf_lag_{tname}_{lag_h}h"
                result[colname] = series.transform(lambda x: x.shift(lag_h))

        return result, {}

    # ==================================================================
    # METHOD 2: ARIMA  (per-city, per-variable, refit every H hours)
    # ==================================================================
    def _generate_arima(
        self, df_agg: pd.DataFrame, year: int, is_train: bool
    ) -> Tuple[pd.DataFrame, Dict]:
        if not statsmodels_available:
            self.log.warning("[ARIMA] statsmodels não instalado, pulando.")
            return pd.DataFrame(), {}

        active = self._active_arima_vars(df_agg)
        if not active:
            self.log.warning("[ARIMA] Nenhuma variável de interesse disponível no df_agg.")
            return pd.DataFrame(), {}

        self._reset_fail_detail_budget()

        result = df_agg[["cidade_norm", "_ts"]].copy()
        for slug in active:
            result[f"tsf_arima_{slug}_pred"]  = np.nan
            result[f"tsf_arima_{slug}_resid"] = np.nan

        cities    = df_agg["cidade_norm"].unique()
        H, W      = self.refit_hours, self.window_hours
        order     = self.arima_order
        min_train = max(order[0], order[2]) + order[1] + 10

        def _fit_fn(train_z, steps):
            model = SM_ARIMA(
                train_z, order=order,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                res = model.fit(method_kwargs={"maxiter": 100})
            return res.forecast(steps=steps)

        total_ok = total_fail = total_skipped = 0
        total_ftypes: Counter = Counter()

        for city in tqdm(cities, desc=f"ARIMA {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            idx     = city_df.index

            for slug, col in active.items():
                z = city_df[col].values.astype(float)
                preds, ok, fail, skipped, ftypes = self._rolling_forecast_univariate(
                    z, min_train, H, W, _fit_fn, "ARIMA", slug, city, year
                )
                total_ok      += ok
                total_fail    += fail
                total_skipped += skipped
                total_ftypes.update(ftypes)

                result.loc[idx, f"tsf_arima_{slug}_pred"]  = preds
                result.loc[idx, f"tsf_arima_{slug}_resid"] = z - preds
                self.layer_a.add("arima", slug, city, year, z, preds, is_train=is_train)

        first_pred = f"tsf_arima_{next(iter(active))}_pred"
        pct_finite = float(np.isfinite(result[first_pred].values).mean() * 100)
        self._log_method_summary(
            "ARIMA", year, list(active.keys()),
            total_ok, total_fail, total_skipped, pct_finite, total_ftypes,
        )
        stats = {
            "blocks_ok": total_ok, "blocks_fail": total_fail,
            "blocks_skipped": total_skipped, "pct_finite_pred": pct_finite,
            "fail_types": dict(total_ftypes),
        }
        return result, stats

    # ==================================================================
    # METHOD 3: SARIMA  (per-city, per-variable, refit every H hours)
    # ==================================================================
    def _generate_sarima(
        self, df_agg: pd.DataFrame, year: int, is_train: bool
    ) -> Tuple[pd.DataFrame, Dict]:
        if not statsmodels_available:
            self.log.warning("[SARIMA] statsmodels não instalado, pulando.")
            return pd.DataFrame(), {}

        active = self._active_arima_vars(df_agg)
        if not active:
            self.log.warning("[SARIMA] Nenhuma variável de interesse disponível no df_agg.")
            return pd.DataFrame(), {}

        self._reset_fail_detail_budget()

        result = df_agg[["cidade_norm", "_ts"]].copy()
        for slug in active:
            result[f"tsf_sarima_{slug}_pred"]  = np.nan
            result[f"tsf_sarima_{slug}_resid"] = np.nan

        cities   = df_agg["cidade_norm"].unique()
        H, W     = self.refit_hours, self.window_hours
        order    = self.arima_order
        seasonal = (1, 1, 1, self.sarima_m)
        min_train = self.sarima_m * 2 + 10

        def _fit_fn(train_z, steps):
            model = SM_SARIMAX(
                train_z, order=order, seasonal_order=seasonal,
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                res = model.fit(maxiter=80, disp=False)
            return res.forecast(steps=steps)

        total_ok = total_fail = total_skipped = 0
        total_ftypes: Counter = Counter()

        for city in tqdm(cities, desc=f"SARIMA {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            idx     = city_df.index

            for slug, col in active.items():
                z = city_df[col].values.astype(float)
                preds, ok, fail, skipped, ftypes = self._rolling_forecast_univariate(
                    z, min_train, H, W, _fit_fn, "SARIMA", slug, city, year
                )
                total_ok      += ok
                total_fail    += fail
                total_skipped += skipped
                total_ftypes.update(ftypes)

                result.loc[idx, f"tsf_sarima_{slug}_pred"]  = preds
                result.loc[idx, f"tsf_sarima_{slug}_resid"] = z - preds
                self.layer_a.add("sarima", slug, city, year, z, preds, is_train=is_train)

        first_pred = f"tsf_sarima_{next(iter(active))}_pred"
        pct_finite = float(np.isfinite(result[first_pred].values).mean() * 100)
        self._log_method_summary(
            "SARIMA", year, list(active.keys()),
            total_ok, total_fail, total_skipped, pct_finite, total_ftypes,
        )
        stats = {
            "blocks_ok": total_ok, "blocks_fail": total_fail,
            "blocks_skipped": total_skipped, "pct_finite_pred": pct_finite,
            "fail_types": dict(total_ftypes),
        }
        return result, stats

    # ==================================================================
    # METHOD 4: ARIMAX  (per-city, one endog + exog from other vars)
    #
    # Horizon policy: exog_future = last observed exog row repeated H times.
    # This is an operacional feature-engineering device, not a physical
    # meteorological forecast. Document this when interpreting results.
    # ==================================================================
    def _generate_arimax(
        self, df_agg: pd.DataFrame, year: int, is_train: bool
    ) -> Tuple[pd.DataFrame, Dict]:
        if not statsmodels_available:
            self.log.warning("[ARIMAX] statsmodels não instalado, pulando.")
            return pd.DataFrame(), {}

        active = self._active_arima_vars(df_agg)
        endog_slug = self.arimax_endog
        endog_col  = active.get(endog_slug)

        if endog_col is None:
            self.log.warning(
                f"[ARIMAX] Endog '{endog_slug}' não disponível em df_agg. "
                f"Slugs presentes: {list(active.keys())}"
            )
            return pd.DataFrame(), {}

        exog_items = [(s, c) for s, c in active.items() if s != endog_slug]
        if not exog_items:
            self.log.warning(
                f"[ARIMAX] Sem variáveis exógenas além de '{endog_slug}'. "
                "Adicione mais slugs em --arima-vars para habilitar ARIMAX."
            )
            return pd.DataFrame(), {}

        exog_slugs = [s for s, _ in exog_items]
        exog_cols  = [c for _, c in exog_items]
        self.log.info(
            f"[ARIMAX {year}] endog={endog_slug} exog={exog_slugs} "
            f"exog_future=last_row_repeated"
        )

        self._reset_fail_detail_budget()

        pred_col  = f"tsf_arimax_{endog_slug}_pred"
        resid_col = f"tsf_arimax_{endog_slug}_resid"
        result    = df_agg[["cidade_norm", "_ts"]].copy()
        result[pred_col]  = np.nan
        result[resid_col] = np.nan

        cities = df_agg["cidade_norm"].unique()
        H, W   = self.refit_hours, self.window_hours
        order  = self.arima_order
        n_exog = len(exog_cols)
        min_train = max(order[0], order[2]) + order[1] + n_exog + 10

        ok = fail = skipped = 0
        fail_types: Counter = Counter()

        for city in tqdm(cities, desc=f"ARIMAX {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            idx     = city_df.index

            z_endog = city_df[endog_col].values.astype(float)
            z_exog  = city_df[exog_cols].values.astype(float)  # (n, n_exog)
            n       = len(z_endog)
            preds   = np.full(n, np.nan)

            for start in range(0, n, H):
                end         = min(start + H, n)
                train_start = max(0, start - W)

                tr_endog = z_endog[train_start:start]
                tr_exog  = z_exog[train_start:start, :]

                # Drop rows with any NaN in endog or exog
                valid_mask  = np.isfinite(tr_endog) & np.all(np.isfinite(tr_exog), axis=1)
                tr_endog_c  = tr_endog[valid_mask]
                tr_exog_c   = tr_exog[valid_mask, :]

                if len(tr_endog_c) < min_train:
                    skipped += 1
                    continue

                # Last observed exog row repeated for the forecast horizon
                exog_future = np.tile(tr_exog_c[-1:, :], (end - start, 1))

                try:
                    model = SM_SARIMAX(
                        tr_endog_c,
                        exog=tr_exog_c,
                        order=order,
                        trend="n",
                        enforce_stationarity=False,
                        enforce_invertibility=False,
                    )
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = model.fit(maxiter=100, disp=False)
                    fc = res.forecast(steps=end - start, exog=exog_future)
                    preds[start:end] = fc
                    ok += 1
                except Exception as exc:
                    fail += 1
                    fail_types[exc.__class__.__name__] += 1
                    detail = (
                        f"city={city} endog={endog_slug} exog={exog_slugs} "
                        f"bloco=[{start}:{end}] train_n={len(tr_endog_c)}"
                    )
                    self._log_tsf_block_failure("ARIMAX", year, detail, exc)

            result.loc[idx, pred_col]  = preds
            result.loc[idx, resid_col] = z_endog - preds
            self.layer_a.add("arimax", endog_slug, city, year, z_endog, preds, is_train=is_train)

        pct_finite = float(np.isfinite(result[pred_col].values).mean() * 100)
        self._log_method_summary(
            "ARIMAX", year, [f"{endog_slug}~{exog_slugs}"],
            ok, fail, skipped, pct_finite, fail_types,
        )
        stats = {
            "blocks_ok": ok, "blocks_fail": fail, "blocks_skipped": skipped,
            "pct_finite_pred": pct_finite, "fail_types": dict(fail_types),
        }
        return result, stats

    # ==================================================================
    # METHOD 5: SARIMAX_exog  (ARIMAX + seasonal component)
    # Same horizon policy as ARIMAX (last exog row repeated).
    # ==================================================================
    def _generate_sarimax_exog(
        self, df_agg: pd.DataFrame, year: int, is_train: bool
    ) -> Tuple[pd.DataFrame, Dict]:
        if not statsmodels_available:
            self.log.warning("[SARIMAX_exog] statsmodels não instalado, pulando.")
            return pd.DataFrame(), {}

        active = self._active_arima_vars(df_agg)
        endog_slug = self.arimax_endog
        endog_col  = active.get(endog_slug)

        if endog_col is None:
            self.log.warning(
                f"[SARIMAX_exog] Endog '{endog_slug}' não disponível em df_agg. "
                f"Slugs presentes: {list(active.keys())}"
            )
            return pd.DataFrame(), {}

        exog_items = [(s, c) for s, c in active.items() if s != endog_slug]
        if not exog_items:
            self.log.warning(
                f"[SARIMAX_exog] Sem variáveis exógenas além de '{endog_slug}'."
            )
            return pd.DataFrame(), {}

        exog_slugs = [s for s, _ in exog_items]
        exog_cols  = [c for _, c in exog_items]
        seasonal   = (1, 1, 1, self.sarima_m)
        self.log.info(
            f"[SARIMAX_exog {year}] endog={endog_slug} exog={exog_slugs} "
            f"seasonal={seasonal} exog_future=last_row_repeated"
        )

        self._reset_fail_detail_budget()

        pred_col  = f"tsf_sarimax_exog_{endog_slug}_pred"
        resid_col = f"tsf_sarimax_exog_{endog_slug}_resid"
        result    = df_agg[["cidade_norm", "_ts"]].copy()
        result[pred_col]  = np.nan
        result[resid_col] = np.nan

        cities = df_agg["cidade_norm"].unique()
        H, W   = self.refit_hours, self.window_hours
        order  = self.arima_order
        n_exog = len(exog_cols)
        min_train = max(self.sarima_m * 2, max(order[0], order[2]) + order[1]) + n_exog + 10

        ok = fail = skipped = 0
        fail_types: Counter = Counter()

        for city in tqdm(cities, desc=f"SARIMAX_exog {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            idx     = city_df.index

            z_endog = city_df[endog_col].values.astype(float)
            z_exog  = city_df[exog_cols].values.astype(float)
            n       = len(z_endog)
            preds   = np.full(n, np.nan)

            for start in range(0, n, H):
                end         = min(start + H, n)
                train_start = max(0, start - W)

                tr_endog = z_endog[train_start:start]
                tr_exog  = z_exog[train_start:start, :]

                valid_mask = np.isfinite(tr_endog) & np.all(np.isfinite(tr_exog), axis=1)
                tr_endog_c = tr_endog[valid_mask]
                tr_exog_c  = tr_exog[valid_mask, :]

                if len(tr_endog_c) < min_train:
                    skipped += 1
                    continue

                exog_future = np.tile(tr_exog_c[-1:, :], (end - start, 1))

                try:
                    model = SM_SARIMAX(
                        tr_endog_c,
                        exog=tr_exog_c,
                        order=order,
                        seasonal_order=seasonal,
                        trend="n",
                        enforce_stationarity=False,
                        enforce_invertibility=False,
                    )
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = model.fit(maxiter=80, disp=False)
                    fc = res.forecast(steps=end - start, exog=exog_future)
                    preds[start:end] = fc
                    ok += 1
                except Exception as exc:
                    fail += 1
                    fail_types[exc.__class__.__name__] += 1
                    detail = (
                        f"city={city} endog={endog_slug} exog={exog_slugs} "
                        f"bloco=[{start}:{end}] train_n={len(tr_endog_c)}"
                    )
                    self._log_tsf_block_failure("SARIMAX_exog", year, detail, exc)

            result.loc[idx, pred_col]  = preds
            result.loc[idx, resid_col] = z_endog - preds
            self.layer_a.add("sarimax_exog", endog_slug, city, year, z_endog, preds, is_train=is_train)

        pct_finite = float(np.isfinite(result[pred_col].values).mean() * 100)
        self._log_method_summary(
            "SARIMAX_exog", year, [f"{endog_slug}~{exog_slugs}"],
            ok, fail, skipped, pct_finite, fail_types,
        )
        stats = {
            "blocks_ok": ok, "blocks_fail": fail, "blocks_skipped": skipped,
            "pct_finite_pred": pct_finite, "fail_types": dict(fail_types),
        }
        return result, stats

    # ==================================================================
    # METHOD 6: Prophet  (per-city, refit every H hours)
    # Note: Prophet is a classical additive time-series model using
    # Fourier seasonalities and changepoint regression — NOT an LLM.
    # ==================================================================
    def _generate_prophet(
        self, df_agg: pd.DataFrame, year: int, is_train: bool
    ) -> Tuple[pd.DataFrame, Dict]:
        if not prophet_available:
            self.log.warning("[Prophet] prophet não instalado, pulando.")
            return pd.DataFrame(), {}

        result = df_agg[["cidade_norm", "_ts"]].copy()
        result["tsf_prophet_precip_pred"]  = np.nan
        result["tsf_prophet_precip_resid"] = np.nan

        self._reset_fail_detail_budget()

        cities = df_agg["cidade_norm"].unique()
        H, W   = self.refit_hours, self.window_hours
        ok = fail = skipped = 0
        fail_types: Counter = Counter()

        for city in tqdm(cities, desc=f"Prophet {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            z  = city_df[Z_PRIMARY].values.astype(float)
            ts = city_df["_ts"].values
            n  = len(z)
            preds = np.full(n, np.nan)

            for start in range(0, n, H):
                end         = min(start + H, n)
                train_start = max(0, start - W)
                train_ts    = ts[train_start:start]
                train_z     = z[train_start:start]

                if len(train_z) < 48:
                    skipped += 1
                    continue

                try:
                    pdf = pd.DataFrame({"ds": train_ts, "y": train_z})
                    m   = FBProphet(
                        yearly_seasonality=False,
                        weekly_seasonality=True,
                        daily_seasonality=True,
                        changepoint_prior_scale=0.05,
                    )
                    m.fit(pdf)

                    future_ts = ts[start:end]
                    future    = pd.DataFrame({"ds": future_ts})
                    fc        = m.predict(future)
                    preds[start:end] = fc["yhat"].values
                    ok += 1
                except Exception as exc:
                    fail += 1
                    fail_types[exc.__class__.__name__] += 1
                    nan_in_train = int(np.isnan(train_z).sum())
                    detail = (
                        f"city={city} slug=precip bloco=[{start}:{end}] "
                        f"train_n={len(train_z)} train_nan={nan_in_train}"
                    )
                    self._log_tsf_block_failure("Prophet", year, detail, exc)

            idx = city_df.index
            result.loc[idx, "tsf_prophet_precip_pred"]  = preds
            result.loc[idx, "tsf_prophet_precip_resid"] = z - preds
            self.layer_a.add("prophet", "precip", city, year, z, preds, is_train=is_train)

        pct_finite = float(np.isfinite(result["tsf_prophet_precip_pred"].values).mean() * 100)
        self._log_method_summary(
            "Prophet", year, ["precip"],
            ok, fail, skipped, pct_finite, fail_types,
        )
        stats = {
            "blocks_ok": ok, "blocks_fail": fail, "blocks_skipped": skipped,
            "pct_finite_pred": pct_finite, "fail_types": dict(fail_types),
        }
        return result, stats

    # ==================================================================
    # METHOD 7: MiniROCKET  (window transform, fit on train years)
    # ==================================================================
    def _build_windows(
        self, df_agg: pd.DataFrame, cols: List[str], L: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        """Build causal sliding windows (n_samples, n_channels, L).
        Returns (X_3d, valid_mask) where valid_mask[i] is True if
        row i has a full window of L past observations."""
        n = len(df_agg)
        C = len(cols)
        X = np.full((n, C, L), np.nan, dtype=np.float32)
        valid = np.zeros(n, dtype=bool)

        cities = df_agg["cidade_norm"].values
        vals   = df_agg[cols].values.astype(np.float32)

        city_starts: Dict[str, int] = {}
        prev_city = None
        for i in range(n):
            c = cities[i]
            if c != prev_city:
                city_starts[c] = i
                prev_city = c

        for i in range(n):
            c         = cities[i]
            cs        = city_starts[c]
            local_idx = i - cs
            if local_idx < L:
                continue
            window = vals[i - L: i]
            if np.any(np.isnan(window)):
                continue
            X[i]     = window.T
            valid[i] = True

        return X, valid

    def _generate_minirocket(
        self, df_agg: pd.DataFrame, year: int, is_train: bool,
        model_key: str,
    ) -> Tuple[pd.DataFrame, Dict]:
        if not minirocket_available:
            self.log.warning("[MiniROCKET] aeon não instalado, pulando.")
            return pd.DataFrame(), {}

        L = self.minirocket_window
        cols_for_window = [
            c for c in [COL_PRECIP, COL_TEMP, COL_UMID, COL_RAD]
            if c in df_agg.columns
        ]
        if not cols_for_window:
            return pd.DataFrame(), {}

        self.log.info(
            f"[MiniROCKET {year}] Construindo janelas L={L}, "
            f"canais={len(cols_for_window)}..."
        )
        self._log_memory(f"MiniROCKET {year} pre-janelas")
        X_3d, valid = self._build_windows(df_agg, cols_for_window, L)
        n_valid = int(valid.sum())
        self.log.info(f"[MiniROCKET {year}] {n_valid}/{len(valid)} janelas válidas")

        if n_valid == 0:
            return pd.DataFrame(), {}

        X_valid = X_3d[valid]

        if is_train and model_key not in self._minirocket_models:
            self.log.info(
                f"[MiniROCKET] Fitting em {n_valid} janelas de treino, "
                f"num_kernels={self.minirocket_kernels}..."
            )
            mr = MiniRocket(n_kernels=self.minirocket_kernels, random_state=42)
            mr.fit(X_valid)
            self._minirocket_models[model_key] = mr

        if model_key not in self._minirocket_models:
            self.log.warning(
                "[MiniROCKET] Modelo ainda não treinado para esta chave "
                f"({model_key}). Pulando ano de teste."
            )
            return pd.DataFrame(), {}

        self.log.info(f"[MiniROCKET {year}] Transformando {n_valid} janelas...")
        self._log_memory(f"MiniROCKET {year} pre-transform")
        feats  = self._minirocket_models[model_key].transform(X_valid)
        n_feat = feats.shape[1]

        result    = df_agg[["cidade_norm", "_ts"]].copy()
        feat_cols = [f"tsf_minirocket_f{i:03d}" for i in range(n_feat)]
        for col in feat_cols:
            result[col] = np.nan
        result.loc[valid, feat_cols] = feats.astype(np.float32)

        self.log.info(f"[MiniROCKET {year}] {n_feat} features geradas")
        self._log_memory(f"MiniROCKET {year} pos-transform")
        return result, {}

    # ==================================================================
    # METHOD 8: TS K-Means  (cluster weekly/daily patterns, fit on train)
    # ==================================================================
    def _generate_tskmeans(
        self, df_agg: pd.DataFrame, year: int, is_train: bool,
        model_key: str,
    ) -> Tuple[pd.DataFrame, Dict]:
        if not tskmeans_available:
            self.log.warning("[TSKMeans] tslearn não instalado, pulando.")
            return pd.DataFrame(), {}

        P      = self.tskmeans_period
        result = df_agg[["cidade_norm", "_ts"]].copy()
        result["tsf_tskmeans_cluster"] = np.nan

        if Z_PRIMARY not in df_agg.columns:
            return result, {}

        cities         = df_agg["cidade_norm"].unique()
        all_segments:  List[np.ndarray]     = []
        segment_keys:  List[Tuple[str, int]] = []

        for city in cities:
            mask  = df_agg["cidade_norm"] == city
            z     = df_agg.loc[mask, Z_PRIMARY].values.astype(float)
            n_seg = len(z) // P
            for s in range(n_seg):
                seg = z[s * P: (s + 1) * P]
                if np.isnan(seg).sum() > P * 0.3:
                    continue
                seg_clean = np.nan_to_num(seg, nan=0.0)
                all_segments.append(seg_clean)
                segment_keys.append((city, s))

        if not all_segments:
            return result, {}

        X_seg = np.array(all_segments)[:, :, np.newaxis]

        if is_train and model_key not in self._tskmeans_models:
            self.log.info(
                f"[TSKMeans] Fitting k={self.tskmeans_k} em "
                f"{len(X_seg)} segmentos..."
            )
            km = TimeSeriesKMeans(
                n_clusters=self.tskmeans_k,
                metric="euclidean",
                max_iter=50,
                random_state=42,
                n_jobs=-1,
            )
            km.fit(X_seg)
            self._tskmeans_models[model_key] = km

        if model_key not in self._tskmeans_models:
            return result, {}

        labels = self._tskmeans_models[model_key].predict(X_seg)

        for idx, (city, seg_idx) in enumerate(segment_keys):
            mask         = df_agg["cidade_norm"] == city
            city_indices = df_agg.index[mask]
            start        = seg_idx * P
            end_idx      = min(start + P, len(city_indices))
            if end_idx > len(city_indices):
                continue
            result.loc[city_indices[start:end_idx], "tsf_tskmeans_cluster"] = float(
                labels[idx]
            )

        n_assigned = result["tsf_tskmeans_cluster"].notna().sum()
        self.log.info(
            f"[TSKMeans {year}] Clusters atribuídos a {n_assigned}/{len(result)} linhas"
        )
        return result, {}

    # ==================================================================
    # Year-level: generate features for a single method
    # ==================================================================
    def _generate_method_features(
        self,
        method: str,
        df_agg: pd.DataFrame,
        year: int,
        is_train: bool,
        model_key: str,
    ) -> Tuple[pd.DataFrame, Dict]:
        if method == "ewma_lags":
            return self._generate_ewma_lags(df_agg)
        if method == "arima":
            return self._generate_arima(df_agg, year, is_train)
        if method == "sarima":
            return self._generate_sarima(df_agg, year, is_train)
        if method == "arimax":
            return self._generate_arimax(df_agg, year, is_train)
        if method == "sarimax_exog":
            return self._generate_sarimax_exog(df_agg, year, is_train)
        if method == "prophet":
            return self._generate_prophet(df_agg, year, is_train)
        if method == "minirocket":
            return self._generate_minirocket(df_agg, year, is_train, model_key)
        if method == "tskmeans":
            return self._generate_tskmeans(df_agg, year, is_train, model_key)
        self.log.warning(f"[UNKNOWN METHOD] {method}")
        return pd.DataFrame(), {}

    def _merge_features_back(
        self,
        df: pd.DataFrame,
        df_agg: pd.DataFrame,
        feature_dfs: List[pd.DataFrame],
    ) -> pd.DataFrame:
        """Merge feature columns back to the original (possibly duplicated) rows."""
        if not feature_dfs:
            return df.drop(columns=["_ts"], errors="ignore")

        merged = feature_dfs[0]
        for extra in feature_dfs[1:]:
            new_cols = [c for c in extra.columns if c not in ("cidade_norm", "_ts")]
            if new_cols:
                merged = merged.merge(
                    extra[["cidade_norm", "_ts"] + new_cols],
                    on=["cidade_norm", "_ts"],
                    how="left",
                )

        df["_ts"] = pd.to_datetime(df["ts_hour"])
        tsf_cols  = [c for c in merged.columns if c not in ("cidade_norm", "_ts")]
        df_out    = df.merge(
            merged[["cidade_norm", "_ts"] + tsf_cols],
            on=["cidade_norm", "_ts"],
            how="left",
        )
        df_out.drop(columns=["_ts"], inplace=True, errors="ignore")
        return df_out

    # ==================================================================
    # Process one year — SPLIT layout (one folder per method)
    # ==================================================================
    def _process_year_split(
        self,
        scenario_key: str,
        folder_name: str,
        year: int,
        src_path: Path,
        is_train: bool,
    ) -> None:
        t0 = time.time()

        df_raw = pd.read_parquet(src_path)
        n_raw  = len(df_raw)
        self._log_memory(f"pos-read {scenario_key}/{year}")

        df = self._parse_ts(df_raw)

        numeric_cols = [
            c for c in [COL_PRECIP, COL_TEMP, COL_UMID, COL_RAD, COL_VENTO, COL_PRESSAO]
            if c in df.columns
        ]
        df_agg = self._aggregate_series(df, numeric_cols)
        self.log.info(
            f"[AGG] {n_raw} linhas raw -> {len(df_agg)} agregadas "
            f"({df_agg['cidade_norm'].nunique()} cidades)"
        )
        self._log_nan_stats(df_agg, scenario_key, year)

        for method in sorted(self.methods):
            out_dir  = self.temporal_fusion_dir / folder_name / method
            out_path = out_dir / src_path.name

            if out_path.exists() and not self.overwrite:
                self.log.info(f"[SKIP] {method}/{out_path.name} já existe")
                continue

            t1 = time.time()
            model_key = self._model_key(scenario_key, method)
            feat, stats = self._generate_method_features(
                method, df_agg, year, is_train, model_key
            )
            elapsed_method = time.time() - t1

            self._append_run_metrics(scenario_key, folder_name, year, method, is_train, stats)

            if feat.empty:
                self.log.info(
                    f"[{method} {year}] sem features geradas em {elapsed_method:.1f}s"
                )
                continue

            df_enriched = self._merge_features_back(df.copy(), df_agg, [feat])
            utils.ensure_dir(out_dir)
            df_enriched.to_parquet(out_path, index=False)

            self.log.info(
                f"[SAVED] {method}/{out_path.name} | {len(df_enriched)} linhas "
                f"| método {elapsed_method:.1f}s"
            )
            self._log_memory(f"pos-save {method}/{year}")

            del df_enriched, feat
            gc.collect()

        elapsed = time.time() - t0
        self.log.info(
            f"[DONE] {scenario_key}/{year} | train={is_train} | total {elapsed:.0f}s"
        )

        del df_raw, df, df_agg
        gc.collect()

    # ==================================================================
    # Process one year — MERGED layout (legacy)
    # ==================================================================
    def _process_year_merged(
        self,
        scenario_key: str,
        folder_name: str,
        year: int,
        src_path: Path,
        out_dir: Path,
        is_train: bool,
    ) -> None:
        out_path = out_dir / src_path.name
        if out_path.exists() and not self.overwrite:
            self.log.info(f"[SKIP] {out_path.name} já existe")
            return

        t0 = time.time()
        self.log.info(
            f"[PROCESS] {scenario_key} / {year} | src={src_path.name} "
            f"| train={is_train}"
        )

        df_raw = pd.read_parquet(src_path)
        n_raw  = len(df_raw)
        self._log_memory(f"pos-read {scenario_key}/{year}")

        df = self._parse_ts(df_raw)

        numeric_cols = [
            c for c in [COL_PRECIP, COL_TEMP, COL_UMID, COL_RAD, COL_VENTO, COL_PRESSAO]
            if c in df.columns
        ]
        df_agg = self._aggregate_series(df, numeric_cols)
        self.log.info(
            f"[AGG] {n_raw} linhas raw -> {len(df_agg)} agregadas "
            f"({df_agg['cidade_norm'].nunique()} cidades)"
        )
        self._log_nan_stats(df_agg, scenario_key, year)

        feature_dfs: List[pd.DataFrame] = []

        for method in sorted(self.methods):
            t1 = time.time()
            mk          = self._model_key(scenario_key, method)
            feat, stats = self._generate_method_features(
                method, df_agg, year, is_train, mk
            )
            self._append_run_metrics(scenario_key, folder_name, year, method, is_train, stats)
            if len(feat):
                feature_dfs.append(feat)
            self.log.info(
                f"[{method} {year}] concluído em {time.time()-t1:.1f}s"
            )

        df_enriched = self._merge_features_back(df.copy(), df_agg, feature_dfs)

        utils.ensure_dir(out_dir)
        df_enriched.to_parquet(out_path, index=False)
        elapsed = time.time() - t0
        self.log.info(
            f"[SAVED] {out_path.name} | {len(df_enriched)} linhas | {elapsed:.0f}s"
        )
        self._log_memory(f"pos-save merged/{year}")

        del df_raw, df, df_agg, df_enriched, feature_dfs
        gc.collect()

    # ==================================================================
    # Main entry point
    # ==================================================================
    def run(self) -> None:
        self.log.info("=" * 70)
        self.log.info("TEMPORAL FUSION FEATURE ENGINEERING")
        self.log.info(f"Layout:      {self.output_layout}")
        self.log.info(f"Métodos:     {sorted(self.methods)}")
        self.log.info(f"Cenários:    {list(self.scenarios.keys())}")
        self.log.info(f"ARIMA vars:  {self.arima_vars}")
        self.log.info(f"ARIMAX endog: {self.arimax_endog}")
        self.log.info(f"Refit H={self.refit_hours}h, Window W={self.window_hours}h")
        self.log.info(
            f"NOTA: As primeiras {TSF_FAIL_DETAIL_LOG_CAP} falhas por método/ano "
            "são logadas em WARNING com mensagem completa (a 1ª inclui traceback). "
            "Todas as falhas em DEBUG se logging.level=DEBUG em config.yaml."
        )
        self.log.info("=" * 70)
        self._log_memory("inicio run")

        for scenario_key, folder_name in self.scenarios.items():
            year_files = self._discover_years(folder_name)
            if not year_files:
                self.log.warning(
                    f"[{scenario_key}] Nenhum parquet encontrado em "
                    f"{self.modeling_dir / folder_name}"
                )
                continue

            years    = [y for y, _ in year_files]
            cut_year = sorted(years)[-self.test_size_years]
            self.log.info(
                f"[{scenario_key}] {len(year_files)} anos: "
                f"{years[0]}..{years[-1]} | teste >= {cut_year}"
            )

            if self.output_layout == "split":
                for year, src_path in tqdm(year_files, desc=scenario_key):
                    is_train = year < cut_year
                    self._process_year_split(
                        scenario_key, folder_name, year, src_path, is_train
                    )
            else:
                # merged (legacy)
                out_folder = f"{folder_name}_tsfusion"
                out_dir    = self.modeling_dir / out_folder
                for year, src_path in tqdm(year_files, desc=scenario_key):
                    is_train = year < cut_year
                    self._process_year_merged(
                        scenario_key, folder_name, year,
                        src_path, out_dir, is_train,
                    )

        # Save Layer A metrics (all years + train-only + method ranking)
        layer_a_path = self._eda_tsf_dir / "layer_a_summary.csv"
        self.layer_a.save(layer_a_path)
        self.log.info(f"[Layer A] Salvo em {layer_a_path.parent}/")
        self.log.info(f"[Run metrics] Salvo em {self._run_metrics_path}")

        self.log.info("TEMPORAL FUSION COMPLETO")
        self._log_memory("fim run")


# ============================================================================
# CLI
# ============================================================================
def main():
    parser = argparse.ArgumentParser(
        description=(
            "Temporal Fusion Feature Engineering — gera features tsf_* "
            "por método (split) ou todas juntas (merged) para bases calculated."
        )
    )
    parser.add_argument(
        "--methods", nargs="+",
        choices=sorted(ALL_METHODS),
        default=None,
        help=(
            "Famílias de método (padrão: todas). "
            "Use --scenarios para restringir cenários e economizar tempo."
        ),
    )
    parser.add_argument(
        "--scenarios", nargs="+",
        default=None,
        help=(
            "Chaves de cenário do config.yaml "
            "(padrão: base_D_calculated base_E_calculated base_F_calculated). "
            "Exemplos: base_D_calculated base_E_calculated"
        ),
    )
    parser.add_argument(
        "--output-layout",
        choices=["split", "merged"],
        default="split",
        help=(
            "split: uma pasta por método em data/temporal_fusion/ (padrão). "
            "merged: todos os métodos num único parquet em data/modeling/ (legado)."
        ),
    )
    parser.add_argument("--years",           nargs="+", type=int, default=None)
    parser.add_argument("--refit-hours",     type=int,  default=168)
    parser.add_argument("--window-hours",    type=int,  default=720)
    parser.add_argument("--sarima-m",        type=int,  default=24)
    parser.add_argument(
        "--arima-order", nargs=3, type=int, default=[2, 1, 2],
        metavar=("P", "D", "Q"),
    )
    parser.add_argument(
        "--arima-vars", nargs="+",
        choices=sorted(ARIMA_VARS_ALL.keys()),
        default=None,
        help=(
            "Slugs das variáveis para ARIMA/SARIMA univariados "
            f"(padrão: {ARIMA_VARS_DEFAULT}). "
            "Também define quais variáveis são candidatas a exog em ARIMAX/SARIMAX_exog."
        ),
    )
    parser.add_argument(
        "--arimax-endog",
        default="precip",
        choices=sorted(ARIMA_VARS_ALL.keys()),
        help=(
            "Slug da variável endógena para ARIMAX e SARIMAX_exog "
            "(padrão: precip). As demais arima-vars serão usadas como exog."
        ),
    )
    parser.add_argument("--minirocket-window",  type=int, default=24)
    parser.add_argument("--minirocket-kernels", type=int, default=84)
    parser.add_argument("--tskmeans-k",         type=int, default=8)
    parser.add_argument("--tskmeans-period",    type=int, default=168)
    parser.add_argument("--test-years",         type=int, default=2)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    cfg = utils.loadConfig()

    scenarios = None
    if args.scenarios:
        scenarios = {}
        for k in args.scenarios:
            folder = cfg["modeling_scenarios"].get(k)
            if folder:
                scenarios[k] = folder
            else:
                print(f"[WARN] Cenário '{k}' não encontrado em config.yaml")

    methods = set(args.methods) if args.methods else None

    eng = TemporalFusionEngineer(
        scenarios=scenarios,
        methods=methods,
        refit_hours=args.refit_hours,
        window_hours=args.window_hours,
        sarima_m=args.sarima_m,
        arima_order=tuple(args.arima_order),
        arima_vars=args.arima_vars,
        arimax_endog=args.arimax_endog,
        minirocket_window=args.minirocket_window,
        minirocket_kernels=args.minirocket_kernels,
        tskmeans_k=args.tskmeans_k,
        tskmeans_period=args.tskmeans_period,
        test_size_years=args.test_years,
        overwrite=args.overwrite,
        filter_years=args.years,
        output_layout=args.output_layout,
    )
    eng.run()


if __name__ == "__main__":
    main()
