# src/feature_engineering_temporal.py
# =============================================================================
# TEMPORAL FUSION FEATURE ENGINEERING
# Generates tsf_* columns (EWMA/lags, ARIMA, SARIMA, Prophet, MiniROCKET,
# TSKMeans) for bases D/F calculated, following the same pattern as
# feature_engineering_physics.py.
#
# Two-layer evaluation:
#   Layer A: MAE/MSE/R² of the temporal model on the continuous series z.
#   Layer B: improvement in HAS_FOCO classification (measured later in
#            train_runner via PR-AUC etc.).
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
import time
import json
import warnings
import argparse
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

Z_PRIMARY = COL_PRECIP  # official series z for Layer A

ALL_METHODS = {"ewma_lags", "arima", "sarima", "prophet", "minirocket", "tskmeans"}

# Scenarios to enrich (only D and F calculated by default)
DEFAULT_SCENARIOS = {
    "base_D_calculated": "base_D_with_rad_drop_rows_calculated",
    "base_F_calculated": "base_F_full_original_calculated",
}

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
    """Accumulates per-city, per-method (y_true, y_pred) and computes
    MAE / MSE / R² for the temporal model on the continuous series z.

    The ``is_train`` flag lets us export a training-only summary that can be
    used to rank methods without leaking information from test years.
    """

    def __init__(self):
        self.records: List[Dict] = []

    def add(self, method: str, city: str, year: int,
            y_true: np.ndarray, y_pred: np.ndarray,
            is_train: bool = False) -> None:
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
            "method": method, "city": city, "year": year,
            "n": int(len(yt)), "mae": mae, "mse": mse, "r2": r2,
            "is_train": bool(is_train),
        })

    def _aggregate(self, df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.groupby(["method", "year"])
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
        """One row per method ranked by mae_mean ascending (train years only)."""
        summ = self.summary_train_df()
        if summ.empty:
            return pd.DataFrame()
        return (
            summ.groupby("method")
            .agg(
                n_cities_total=("n_cities", "sum"),
                n_obs_total=("n_total", "sum"),
                mae_mean=("mae_mean", "mean"),
                mse_mean=("mse_mean", "mean"),
                r2_mean=("r2_mean", "mean"),
            )
            .reset_index()
            .sort_values("mae_mean")
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
        self.modeling_dir = Path(self.cfg["paths"]["data"]["modeling"])
        self.temporal_fusion_dir = Path(self.cfg["paths"]["data"]["temporal_fusion"])

        self.scenarios = scenarios or DEFAULT_SCENARIOS
        self.methods = methods if methods is not None else ALL_METHODS
        self.refit_hours = refit_hours
        self.window_hours = window_hours
        self.sarima_m = sarima_m
        self.arima_order = arima_order
        self.minirocket_window = minirocket_window
        self.minirocket_kernels = minirocket_kernels
        self.tskmeans_k = tskmeans_k
        self.tskmeans_period = tskmeans_period
        self.test_size_years = test_size_years
        self.overwrite = overwrite
        self.filter_years = filter_years
        self.output_layout = output_layout.lower().strip()

        if self.output_layout not in ("split", "merged"):
            self.log.warning(
                f"[INIT] output_layout='{output_layout}' desconhecido; usando 'split'"
            )
            self.output_layout = "split"

        self.layer_a = LayerATracker()

        # Fitted transform objects keyed by (scenario_key, method) so that
        # per-method output directories get their own independent model state.
        self._minirocket_models: Dict[str, object] = {}
        self._tskmeans_models: Dict[str, object] = {}

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------
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
    def _aggregate_series(df: pd.DataFrame,
                          cols: List[str]) -> pd.DataFrame:
        """One row per (cidade_norm, _ts) with mean of numeric cols."""
        grp = df.groupby(["cidade_norm", "_ts"], sort=True)
        agg = grp[cols].mean().reset_index()
        return agg

    # ==================================================================
    # METHOD 1: EWMA multiple + lags  (fast, vectorized)
    # ==================================================================
    def _generate_ewma_lags(self, df_agg: pd.DataFrame) -> pd.DataFrame:
        result = df_agg[["cidade_norm", "_ts"]].copy()

        alphas = {"a01": 0.1, "a03": 0.3, "a08": 0.8}
        targets = {
            "precip": COL_PRECIP,
            "temp":   COL_TEMP,
            "umid":   COL_UMID,
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
                result[colname] = series.transform(
                    lambda x: x.shift(lag_h)
                )

        return result

    # ==================================================================
    # METHOD 2: ARIMA  (per-city, refit every H hours)
    # ==================================================================
    def _fit_predict_arima(
        self, series: np.ndarray, order: Tuple[int, int, int]
    ) -> np.ndarray:
        """Fit ARIMA on `series` and return in-sample one-step-ahead preds."""
        try:
            model = SM_ARIMA(series, order=order,
                             enforce_stationarity=False,
                             enforce_invertibility=False)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                res = model.fit(method_kwargs={"maxiter": 100}, disp=False)
            return res.fittedvalues
        except Exception:
            return np.full(len(series), np.nan)

    def _generate_arima(self, df_agg: pd.DataFrame,
                        year: int, is_train: bool) -> pd.DataFrame:
        if not statsmodels_available:
            self.log.warning("[ARIMA] statsmodels não instalado, pulando.")
            return pd.DataFrame()

        result = df_agg[["cidade_norm", "_ts"]].copy()
        result["tsf_arima_precip_pred"]  = np.nan
        result["tsf_arima_precip_resid"] = np.nan

        cities = df_agg["cidade_norm"].unique()
        H = self.refit_hours
        W = self.window_hours
        order = self.arima_order
        ok, fail = 0, 0

        for city in tqdm(cities, desc=f"ARIMA {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            z = city_df[Z_PRIMARY].values.astype(float)
            n = len(z)

            preds = np.full(n, np.nan)

            for start in range(0, n, H):
                end        = min(start + H, n)
                train_start = max(0, start - W)
                train_z    = z[train_start:start]

                if len(train_z) < max(order[0], order[2]) + order[1] + 10:
                    continue

                try:
                    model = SM_ARIMA(train_z, order=order,
                                     enforce_stationarity=False,
                                     enforce_invertibility=False)
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = model.fit(
                            method_kwargs={"maxiter": 100}, disp=False
                        )
                    fc = res.forecast(steps=end - start)
                    preds[start:end] = fc
                    ok += 1
                except Exception as exc:
                    fail += 1
                    if fail <= 5:
                        self.log.debug(
                            f"[ARIMA] city={city} year={year} "
                            f"bloco=[{start}:{end}] erro: {exc}"
                        )

            idx = city_df.index
            result.loc[idx, "tsf_arima_precip_pred"]  = preds
            result.loc[idx, "tsf_arima_precip_resid"] = z - preds

            self.layer_a.add("arima", city, year, z, preds, is_train=is_train)

        self.log.info(f"[ARIMA {year}] blocos ok={ok} fail={fail}")
        return result

    # ==================================================================
    # METHOD 3: SARIMA  (per-city, refit every H hours, seasonal m)
    # ==================================================================
    def _generate_sarima(self, df_agg: pd.DataFrame,
                         year: int, is_train: bool) -> pd.DataFrame:
        if not statsmodels_available:
            self.log.warning("[SARIMA] statsmodels não instalado, pulando.")
            return pd.DataFrame()

        result = df_agg[["cidade_norm", "_ts"]].copy()
        result["tsf_sarima_precip_pred"]  = np.nan
        result["tsf_sarima_precip_resid"] = np.nan

        cities   = df_agg["cidade_norm"].unique()
        H        = self.refit_hours
        W        = self.window_hours
        order    = self.arima_order
        seasonal = (1, 1, 1, self.sarima_m)
        ok, fail = 0, 0

        for city in tqdm(cities, desc=f"SARIMA {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            z = city_df[Z_PRIMARY].values.astype(float)
            n = len(z)
            preds = np.full(n, np.nan)

            for start in range(0, n, H):
                end        = min(start + H, n)
                train_start = max(0, start - W)
                train_z    = z[train_start:start]

                min_obs = self.sarima_m * 2 + 10
                if len(train_z) < min_obs:
                    continue

                try:
                    model = SM_SARIMAX(
                        train_z, order=order,
                        seasonal_order=seasonal,
                        enforce_stationarity=False,
                        enforce_invertibility=False,
                    )
                    with warnings.catch_warnings():
                        warnings.simplefilter("ignore")
                        res = model.fit(maxiter=80, disp=False)
                    fc = res.forecast(steps=end - start)
                    preds[start:end] = fc
                    ok += 1
                except Exception as exc:
                    fail += 1
                    if fail <= 5:
                        self.log.debug(
                            f"[SARIMA] city={city} year={year} "
                            f"bloco=[{start}:{end}] erro: {exc}"
                        )

            idx = city_df.index
            result.loc[idx, "tsf_sarima_precip_pred"]  = preds
            result.loc[idx, "tsf_sarima_precip_resid"] = z - preds

            self.layer_a.add("sarima", city, year, z, preds, is_train=is_train)

        self.log.info(f"[SARIMA {year}] blocos ok={ok} fail={fail}")
        return result

    # ==================================================================
    # METHOD 4: Prophet  (per-city, refit every H hours)
    # Note: Prophet is a classical additive time-series model using
    # Fourier seasonalities and changepoint regression — NOT an LLM.
    # ==================================================================
    def _generate_prophet(self, df_agg: pd.DataFrame,
                          year: int, is_train: bool) -> pd.DataFrame:
        if not prophet_available:
            self.log.warning("[Prophet] prophet não instalado, pulando.")
            return pd.DataFrame()

        result = df_agg[["cidade_norm", "_ts"]].copy()
        result["tsf_prophet_precip_pred"]  = np.nan
        result["tsf_prophet_precip_resid"] = np.nan

        cities   = df_agg["cidade_norm"].unique()
        H        = self.refit_hours
        W        = self.window_hours
        ok, fail = 0, 0

        for city in tqdm(cities, desc=f"Prophet {year}", leave=False):
            mask    = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask].copy()
            z  = city_df[Z_PRIMARY].values.astype(float)
            ts = city_df["_ts"].values
            n  = len(z)
            preds = np.full(n, np.nan)

            for start in range(0, n, H):
                end        = min(start + H, n)
                train_start = max(0, start - W)
                train_ts   = ts[train_start:start]
                train_z    = z[train_start:start]

                if len(train_z) < 48:
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
                    if fail <= 5:
                        self.log.debug(
                            f"[Prophet] city={city} year={year} "
                            f"bloco=[{start}:{end}] erro: {exc}"
                        )

            idx = city_df.index
            result.loc[idx, "tsf_prophet_precip_pred"]  = preds
            result.loc[idx, "tsf_prophet_precip_resid"] = z - preds

            self.layer_a.add("prophet", city, year, z, preds, is_train=is_train)

        self.log.info(f"[Prophet {year}] blocos ok={ok} fail={fail}")
        return result

    # ==================================================================
    # METHOD 5: MiniROCKET  (window transform, fit on train years)
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
            c        = cities[i]
            cs       = city_starts[c]
            local_idx = i - cs
            if local_idx < L:
                continue
            window = vals[i - L: i]
            if np.any(np.isnan(window)):
                continue
            X[i]    = window.T
            valid[i] = True

        return X, valid

    def _generate_minirocket(
        self, df_agg: pd.DataFrame, year: int, is_train: bool,
        model_key: str,
    ) -> pd.DataFrame:
        if not minirocket_available:
            self.log.warning("[MiniROCKET] aeon não instalado, pulando.")
            return pd.DataFrame()

        L = self.minirocket_window
        cols_for_window = [c for c in [COL_PRECIP, COL_TEMP, COL_UMID]
                           if c in df_agg.columns]
        if not cols_for_window:
            return pd.DataFrame()

        self.log.info(
            f"[MiniROCKET {year}] Construindo janelas L={L}, "
            f"canais={len(cols_for_window)}..."
        )
        self._log_memory(f"MiniROCKET {year} pre-janelas")
        X_3d, valid = self._build_windows(df_agg, cols_for_window, L)
        n_valid = int(valid.sum())
        self.log.info(f"[MiniROCKET {year}] {n_valid}/{len(valid)} janelas válidas")

        if n_valid == 0:
            return pd.DataFrame()

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
            return pd.DataFrame()

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
        return result

    # ==================================================================
    # METHOD 6: TS K-Means  (cluster weekly/daily patterns, fit on train)
    # ==================================================================
    def _generate_tskmeans(
        self, df_agg: pd.DataFrame, year: int, is_train: bool,
        model_key: str,
    ) -> pd.DataFrame:
        if not tskmeans_available:
            self.log.warning("[TSKMeans] tslearn não instalado, pulando.")
            return pd.DataFrame()

        P      = self.tskmeans_period
        result = df_agg[["cidade_norm", "_ts"]].copy()
        result["tsf_tskmeans_cluster"] = np.nan

        if Z_PRIMARY not in df_agg.columns:
            return result

        cities        = df_agg["cidade_norm"].unique()
        all_segments: List[np.ndarray] = []
        segment_keys: List[Tuple[str, int]] = []

        for city in cities:
            mask = df_agg["cidade_norm"] == city
            z    = df_agg.loc[mask, Z_PRIMARY].values.astype(float)
            n_seg = len(z) // P
            for s in range(n_seg):
                seg = z[s * P: (s + 1) * P]
                if np.isnan(seg).sum() > P * 0.3:
                    continue
                seg_clean = np.nan_to_num(seg, nan=0.0)
                all_segments.append(seg_clean)
                segment_keys.append((city, s))

        if not all_segments:
            return result

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
            return result

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
        return result

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
    ) -> pd.DataFrame:
        if method == "ewma_lags":
            return self._generate_ewma_lags(df_agg)
        if method == "arima":
            return self._generate_arima(df_agg, year, is_train)
        if method == "sarima":
            return self._generate_sarima(df_agg, year, is_train)
        if method == "prophet":
            return self._generate_prophet(df_agg, year, is_train)
        if method == "minirocket":
            return self._generate_minirocket(df_agg, year, is_train, model_key)
        if method == "tskmeans":
            return self._generate_tskmeans(df_agg, year, is_train, model_key)
        self.log.warning(f"[UNKNOWN METHOD] {method}")
        return pd.DataFrame()

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
            c for c in [COL_PRECIP, COL_TEMP, COL_UMID, COL_VENTO, COL_PRESSAO]
            if c in df.columns
        ]
        df_agg = self._aggregate_series(df, numeric_cols)
        self.log.info(
            f"[AGG] {n_raw} linhas raw -> {len(df_agg)} agregadas "
            f"({df_agg['cidade_norm'].nunique()} cidades)"
        )

        for method in sorted(self.methods):
            out_dir  = self.temporal_fusion_dir / folder_name / method
            out_path = out_dir / src_path.name

            if out_path.exists() and not self.overwrite:
                self.log.info(f"[SKIP] {method}/{out_path.name} já existe")
                continue

            t1 = time.time()
            model_key = self._model_key(scenario_key, method)
            feat = self._generate_method_features(
                method, df_agg, year, is_train, model_key
            )
            elapsed_method = time.time() - t1

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
            c for c in [COL_PRECIP, COL_TEMP, COL_UMID, COL_VENTO, COL_PRESSAO]
            if c in df.columns
        ]
        df_agg = self._aggregate_series(df, numeric_cols)
        self.log.info(
            f"[AGG] {n_raw} linhas raw -> {len(df_agg)} agregadas "
            f"({df_agg['cidade_norm'].nunique()} cidades)"
        )

        feature_dfs: List[pd.DataFrame] = []
        model_key = self._model_key(scenario_key, "merged")

        for method in sorted(self.methods):
            t1 = time.time()
            mk = self._model_key(scenario_key, method)
            feat = self._generate_method_features(
                method, df_agg, year, is_train, mk
            )
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
        self.log.info(f"Layout:    {self.output_layout}")
        self.log.info(f"Métodos:   {sorted(self.methods)}")
        self.log.info(f"Cenários:  {list(self.scenarios.keys())}")
        self.log.info(f"Refit H={self.refit_hours}h, Window W={self.window_hours}h")
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
        eda_dir = Path(self.cfg["paths"]["data"].get("dataset", "data/dataset"))
        layer_a_path = (
            eda_dir.parent / "eda" / "temporal_fusion" / "layer_a_summary.csv"
        )
        self.layer_a.save(layer_a_path)
        self.log.info(f"[Layer A] Salvo em {layer_a_path.parent}/")

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
        help="Famílias de método (padrão: todas)",
    )
    parser.add_argument(
        "--scenarios", nargs="+",
        default=None,
        help=(
            "Chaves de cenário do config.yaml "
            "(padrão: base_D_calculated base_F_calculated)"
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
