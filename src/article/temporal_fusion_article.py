# src/article/temporal_fusion_article.py
# =============================================================================
# TEMPORAL FUSION FEATURE ENGINEERING — PIPELINE DO ARTIGO
#
# Gera features tsf_* a partir das bases enriquecidas com coordenadas e
# indices de biomassa (NDVI/EVI) em data/_article/0_datasets_with_coords/.
# Saida: data/_article/1_datasets_with_fusion/{cenario}/{metodo}/.
#
# Tres metodos "elite" (ver doc/planos/plano_fusao_article_v1.md):
#   1. ewma_lags    - EWMA multi-alpha + lags horarios (meteo + biomassa)
#   2. sarimax_exog - SARIMAX com precip endog e meteo+NDVI_buffer exog
#   3. minirocket   - Embeddings multicanal (5 canais, janela 168h)
#
# Reutiliza helpers de src/feature_engineering_temporal.py quando relevante,
# mas opera em paths e slugs proprios do artigo (biomassa como variavel
# de primeira classe).
# =============================================================================
from __future__ import annotations

import gc
import inspect
import sys
import time
import warnings
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import numpy as np
import pandas as pd
from tqdm import tqdm

warnings.filterwarnings("ignore", category=FutureWarning)

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils  # noqa: E402
from src.feature_engineering_temporal import (  # noqa: E402
    COL_PRECIP,
    COL_TEMP,
    COL_UMID,
    COL_RAD,
    COL_VENTO,
    COL_PRESSAO,
    TSF_FAIL_DETAIL_LOG_CAP,
)

# ---------------------------------------------------------------------------
# Registro estendido de variaveis do artigo (inclui biomassa GEE).
# ---------------------------------------------------------------------------
COL_NDVI_BUFFER = "NDVI_buffer"
COL_EVI_BUFFER = "EVI_buffer"
COL_NDVI_POINT = "NDVI_point"
COL_EVI_POINT = "EVI_point"

# Slug -> coluna real no parquet.
ARTICLE_VARS_ALL: Dict[str, str] = {
    "precip": COL_PRECIP,
    "temp": COL_TEMP,
    "umid": COL_UMID,
    "rad": COL_RAD,
    "ndvi_buffer": COL_NDVI_BUFFER,
    "evi_buffer": COL_EVI_BUFFER,
}

METEO_SLUGS: List[str] = ["precip", "temp", "umid", "rad"]
BIOMASS_SLUGS: List[str] = ["ndvi_buffer", "evi_buffer"]

ALLOWED_METHODS: Set[str] = {"ewma_lags", "sarimax_exog", "minirocket"}

# Default para transform_chunk_size (config minirocket): o MiniRocket (aeon) materializa
# buffers internos float64 O(n_canais_internos * n_janelas * L); chunks grandes (~225k)
# estouram RAM tipica de notebook. Ver article_pipeline.temporal_fusion.minirocket.
_MINIROCKET_TRANSFORM_CHUNK_DEFAULT = 10_000


def _minirocket_jitter_f32(
    shape: Tuple[int, ...], scale: float, rng: np.random.Generator
) -> np.ndarray:
    """Ruido gaussiano em float32 sem alocar um array float64 do tamanho de `shape`."""
    try:
        noise = rng.standard_normal(shape, dtype=np.float32)
    except TypeError:
        noise = rng.standard_normal(shape).astype(np.float32, copy=False)
    noise *= np.float32(scale)
    return noise

# ---------------------------------------------------------------------------
# Guardrail de memoria — thresholds (% de RAM usada)
# ---------------------------------------------------------------------------
_MEM_PRESSURE_PCT = 90         # pausa submissao de novos workers
_MEM_SERIAL_FALLBACK_PCT = 94  # nem tenta paralelo; vai direto serial
_MEM_RECOVER_PCT = 85          # restaura paralelismo apos estabilizacao


# ---------------------------------------------------------------------------
# Worker function (module-level para ser picklavel no Windows/spawn)
# ---------------------------------------------------------------------------
def _sarimax_city_worker(
    z_endog: np.ndarray,
    z_exog: np.ndarray,
    order: tuple,
    seasonal_order: tuple,
    H: int,
    W: int,
    min_train: int,
) -> Tuple[np.ndarray, int, int, int, dict]:
    """SARIMAX rolling forecast para uma cidade. Roda em processo filho."""
    import warnings as _w
    from statsmodels.tsa.statespace.sarimax import SARIMAX as _SARIMAX

    n = len(z_endog)
    preds = np.full(n, np.nan)
    ok = fail = skipped = 0
    fail_types: dict = {}

    for start in range(0, n, H):
        end = min(start + H, n)
        train_start = max(0, start - W)

        tr_endog = z_endog[train_start:start]
        tr_exog = z_exog[train_start:start, :]

        valid = np.isfinite(tr_endog) & np.all(np.isfinite(tr_exog), axis=1)
        tr_endog_c = tr_endog[valid]
        tr_exog_c = tr_exog[valid, :]

        if len(tr_endog_c) < min_train:
            skipped += 1
            continue

        exog_future = np.tile(tr_exog_c[-1:, :], (end - start, 1))

        try:
            model = _SARIMAX(
                tr_endog_c,
                exog=tr_exog_c,
                order=order,
                seasonal_order=seasonal_order,
                trend="n",
                enforce_stationarity=False,
                enforce_invertibility=False,
            )
            with _w.catch_warnings():
                _w.simplefilter("ignore")
                res = model.fit(maxiter=80, disp=False)
            fc = res.forecast(steps=end - start, exog=exog_future)
            preds[start:end] = fc
            ok += 1
        except Exception as exc:
            fail += 1
            key = exc.__class__.__name__
            fail_types[key] = fail_types.get(key, 0) + 1

    return preds, ok, fail, skipped, fail_types


# Dependencias opcionais
_statsmodels_available = False
try:
    from statsmodels.tsa.statespace.sarimax import SARIMAX as SM_SARIMAX  # noqa: E402
    _statsmodels_available = True
except ImportError:
    pass

_minirocket_available = False
try:
    from aeon.transformations.collection.convolution_based import MiniRocket  # noqa: E402
    _minirocket_available = True
except ImportError:
    pass


# ============================================================================
# Carregador de configuracao (le config.yaml -> article_pipeline.temporal_fusion)
# ============================================================================
def load_fusion_config() -> Dict[str, Any]:
    cfg = utils.loadConfig()
    article_cfg = cfg.get("article_pipeline", {}) or {}
    fusion_cfg = article_cfg.get("temporal_fusion", {}) or {}
    if not fusion_cfg:
        raise ValueError(
            "Bloco 'article_pipeline.temporal_fusion' ausente em config.yaml. "
            "Ver doc/planos/plano_fusao_article_v1.md para template."
        )

    output_root = Path(cfg["paths"]["data"]["article"])
    eda_base = Path(cfg["paths"]["data"].get("dataset", "data/dataset")).parent / "eda"

    return {
        "scenarios": article_cfg.get("scenarios", {}) or {},
        "output_root": output_root,
        "input_dir": output_root / fusion_cfg.get("input_subdir", "0_datasets_with_coords"),
        "output_dir": output_root / fusion_cfg.get("output_subdir", "1_datasets_with_fusion"),
        "results_dir": output_root / fusion_cfg.get("results_subdir", "results"),
        "eda_dir": eda_base / fusion_cfg.get("eda_subdir", "temporal_fusion"),
        "default_scenario": fusion_cfg.get("default_scenario", "E"),
        "test_size_years": int(fusion_cfg.get("test_size_years", 2)),
        "top_k": int(fusion_cfg.get("top_k", 50)),
        "methods": list(fusion_cfg.get("methods", sorted(ALLOWED_METHODS))),
        "ewma_lags": fusion_cfg.get("ewma_lags", {}) or {},
        "sarimax_exog": fusion_cfg.get("sarimax_exog", {}) or {},
        "minirocket": fusion_cfg.get("minirocket", {}) or {},
        "feature_selection": fusion_cfg.get("feature_selection", {}) or {},
        "raw": fusion_cfg,
    }


def resolve_scenario(scenario_arg: str, scenarios_map: Dict[str, str]) -> str:
    """Aceita 'E', 'base_E' ou o folder name completo. Retorna o folder name."""
    s = scenario_arg.strip()
    if s in scenarios_map:
        return scenarios_map[s]
    upper = s.upper()
    if upper in scenarios_map:
        return scenarios_map[upper]
    if s in scenarios_map.values():
        return s
    raise ValueError(
        f"Cenario '{scenario_arg}' nao resolve em article_pipeline.scenarios "
        f"({scenarios_map})."
    )


# ============================================================================
# Engenharia de features tsf_*
# ============================================================================
class ArticleTemporalFusion:
    """Orquestra os 3 metodos elite sobre os parquets do artigo."""

    def __init__(
        self,
        scenario_folder: str,
        methods: Optional[List[str]] = None,
        overwrite: bool = False,
        filter_years: Optional[List[int]] = None,
        test_size_years: Optional[int] = None,
        sarimax_workers: Optional[int] = None,
        log=None,
    ) -> None:
        self.fcfg = load_fusion_config()
        self.scenario_folder = scenario_folder
        self.methods = [m for m in (methods or self.fcfg["methods"]) if m in ALLOWED_METHODS]
        if not self.methods:
            raise ValueError(
                f"Nenhum metodo valido selecionado. Escolha entre: {sorted(ALLOWED_METHODS)}"
            )
        self.overwrite = overwrite
        self.filter_years = filter_years
        self.test_size_years = (
            test_size_years if test_size_years is not None else self.fcfg["test_size_years"]
        )

        self.log = log or utils.get_logger(
            "article.fusion", kind="article", per_run_file=True
        )

        self.input_dir = self.fcfg["input_dir"] / scenario_folder
        self.output_dir = self.fcfg["output_dir"] / scenario_folder

        if not self.input_dir.exists():
            raise FileNotFoundError(
                f"Diretorio de entrada nao existe: {self.input_dir}"
            )

        # Modelos treinados reutilizados entre anos de teste (MiniRocket).
        self._minirocket_model = None
        mr_cfg = self.fcfg.get("minirocket", {}) or {}
        self._minirocket_transform_chunk = max(
            256,
            int(
                mr_cfg.get(
                    "transform_chunk_size",
                    _MINIROCKET_TRANSFORM_CHUNK_DEFAULT,
                )
            ),
        )

        # Numero de workers para sarimax_exog (CLI > config > 1).
        if sarimax_workers is not None:
            self._sarimax_workers = max(1, sarimax_workers)
        else:
            self._sarimax_workers = max(
                1, int(self.fcfg["sarimax_exog"].get("workers", 1))
            )

        # Orcamento de falhas com detalhe em WARNING.
        self._fail_detail_remaining = 0

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _get_mem_used_pct(self) -> float:
        """Retorna % de RAM usada (0.0 se psutil indisponivel)."""
        try:
            import psutil
            return psutil.virtual_memory().percent
        except Exception:
            return 0.0

    def _reset_fail_budget(self) -> None:
        self._fail_detail_remaining = TSF_FAIL_DETAIL_LOG_CAP

    def _log_block_failure(
        self,
        method_tag: str,
        year: int,
        detail: str,
        exc: BaseException,
    ) -> None:
        msg = f"{detail} | {exc.__class__.__name__}: {exc}"
        self.log.debug(f"[{method_tag}] {msg}")
        if self._fail_detail_remaining <= 0:
            return
        idx = TSF_FAIL_DETAIL_LOG_CAP - self._fail_detail_remaining + 1
        self._fail_detail_remaining -= 1
        self.log.warning(
            f"[{method_tag}] falha {idx}/{TSF_FAIL_DETAIL_LOG_CAP} "
            f"(year={year}) {msg}",
            exc_info=(idx == 1),
        )

    def _log_memory(self, ctx: str) -> None:
        try:
            import os
            import psutil
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

    def _discover_years(self) -> List[Tuple[int, Path]]:
        files = sorted(self.input_dir.glob("inmet_bdq_*_cerrado.parquet"))
        out: List[Tuple[int, Path]] = []
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
        df = df.copy()
        df["_ts"] = pd.to_datetime(df["ts_hour"])
        return df.sort_values(["cidade_norm", "_ts"]).reset_index(drop=True)

    @staticmethod
    def _aggregate_series(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
        """Uma linha por (cidade_norm, _ts) com media de colunas numericas.

        Biomassa e meteo sao agregadas juntas; biomassa chega na base ja
        em resolucao horaria (merge_asof + ffill em gee_biomass.py).
        """
        grp = df.groupby(["cidade_norm", "_ts"], sort=True)
        return grp[cols].mean().reset_index()

    def _active_slugs(self, df_agg: pd.DataFrame, wanted: List[str]) -> Dict[str, str]:
        return {
            slug: ARTICLE_VARS_ALL[slug]
            for slug in wanted
            if slug in ARTICLE_VARS_ALL and ARTICLE_VARS_ALL[slug] in df_agg.columns
        }

    def _log_nan_stats(self, df_agg: pd.DataFrame, year: int) -> None:
        n = len(df_agg)
        parts: List[str] = []
        for slug, col in ARTICLE_VARS_ALL.items():
            if col in df_agg.columns:
                pct = df_agg[col].isna().mean() * 100
                parts.append(f"{slug}={pct:.1f}%")
        self.log.info(
            f"[NAN] {self.scenario_folder} year={year} n_agg={n} | "
            + (" ".join(parts) if parts else "sem colunas de interesse")
        )

    # ==================================================================
    # Metodo 1 — EWMA + Lags (vetorizado, meteo + biomassa)
    # ==================================================================
    def _generate_ewma_lags(self, df_agg: pd.DataFrame) -> pd.DataFrame:
        cfg = self.fcfg["ewma_lags"]
        meteo_vars = cfg.get("meteo_vars", METEO_SLUGS)
        biomass_vars = cfg.get("biomass_vars", BIOMASS_SLUGS)
        meteo_lags = cfg.get("meteo_lags_h", [1, 24, 168])
        biomass_lags = cfg.get("biomass_lags_h", [168, 336])
        alphas = cfg.get("alphas", {"a01": 0.1, "a03": 0.3, "a08": 0.8}) or {}

        result = df_agg[["cidade_norm", "_ts"]].copy()

        active_meteo = self._active_slugs(df_agg, meteo_vars)
        active_biomass = self._active_slugs(df_agg, biomass_vars)
        all_targets: List[Tuple[str, str, List[int]]] = []
        for slug, col in active_meteo.items():
            all_targets.append((slug, col, list(meteo_lags)))
        for slug, col in active_biomass.items():
            all_targets.append((slug, col, list(biomass_lags)))

        if not all_targets:
            self.log.warning("[ewma_lags] nenhuma variavel ativa encontrada.")
            return result

        n_added = 0
        for slug, col, lags in all_targets:
            series = df_agg.groupby("cidade_norm", sort=False)[col]
            for aname, alpha in alphas.items():
                colname = f"tsf_ewma_{slug}_{aname}"
                result[colname] = series.transform(
                    lambda x, a=alpha: x.ewm(alpha=a, adjust=False).mean()
                ).values
                n_added += 1
            for lag_h in lags:
                colname = f"tsf_lag_{slug}_{int(lag_h)}h"
                result[colname] = series.transform(lambda x, lh=lag_h: x.shift(lh)).values
                n_added += 1

        self.log.info(
            f"[ewma_lags] {n_added} features geradas "
            f"(meteo={list(active_meteo.keys())}, biomassa={list(active_biomass.keys())})"
        )
        return result

    # ==================================================================
    # Metodo 2 — SARIMAX com biomassa como exogena
    #            Suporta execucao paralela por cidade (ProcessPoolExecutor).
    # ==================================================================
    def _generate_sarimax_exog(
        self, df_agg: pd.DataFrame, year: int
    ) -> pd.DataFrame:
        if not _statsmodels_available:
            self.log.warning("[sarimax_exog] statsmodels nao instalado, pulando.")
            return pd.DataFrame()

        cfg = self.fcfg["sarimax_exog"]
        endog_slug = cfg.get("endog", "precip")
        exog_slugs = cfg.get("exog", ["temp", "umid", "rad", "ndvi_buffer"])

        active = self._active_slugs(df_agg, [endog_slug] + list(exog_slugs))
        endog_col = active.get(endog_slug)
        if endog_col is None:
            self.log.warning(
                f"[sarimax_exog] endog '{endog_slug}' ausente; pulando."
            )
            return pd.DataFrame()

        exog_items = [(s, active[s]) for s in exog_slugs if s in active]
        if not exog_items:
            self.log.warning(
                "[sarimax_exog] nenhuma exog disponivel (esperadas: "
                f"{exog_slugs}). Pulando."
            )
            return pd.DataFrame()

        exog_cols = [c for _, c in exog_items]
        exog_slug_list = [s for s, _ in exog_items]
        order = tuple(cfg.get("order", [2, 1, 2]))
        seasonal_order = tuple(cfg.get("seasonal_order", [1, 1, 1, 24]))
        H = int(cfg.get("refit_hours", 336))
        W = int(cfg.get("window_hours", 720))
        n_workers = self._sarimax_workers

        self.log.info(
            f"[sarimax_exog {year}] endog={endog_slug} exog={exog_slug_list} "
            f"order={order} seasonal={seasonal_order} H={H}h W={W}h "
            f"workers={n_workers}"
        )

        self._reset_fail_budget()

        pred_col = f"tsf_sarimax_exog_{endog_slug}_pred"
        resid_col = f"tsf_sarimax_exog_{endog_slug}_resid"
        result = df_agg[["cidade_norm", "_ts"]].copy()
        result[pred_col] = np.nan
        result[resid_col] = np.nan

        cities = df_agg["cidade_norm"].unique()
        n_exog = len(exog_cols)
        min_train = max(seasonal_order[3] * 2, order[0] + order[2]) + order[1] + n_exog + 10

        # --- preparar payloads por cidade (arrays numpy, pickle-safe) ---
        city_data: List[dict] = []
        for city in cities:
            mask = df_agg["cidade_norm"] == city
            city_df = df_agg.loc[mask]
            city_data.append({
                "city": city,
                "idx": city_df.index.to_numpy(),
                "z_endog": city_df[endog_col].to_numpy(dtype=float),
                "z_exog": city_df[exog_cols].to_numpy(dtype=float),
            })

        worker_args = (order, seasonal_order, H, W, min_train)

        ok = fail = skipped = 0
        fail_types: Counter = Counter()

        # --- decidir modo de execucao ---
        use_parallel = n_workers > 1 and len(city_data) > 1

        if use_parallel:
            mem_pct = self._get_mem_used_pct()
            if mem_pct > _MEM_SERIAL_FALLBACK_PCT:
                self.log.warning(
                    f"[sarimax_exog] memoria={mem_pct:.0f}% >= "
                    f"{_MEM_SERIAL_FALLBACK_PCT}%; forcando modo serial."
                )
                use_parallel = False

        if use_parallel:
            self.log.info(
                f"[sarimax_exog {year}] modo PARALELO "
                f"({n_workers} workers, {len(city_data)} cidades)"
            )
            try:
                from concurrent.futures import (
                    ProcessPoolExecutor,
                    wait,
                    FIRST_COMPLETED,
                )

                with ProcessPoolExecutor(max_workers=n_workers) as pool:
                    pending: Dict[Any, dict] = {}
                    queue = list(city_data)
                    pbar = tqdm(
                        total=len(city_data),
                        desc=f"sarimax_exog {year}",
                        leave=False,
                    )

                    inflight_limit = n_workers
                    while queue or pending:
                        mem_pct = self._get_mem_used_pct()
                        new_limit = 1 if mem_pct > _MEM_PRESSURE_PCT else n_workers
                        if mem_pct <= _MEM_RECOVER_PCT:
                            new_limit = n_workers
                        if new_limit != inflight_limit:
                            self.log.warning(
                                f"[sarimax_exog GUARDRAIL] mem={mem_pct:.0f}% "
                                f"limite_concorrencia {inflight_limit}->{new_limit}"
                            )
                            inflight_limit = new_limit

                        # Submeter ate preencher workers livres
                        while queue and len(pending) < inflight_limit:
                            cd = queue.pop(0)
                            fut = pool.submit(
                                _sarimax_city_worker,
                                cd["z_endog"],
                                cd["z_exog"],
                                *worker_args,
                            )
                            pending[fut] = cd

                        if not pending:
                            break

                        done, _ = wait(
                            pending, timeout=300, return_when=FIRST_COMPLETED,
                        )
                        if not done:
                            continue

                        for fut in done:
                            cd = pending.pop(fut)
                            try:
                                preds, c_ok, c_fail, c_skip, c_ft = fut.result()
                                idx_arr = cd["idx"]
                                result.loc[idx_arr, pred_col] = preds
                                result.loc[idx_arr, resid_col] = (
                                    cd["z_endog"] - preds
                                )
                                ok += c_ok
                                fail += c_fail
                                skipped += c_skip
                                fail_types.update(c_ft)
                            except Exception as exc:
                                self.log.error(
                                    f"[sarimax_exog] worker crash "
                                    f"city={cd['city']}: "
                                    f"{exc.__class__.__name__}: {exc}"
                                )
                            pbar.update(1)

                    pbar.close()

            except Exception as exc:
                self.log.error(
                    f"[sarimax_exog] pool falhou "
                    f"({exc.__class__.__name__}: {exc}); fallback serial."
                )
                result[pred_col] = np.nan
                result[resid_col] = np.nan
                ok = fail = skipped = 0
                fail_types = Counter()
                use_parallel = False

        if not use_parallel:
            if n_workers > 1:
                self.log.info(
                    f"[sarimax_exog {year}] modo SERIAL (fallback)"
                )
            for cd in tqdm(city_data, desc=f"sarimax_exog {year}", leave=False):
                preds, c_ok, c_fail, c_skip, c_ft = _sarimax_city_worker(
                    cd["z_endog"], cd["z_exog"], *worker_args,
                )
                result.loc[cd["idx"], pred_col] = preds
                result.loc[cd["idx"], resid_col] = cd["z_endog"] - preds
                ok += c_ok
                fail += c_fail
                skipped += c_skip
                fail_types.update(c_ft)

        pct_finite = float(np.isfinite(result[pred_col].to_numpy()).mean() * 100)
        breakdown = ""
        if fail > 0:
            breakdown = " | fail_types: " + ", ".join(
                f"{k}:{v}" for k, v in fail_types.most_common()
            )
        self.log.info(
            f"[sarimax_exog {year}] ok={ok} fail={fail} skipped={skipped} "
            f"pct_finite={pct_finite:.1f}%{breakdown}"
        )
        return result

    # ==================================================================
    # Metodo 3 — MiniRocket multicanal
    # ==================================================================
    def _build_windows(
        self, df_agg: pd.DataFrame, cols: List[str], L: int
    ) -> Tuple[np.ndarray, np.ndarray]:
        n = len(df_agg)
        C = len(cols)
        X = np.full((n, C, L), np.nan, dtype=np.float32)
        valid = np.zeros(n, dtype=bool)

        cities = df_agg["cidade_norm"].to_numpy()
        vals = df_agg[cols].to_numpy(dtype=np.float32)

        city_starts: Dict[Any, int] = {}
        prev = object()
        for i in range(n):
            c = cities[i]
            if c != prev:
                city_starts[c] = i
                prev = c

        for i in range(n):
            c = cities[i]
            cs = city_starts[c]
            local_idx = i - cs
            if local_idx < L:
                continue
            window = vals[i - L:i]
            if np.any(np.isnan(window)):
                continue
            X[i] = window.T
            valid[i] = True

        return X, valid

    def _prepare_df_agg(self, src_path: Path) -> pd.DataFrame:
        df_raw = pd.read_parquet(src_path)
        df = self._parse_ts(df_raw)
        numeric_cols = [
            c for c in [
                COL_PRECIP, COL_TEMP, COL_UMID, COL_RAD, COL_VENTO, COL_PRESSAO,
                COL_NDVI_BUFFER, COL_EVI_BUFFER, COL_NDVI_POINT, COL_EVI_POINT,
            ]
            if c in df.columns
        ]
        df_agg = self._aggregate_series(df, numeric_cols)
        del df_raw, df
        gc.collect()
        return df_agg

    def _minirocket_channel_columns(
        self, df_agg: pd.DataFrame
    ) -> Optional[List[str]]:
        cfg = self.fcfg["minirocket"]
        channels_slugs = cfg.get(
            "channels",
            ["precip", "temp", "umid", "ndvi_buffer", "evi_buffer"],
        )
        active = self._active_slugs(df_agg, channels_slugs)
        if len(active) < 2:
            return None
        return list(active.values())

    @staticmethod
    def _minirocket_subsample_windows(
        X: np.ndarray, max_n: int, rng: np.random.Generator
    ) -> np.ndarray:
        n = len(X)
        if n <= max_n:
            return X
        idx = rng.choice(n, size=max_n, replace=False)
        return np.ascontiguousarray(X[idx])

    def _minirocket_fit_global(
        self, train_year_files: List[Tuple[int, Path]]
    ) -> None:
        if not _minirocket_available:
            self.log.warning("[minirocket] aeon nao instalado; fit global omitido.")
            return
        if not train_year_files:
            self.log.warning("[minirocket] fit global: nenhum ano de treino.")
            return

        cfg = self.fcfg["minirocket"]
        L = int(cfg.get("window", 168))
        n_kernels = int(cfg.get("n_kernels", 168))
        random_state = int(cfg.get("random_state", 42))
        max_per_year = int(cfg.get("fit_max_windows_per_year", 10_000))
        n_jobs_cfg = cfg.get("n_jobs", None)
        rng = np.random.default_rng(random_state)

        chunks: List[np.ndarray] = []
        for year, src_path in train_year_files:
            df_agg = self._prepare_df_agg(src_path)
            self._log_nan_stats(df_agg, year)
            cols_for_window = self._minirocket_channel_columns(df_agg)
            if not cols_for_window:
                self.log.warning(
                    f"[minirocket fit global {year}] menos de 2 canais; pulando ano."
                )
                del df_agg
                gc.collect()
                continue

            self.log.info(
                f"[minirocket fit global {year}] janelas L={L} "
                f"canais={cols_for_window}"
            )
            self._log_memory(f"minirocket fit global {year} pre-janelas")
            X_3d, valid = self._build_windows(df_agg, cols_for_window, L)
            del df_agg
            gc.collect()

            X_valid = X_3d[valid]
            del X_3d
            gc.collect()

            n_valid = int(len(X_valid))
            self.log.info(
                f"[minirocket fit global {year}] {n_valid} janelas validas "
                f"(cap {max_per_year}/ano)"
            )
            if n_valid == 0:
                continue

            X_sub = self._minirocket_subsample_windows(
                X_valid, max_per_year, rng
            )
            del X_valid
            gc.collect()
            chunks.append(X_sub)
            del X_sub
            gc.collect()

        if not chunks:
            self.log.warning(
                "[minirocket] fit global: nenhuma janela valida em anos de treino."
            )
            return

        X_train_global = np.concatenate(chunks, axis=0)
        del chunks
        gc.collect()
        self.log.info(
            f"[minirocket] fit global: {len(X_train_global)} janelas agregadas, "
            f"fit(n_kernels={n_kernels})..."
        )
        self._log_memory("minirocket fit global pre-fit")

        mr_kwargs: Dict[str, Any] = {
            "n_kernels": n_kernels,
            "random_state": random_state,
        }
        if n_jobs_cfg is not None:
            try:
                sig = inspect.signature(MiniRocket.__init__)
                if "n_jobs" in sig.parameters:
                    mr_kwargs["n_jobs"] = n_jobs_cfg
            except Exception:
                pass

        mr = MiniRocket(**mr_kwargs)
        X_train_global += _minirocket_jitter_f32(X_train_global.shape, 1e-5, rng)
        mr.fit(X_train_global)
        del X_train_global
        gc.collect()

        self._minirocket_model = mr
        self._log_memory("minirocket fit global pos-fit")

    def _generate_minirocket(self, df_agg: pd.DataFrame, year: int) -> pd.DataFrame:
        if not _minirocket_available:
            self.log.warning("[minirocket] aeon nao instalado, pulando.")
            return pd.DataFrame()

        cfg = self.fcfg["minirocket"]
        L = int(cfg.get("window", 168))

        cols_for_window = self._minirocket_channel_columns(df_agg)
        if not cols_for_window:
            self.log.warning(
                f"[minirocket] menos de 2 canais ativos no ano {year}; pulando."
            )
            return pd.DataFrame()

        self.log.info(
            f"[minirocket {year}] construindo janelas L={L} transform-only"
        )
        self._log_memory(f"minirocket {year} pre-janelas")
        X_3d, valid = self._build_windows(df_agg, cols_for_window, L)
        n_valid = int(valid.sum())
        self.log.info(f"[minirocket {year}] {n_valid}/{len(valid)} janelas validas")

        if n_valid == 0:
            return pd.DataFrame()

        X_valid = X_3d[valid]
        del X_3d
        gc.collect()

        if self._minirocket_model is None:
            self.log.warning(
                "[minirocket] modelo ausente (fit global nao executado ou falhou); "
                "pulando."
            )
            return pd.DataFrame()

        self._log_memory(f"minirocket {year} pre-transform")
        mr = self._minirocket_model
        valid_idx = np.flatnonzero(valid)
        n_windows = int(X_valid.shape[0])

        probe = mr.transform(np.ascontiguousarray(X_valid[:1]))
        n_feat = int(probe.shape[1])
        del probe
        gc.collect()

        feat_cols = [f"tsf_minirocket_f{i:03d}" for i in range(n_feat)]
        base = df_agg[["cidade_norm", "_ts"]].copy()
        nan_block = np.full((len(base), n_feat), np.nan, dtype=np.float32)
        feat_block = pd.DataFrame(
            nan_block, index=base.index, columns=feat_cols
        )
        result = pd.concat([base, feat_block], axis=1)
        del base, nan_block, feat_block
        gc.collect()

        feat_j = result.columns.get_indexer(feat_cols)
        chunk_sz = self._minirocket_transform_chunk
        rng_tr = np.random.default_rng(int(cfg.get("random_state", 42)))
        for start in range(0, n_windows, chunk_sz):
            end = min(start + chunk_sz, n_windows)
            X_chunk = np.ascontiguousarray(X_valid[start:end])
            X_chunk += _minirocket_jitter_f32(X_chunk.shape, 1e-5, rng_tr)
            transformed = mr.transform(X_chunk)
            out_f32 = np.asarray(transformed, dtype=np.float32)
            del transformed, X_chunk
            rows = valid_idx[start:end]
            result.iloc[rows, feat_j] = out_f32
            del out_f32
            gc.collect()

        del X_valid
        gc.collect()

        self.log.info(f"[minirocket {year}] {n_feat} embeddings gerados")
        self._log_memory(f"minirocket {year} pos-transform")
        return result

    # ==================================================================
    # Roteamento
    # ==================================================================
    def _generate(
        self, method: str, df_agg: pd.DataFrame, year: int, is_train: bool
    ) -> pd.DataFrame:
        if method == "ewma_lags":
            return self._generate_ewma_lags(df_agg)
        if method == "sarimax_exog":
            return self._generate_sarimax_exog(df_agg, year)
        if method == "minirocket":
            return self._generate_minirocket(df_agg, year)
        self.log.warning(f"[UNKNOWN METHOD] {method}")
        return pd.DataFrame()

    # ==================================================================
    # Merge de features de volta para o df original
    # ==================================================================
    @staticmethod
    def _merge_back(df: pd.DataFrame, feat_df: pd.DataFrame) -> pd.DataFrame:
        if feat_df is None or feat_df.empty:
            return df.drop(columns=["_ts"], errors="ignore")
        idx = ["cidade_norm", "_ts"]
        tsf_cols = [c for c in feat_df.columns if c not in idx]
        right = feat_df.set_index(idx)[tsf_cols]
        df_out = df.join(right, on=idx, how="left")
        del right
        gc.collect()
        df_out = df_out.drop(columns=["_ts"], errors="ignore")
        return df_out

    # ==================================================================
    # Ano
    # ==================================================================
    def _process_year(
        self, year: int, src_path: Path, is_train: bool
    ) -> None:
        t0 = time.time()
        self.log.info(
            f"[PROCESS] {self.scenario_folder} / {year} | src={src_path.name} "
            f"| train={is_train}"
        )

        # Checa se todos os metodos-alvo ja existem (evita leitura desnecessaria).
        out_paths = {
            m: self.output_dir / m / src_path.name for m in self.methods
        }
        if not self.overwrite and all(p.exists() for p in out_paths.values()):
            self.log.info(
                f"[SKIP] todos os metodos ({self.methods}) ja existem para {year}; "
                f"use --overwrite para refazer."
            )
            return

        df_raw = pd.read_parquet(src_path)
        self._log_memory(f"pos-read {year}")
        n_raw = len(df_raw)
        df = self._parse_ts(df_raw)

        numeric_cols = [
            c for c in [
                COL_PRECIP, COL_TEMP, COL_UMID, COL_RAD, COL_VENTO, COL_PRESSAO,
                COL_NDVI_BUFFER, COL_EVI_BUFFER, COL_NDVI_POINT, COL_EVI_POINT,
            ]
            if c in df.columns
        ]
        df_agg = self._aggregate_series(df, numeric_cols)
        self.log.info(
            f"[AGG] {n_raw} linhas raw -> {len(df_agg)} agregadas "
            f"({df_agg['cidade_norm'].nunique()} cidades)"
        )
        self._log_nan_stats(df_agg, year)

        for method in self.methods:
            out_path = out_paths[method]
            if out_path.exists() and not self.overwrite:
                self.log.info(f"[SKIP] {method}/{out_path.name} ja existe")
                continue

            t1 = time.time()
            try:
                feat = self._generate(method, df_agg, year, is_train)
            except Exception as exc:
                self.log.error(
                    f"[{method} {year}] falha fatal: "
                    f"{exc.__class__.__name__}: {exc}",
                    exc_info=True,
                )
                continue
            elapsed = time.time() - t1

            if feat is None or feat.empty:
                self.log.info(
                    f"[{method} {year}] sem features geradas em {elapsed:.1f}s"
                )
                continue

            df_out = self._merge_back(df, feat)
            utils.ensure_dir(out_path.parent)
            df_out.to_parquet(out_path, index=False)
            self.log.info(
                f"[SAVED] {method}/{out_path.name} | {len(df_out)} linhas "
                f"| metodo {elapsed:.1f}s"
            )
            self._log_memory(f"pos-save {method}/{year}")

            del df_out, feat
            gc.collect()

        del df_raw, df, df_agg
        gc.collect()
        self.log.info(
            f"[DONE] {self.scenario_folder}/{year} | total {time.time() - t0:.0f}s"
        )

    # ==================================================================
    # Entry point
    # ==================================================================
    def run(self) -> None:
        self.log.info("=" * 70)
        self.log.info("ARTICLE TEMPORAL FUSION")
        self.log.info(f"Cenario:   {self.scenario_folder}")
        self.log.info(f"Metodos:   {self.methods}")
        self.log.info(f"SARIMAX workers: {self._sarimax_workers}")
        self.log.info(f"Input:     {self.input_dir}")
        self.log.info(f"Output:    {self.output_dir}")
        self.log.info(f"Overwrite: {self.overwrite}")
        self.log.info("=" * 70)
        self._log_memory("inicio run")

        year_files = self._discover_years()
        if not year_files:
            self.log.warning(
                f"Nenhum parquet encontrado em {self.input_dir}"
            )
            return

        years = [y for y, _ in year_files]
        if len(years) <= self.test_size_years:
            cut_year = max(years) + 1  # tudo treino
        else:
            cut_year = sorted(years)[-self.test_size_years]
        self.log.info(
            f"{len(year_files)} anos: {years[0]}..{years[-1]} "
            f"| corte teste >= {cut_year}"
        )

        # Roda anos ordenados para que MiniRocket treine antes do teste.
        year_files.sort(key=lambda t: t[0])

        if "minirocket" in self.methods:
            train_year_files = [
                (y, p) for y, p in year_files if y < cut_year
            ]
            self._minirocket_fit_global(train_year_files)
            del train_year_files
            gc.collect()

        for year, src_path in year_files:
            is_train = year < cut_year
            self._process_year(year, src_path, is_train)

        self.log.info("ARTICLE TEMPORAL FUSION - concluido.")
        self._log_memory("fim run")


# ============================================================================
# Funcao de alto nivel para uso pelo orquestrador
# ============================================================================
def run_article_fusion(
    scenario_folder: str,
    methods: Optional[List[str]] = None,
    overwrite: bool = False,
    years: Optional[List[int]] = None,
    test_size_years: Optional[int] = None,
    sarimax_workers: Optional[int] = None,
    log=None,
) -> None:
    """Executa fusao temporal no cenario especificado.

    Args:
        scenario_folder: folder name (ex: 'base_E_with_rad_knn_calculated').
        methods: subset de ALLOWED_METHODS. None = todos os configurados.
        overwrite: regravar parquets existentes.
        years: subset de anos; None = todos descobertos.
        test_size_years: ultimos N anos reservados para teste (MiniRocket nao fita).
        sarimax_workers: override de workers para sarimax_exog (None = config).
        log: logger externo (opcional).
    """
    engine = ArticleTemporalFusion(
        scenario_folder=scenario_folder,
        methods=methods,
        overwrite=overwrite,
        filter_years=years,
        test_size_years=test_size_years,
        sarimax_workers=sarimax_workers,
        log=log,
    )
    engine.run()
