# src/article/feature_selection_article.py
# =============================================================================
# CAMADA A — Feature Selection para o pipeline do artigo.
#
# Objetivo:
#   Dados os parquets enriquecidos com features tsf_* em
#   data/_article/1_datasets_with_fusion/{cenario}/{metodo}/, calcula:
#     - Spearman(tsf_col, HAS_FOCO) por feature
#     - Mutual Information(tsf_col, HAS_FOCO) por feature
#   Normaliza |r_spearman| e MI para [0,1], gera score composto (media
#   ponderada), ordena e salva:
#     - data/eda/temporal_fusion/method_ranking_article.csv
#     - data/eda/temporal_fusion/selected_features_article.json
#
# A triagem opera EXCLUSIVAMENTE sobre colunas que comecem com 'tsf_'.
# Colunas originais (*_calculated, coords, biomassa raw) nao sao testadas
# nem removidas.
# =============================================================================
from __future__ import annotations

import gc
import json
import sys
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils  # noqa: E402
from src.article.temporal_fusion_article import (  # noqa: E402
    ALLOWED_METHODS,
    load_fusion_config,
)

try:
    from scipy.stats import spearmanr  # type: ignore
    _scipy_available = True
except ImportError:
    _scipy_available = False

try:
    from sklearn.feature_selection import mutual_info_classif  # type: ignore
    _sklearn_available = True
except ImportError:
    _sklearn_available = False


TARGET_COL = "HAS_FOCO"
TSF_ABS_CLIP = 1e15


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _method_from_feature_name(feat: str) -> str:
    """Tenta inferir o metodo de origem a partir do nome da feature tsf_*."""
    if not feat.startswith("tsf_"):
        return "unknown"
    parts = feat.split("_")
    if len(parts) < 2:
        return "unknown"
    head = parts[1]
    if head == "ewma":
        return "ewma_lags"
    if head == "lag":
        return "ewma_lags"
    if head == "minirocket":
        return "minirocket"
    if head == "sarimax":
        # tsf_sarimax_exog_*
        if len(parts) >= 3 and parts[2] == "exog":
            return "sarimax_exog"
        return "sarimax"
    if head in ("arima", "sarima", "arimax", "prophet", "tskmeans"):
        return head
    return "unknown"


def _target_var_from_feature_name(feat: str) -> str:
    """Tenta extrair o slug alvo (precip, temp, ndvi_buffer, ...) do nome."""
    known_slugs = [
        "precip", "temp", "umid", "rad",
        "ndvi_buffer", "evi_buffer", "ndvi_point", "evi_point",
    ]
    low = feat.lower()
    for slug in known_slugs:
        if f"_{slug}_" in low or low.endswith(f"_{slug}"):
            return slug
    if low.startswith("tsf_minirocket_"):
        return "multicanal"
    return ""


def _discover_train_test_years(
    method_dir: Path, test_size_years: int
) -> Tuple[List[Tuple[int, Path]], List[Tuple[int, Path]], int]:
    """Retorna (train_files, test_files, cut_year) ordenados por ano."""
    files = sorted(method_dir.glob("inmet_bdq_*_cerrado.parquet"))
    years: List[Tuple[int, Path]] = []
    for f in files:
        try:
            y = int(f.stem.split("_")[2])
            years.append((y, f))
        except Exception:
            pass
    years.sort(key=lambda t: t[0])
    if not years:
        return [], [], -1
    if len(years) <= test_size_years:
        cut_year = max(y for y, _ in years) + 1  # tudo treino
        return years, [], cut_year
    cut_year = sorted([y for y, _ in years])[-test_size_years]
    train = [(y, p) for y, p in years if y < cut_year]
    test = [(y, p) for y, p in years if y >= cut_year]
    return train, test, cut_year


def _load_train_tsf_plus_target(
    method_dir: Path,
    train_files: Iterable[Tuple[int, Path]],
    log,
) -> Tuple[pd.DataFrame, pd.Series]:
    """Concatena anos de treino carregando somente colunas tsf_* + HAS_FOCO.

    Usa PyArrow column projection para manter a RAM baixa.
    """
    frames: List[pd.DataFrame] = []
    for year, path in train_files:
        # Descobre colunas via metadado (rapido, nao carrega dados).
        try:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(str(path))
            all_cols = list(pf.schema_arrow.names)
        except Exception:
            # Fallback: le tudo (mais caro, mas robusto)
            all_cols = list(pd.read_parquet(path, engine="pyarrow").columns)

        wanted = [c for c in all_cols if c.startswith("tsf_") or c == TARGET_COL]
        if TARGET_COL not in wanted:
            log.warning(
                f"[selection] {path.name} sem coluna {TARGET_COL}; ignorado."
            )
            continue
        if len([c for c in wanted if c.startswith("tsf_")]) == 0:
            log.warning(
                f"[selection] {path.name} sem colunas tsf_*; ignorado."
            )
            continue

        df = pd.read_parquet(path, columns=wanted, engine="pyarrow")
        tsf_cols = [c for c in wanted if c.startswith("tsf_")]
        if tsf_cols:
            # Sanitizacao defensiva: infinities e magnitudes extremas podem
            # quebrar mutual_info_classif ao converter para float32.
            df[tsf_cols] = df[tsf_cols].replace([np.inf, -np.inf], np.nan)
            df[tsf_cols] = df[tsf_cols].clip(
                lower=-TSF_ABS_CLIP,
                upper=TSF_ABS_CLIP,
            )
        frames.append(df)
        log.info(
            f"[selection] carregado {path.name} ({year}) rows={len(df)} "
            f"cols_tsf={len(wanted) - 1}"
        )

    if not frames:
        raise RuntimeError(
            f"Nenhum parquet valido em {method_dir}. "
            "Rode a etapa de fusion antes da Camada A."
        )

    df_all = pd.concat(frames, axis=0, ignore_index=True)
    del frames
    gc.collect()

    y = df_all[TARGET_COL].astype(np.int8)
    X = df_all.drop(columns=[TARGET_COL])
    return X, y


def _compute_spearman(
    X: pd.DataFrame, y: np.ndarray, log
) -> pd.DataFrame:
    if not _scipy_available:
        raise ImportError(
            "scipy nao instalado; necessario para correlacao de Spearman."
        )
    rows: List[Dict[str, Any]] = []
    yf = y.astype(float)
    for col in X.columns:
        x_col = X[col].to_numpy(dtype=float)
        mask = np.isfinite(x_col) & np.isfinite(yf)
        n_valid = int(mask.sum())
        if n_valid < 30:
            rows.append({
                "feature_name": col,
                "spearman_r": np.nan,
                "spearman_p": np.nan,
                "n_obs": n_valid,
            })
            continue
        r, p = spearmanr(x_col[mask], yf[mask])
        rows.append({
            "feature_name": col,
            "spearman_r": float(r) if np.isfinite(r) else np.nan,
            "spearman_p": float(p) if np.isfinite(p) else np.nan,
            "n_obs": n_valid,
        })
    return pd.DataFrame(rows)


def _compute_mi(
    X: pd.DataFrame,
    y: np.ndarray,
    sample_cutoff: int,
    sample_size: int,
    stratify_col: Optional[np.ndarray],
    log,
    random_state: int = 42,
) -> pd.DataFrame:
    if not _sklearn_available:
        raise ImportError(
            "scikit-learn nao instalado; necessario para mutual_info_classif."
        )
    n = len(X)
    if n >= sample_cutoff and sample_size > 0 and sample_size < n:
        log.info(
            f"[selection] MI: amostrando {sample_size}/{n} linhas "
            f"(stratify_by={'HAS_FOCO' if stratify_col is not None else None})"
        )
        rng = np.random.default_rng(random_state)
        if stratify_col is not None:
            # Amostragem estratificada simples por valor do alvo.
            strata = np.unique(stratify_col)
            idxs: List[np.ndarray] = []
            for s in strata:
                idx_s = np.where(stratify_col == s)[0]
                frac = min(1.0, sample_size / n)
                n_s = max(1, int(round(len(idx_s) * frac)))
                picks = rng.choice(idx_s, size=min(n_s, len(idx_s)), replace=False)
                idxs.append(picks)
            sample_idx = np.concatenate(idxs)
        else:
            sample_idx = rng.choice(n, size=sample_size, replace=False)
        X_s = X.iloc[sample_idx]
        y_s = y[sample_idx]
    else:
        X_s = X
        y_s = y

    # mutual_info_classif nao aceita NaN -> imputamos com mediana por coluna.
    X_filled = X_s.fillna(X_s.median(numeric_only=True))
    # Caso medianas sejam NaN (coluna 100% NaN), substituir por 0.
    X_filled = X_filled.fillna(0.0)

    t0 = time.time()
    mi = mutual_info_classif(
        X_filled.to_numpy(dtype=np.float32),
        y_s.astype(np.int8),
        discrete_features=False,
        n_neighbors=3,
        random_state=random_state,
    )
    log.info(f"[selection] MI calculado em {time.time() - t0:.1f}s")
    return pd.DataFrame({"feature_name": list(X_s.columns), "mi_score": mi})


def _normalize_series(s: pd.Series) -> pd.Series:
    x = s.astype(float)
    maxv = np.nanmax(x.values) if len(x) else 0.0
    if not np.isfinite(maxv) or maxv <= 0:
        return pd.Series(np.zeros(len(x)), index=s.index)
    return x / maxv


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def run_feature_selection(
    scenario_folder: str,
    methods: Optional[List[str]] = None,
    top_k: Optional[int] = None,
    log=None,
) -> Dict[str, Any]:
    """Executa a Camada A.

    - Le parquets de treino para cada metodo em 1_datasets_with_fusion/{cenario}/{metodo}.
    - Concatena features tsf_* e calcula Spearman + MI vs HAS_FOCO.
    - Salva ranking CSV + JSON de features selecionadas.

    Returns:
        dict com metadados da execucao e caminhos dos artefatos.
    """
    fcfg = load_fusion_config()
    methods = [m for m in (methods or fcfg["methods"]) if m in ALLOWED_METHODS]
    if not methods:
        raise ValueError(
            f"Metodos invalidos. Esperados: {sorted(ALLOWED_METHODS)}"
        )

    top_k = int(top_k if top_k is not None else fcfg["top_k"])
    fs_cfg = fcfg.get("feature_selection", {}) or {}
    max_nan_fraction = float(fs_cfg.get("max_nan_fraction", 0.5))
    spearman_weight = float(fs_cfg.get("spearman_weight", 0.5))
    mi_weight = float(fs_cfg.get("mi_weight", 0.5))
    mi_sample_cutoff = int(fs_cfg.get("mi_sample_cutoff", 2_000_000))
    mi_sample_size = int(fs_cfg.get("mi_sample_size", 500_000))
    stratify_by = fs_cfg.get("stratify_by", "HAS_FOCO")

    test_size_years = int(fcfg["test_size_years"])

    log = log or utils.get_logger(
        "article.selection", kind="article", per_run_file=True
    )

    scenario_output_dir = fcfg["output_dir"] / scenario_folder
    if not scenario_output_dir.exists():
        raise FileNotFoundError(
            f"Saida de fusion ausente: {scenario_output_dir}. "
            "Rode a Etapa 1 antes da Camada A."
        )

    eda_dir = fcfg["eda_dir"]
    utils.ensure_dir(eda_dir)

    all_ranking_rows: List[pd.DataFrame] = []
    cut_year_global: Optional[int] = None
    n_train_rows_total = 0

    for method in methods:
        method_dir = scenario_output_dir / method
        if not method_dir.exists():
            log.warning(f"[selection] {method_dir} nao existe; metodo pulado.")
            continue

        train_files, test_files, cut_year = _discover_train_test_years(
            method_dir, test_size_years
        )
        if not train_files:
            log.warning(
                f"[selection] sem anos de treino para {method}; pulando."
            )
            continue
        cut_year_global = cut_year
        log.info(
            f"[selection] {method}: {len(train_files)} anos treino "
            f"({train_files[0][0]}..{train_files[-1][0]}) | "
            f"{len(test_files)} anos teste | cut={cut_year}"
        )

        X, y = _load_train_tsf_plus_target(method_dir, train_files, log)
        n_train_rows_total = max(n_train_rows_total, len(X))
        log.info(f"[selection] {method}: dataframe de treino {X.shape}")

        # Filtro de features com NaN > max_nan_fraction.
        nan_frac = X.isna().mean()
        to_drop = nan_frac[nan_frac > max_nan_fraction].index.tolist()
        if to_drop:
            log.warning(
                f"[selection] {method}: {len(to_drop)} features descartadas "
                f"por NaN>{max_nan_fraction:.0%} (ex.: {to_drop[:3]}...)"
            )
        X_kept = X.drop(columns=to_drop)

        if X_kept.empty:
            log.warning(
                f"[selection] {method}: nenhuma feature tsf_* sobrou apos filtro."
            )
            continue

        # Spearman.
        log.info(f"[selection] {method}: computando Spearman em {X_kept.shape[1]} features...")
        sp_df = _compute_spearman(X_kept, y.to_numpy(), log)

        # Mutual Information.
        log.info(f"[selection] {method}: computando Mutual Information...")
        strat_col = y.to_numpy() if stratify_by == TARGET_COL else None
        mi_df = _compute_mi(
            X_kept,
            y.to_numpy(),
            sample_cutoff=mi_sample_cutoff,
            sample_size=mi_sample_size,
            stratify_col=strat_col,
            log=log,
        )

        # Merge + metadata.
        meth_ranking = sp_df.merge(mi_df, on="feature_name", how="outer")
        meth_ranking["method"] = method
        meth_ranking["target_var"] = meth_ranking["feature_name"].map(
            _target_var_from_feature_name
        )
        meth_ranking["pct_nan"] = meth_ranking["feature_name"].map(
            lambda f: float(nan_frac.get(f, np.nan))
        )

        all_ranking_rows.append(meth_ranking)

        del X, X_kept, y, sp_df, mi_df, meth_ranking
        gc.collect()

    if not all_ranking_rows:
        raise RuntimeError(
            "Nenhum metodo produziu ranking. Rode a etapa de fusion primeiro."
        )

    ranking = pd.concat(all_ranking_rows, axis=0, ignore_index=True)

    # Normalizacao e score composto.
    ranking["spearman_abs"] = ranking["spearman_r"].abs()
    ranking["spearman_abs_norm"] = _normalize_series(ranking["spearman_abs"])
    ranking["mi_norm"] = _normalize_series(ranking["mi_score"])

    w_sum = max(spearman_weight + mi_weight, 1e-9)
    ranking["score_composite"] = (
        spearman_weight * ranking["spearman_abs_norm"].fillna(0.0)
        + mi_weight * ranking["mi_norm"].fillna(0.0)
    ) / w_sum

    ranking = ranking.sort_values("score_composite", ascending=False).reset_index(drop=True)
    ranking["rank"] = np.arange(1, len(ranking) + 1)

    # Colunas finais (ordem amigavel).
    final_cols = [
        "feature_name", "method", "target_var",
        "spearman_r", "spearman_p", "spearman_abs_norm",
        "mi_score", "mi_norm", "score_composite", "rank",
        "n_obs", "pct_nan",
    ]
    ranking = ranking[[c for c in final_cols if c in ranking.columns]]

    ranking_path = eda_dir / "method_ranking_article.csv"
    ranking.to_csv(ranking_path, index=False)
    log.info(f"[selection] ranking salvo em {ranking_path} ({len(ranking)} linhas)")

    # TOP K (respeitando metodos selecionados e filtrando NaN score).
    top = (
        ranking[np.isfinite(ranking["score_composite"])]
        .head(top_k)
        .reset_index(drop=True)
    )

    selected_json_path = eda_dir / "selected_features_article.json"
    payload = {
        "scenario": scenario_folder,
        "methods": methods,
        "top_k": top_k,
        "test_years_cutoff": cut_year_global,
        "n_train_rows": int(n_train_rows_total),
        "weights": {"spearman": spearman_weight, "mi": mi_weight},
        "max_nan_fraction": max_nan_fraction,
        "selected_features": [
            {
                "name": str(r["feature_name"]),
                "method": str(r.get("method", "")),
                "target_var": str(r.get("target_var", "")),
                "score": float(r["score_composite"]),
                "rank": int(r["rank"]),
            }
            for _, r in top.iterrows()
        ],
    }
    with open(selected_json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(
        f"[selection] TOP {top_k} salvos em {selected_json_path}"
    )

    return {
        "ranking_csv": str(ranking_path),
        "selected_json": str(selected_json_path),
        "n_features_total": int(len(ranking)),
        "n_features_selected": int(len(top)),
        "cut_year": cut_year_global,
    }


def load_selected_features(
    eda_dir: Optional[Path] = None,
) -> List[str]:
    """Carrega apenas os nomes das features selecionadas do JSON."""
    fcfg = load_fusion_config()
    eda_dir = eda_dir or fcfg["eda_dir"]
    json_path = Path(eda_dir) / "selected_features_article.json"
    if not json_path.exists():
        raise FileNotFoundError(
            f"Arquivo de features selecionadas nao encontrado: {json_path}. "
            "Rode a Etapa 2 (Camada A)."
        )
    with open(json_path, "r", encoding="utf-8") as f:
        payload = json.load(f)
    return [item["name"] for item in payload.get("selected_features", [])]
