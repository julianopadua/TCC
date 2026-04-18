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
_REDUNDANCY_RNG_SEED = 42


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _abs_spearman_pair(
    a: np.ndarray,
    b: np.ndarray,
    max_sample: int,
    rng: np.random.Generator,
) -> float:
    """|rho| de Spearman entre dois vetores; subsample para limitar RAM."""
    if not _scipy_available:
        return 0.0
    n = min(len(a), len(b))
    if n < 30:
        return 0.0
    a = np.asarray(a[:n], dtype=np.float64)
    b = np.asarray(b[:n], dtype=np.float64)
    m = np.isfinite(a) & np.isfinite(b)
    a = a[m]
    b = b[m]
    if len(a) < 30:
        return 0.0
    if len(a) > max_sample:
        idx = rng.choice(len(a), size=max_sample, replace=False)
        a = a[idx]
        b = b[idx]
    r, _ = spearmanr(a, b)
    return abs(float(r)) if np.isfinite(r) else 0.0


def _greedy_non_redundant(
    ranking: pd.DataFrame,
    method_X: Dict[str, pd.DataFrame],
    redundancy_threshold: float,
    top_k: int,
    max_sample: int,
    log,
) -> Tuple[List[Tuple[str, str]], np.ndarray]:
    """Seleção gulosa por |Spearman| vs features já aceitas."""
    accepted: List[Tuple[str, str]] = []
    n = len(ranking)
    is_redundant = np.zeros(n, dtype=bool)
    rng = np.random.default_rng(_REDUNDANCY_RNG_SEED)

    for pos in range(n):
        row = ranking.iloc[pos]
        fname = str(row["feature_name"])
        meth = str(row["method"])
        if meth not in method_X or fname not in method_X[meth].columns:
            continue

        v = method_X[meth][fname].to_numpy(dtype=np.float64, copy=False)
        redundant = False
        for aname, ameth in accepted:
            if ameth not in method_X or aname not in method_X[ameth].columns:
                continue
            w = method_X[ameth][aname].to_numpy(dtype=np.float64, copy=False)
            if _abs_spearman_pair(v, w, max_sample, rng) >= redundancy_threshold:
                redundant = True
                break

        if redundant:
            is_redundant[pos] = True
            continue

        if len(accepted) < top_k:
            accepted.append((fname, meth))

    log.info(
        f"[selection] greedy redundancia: aceitas={len(accepted)} "
        f"(limite top_k={top_k}) | linhas marcadas redundantes={int(is_redundant.sum())}"
    )
    return accepted, is_redundant


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


def _sanitize_tsf_columns_inplace(df: pd.DataFrame, tsf_cols: List[str]) -> None:
    """Inf/NaN e clip de magnitude, coluna a coluna (float32 para metade da RAM).

    Evita o replace/clip em bloco do pandas, que materializa mascaras booleanas
    (n_cols x n_rows) e estoura RAM em bases largas (ex.: minirocket).
    Usa copy=False quando possivel; copia so se o buffer nao for gravavel.
    """
    lo = np.float32(-TSF_ABS_CLIP)
    hi = np.float32(TSF_ABS_CLIP)
    for c in tsf_cols:
        v = df[c].to_numpy(dtype=np.float32, copy=False)
        if not v.flags.writeable:
            v = v.copy()
        np.clip(v, lo, hi, out=v)
        v[~np.isfinite(v)] = np.nan
        df[c] = v


def _parquet_row_count(path: Path) -> int:
    """Numero de linhas via metadado PyArrow (sem carregar dados)."""
    try:
        import pyarrow.parquet as pq

        return int(pq.ParquetFile(str(path)).metadata.num_rows)
    except Exception:
        return len(pd.read_parquet(path, columns=[TARGET_COL], engine="pyarrow"))


def _concat_train_frames(
    frames: List[pd.DataFrame],
    max_train_rows: int,
    log,
) -> pd.DataFrame:
    """Concatena anos de treino; se exceder teto, amostra proporcionalmente antes do concat.

    O concat monolitico de dezenas de milhoes de linhas forca um bloco contiguo
    grande no BlockManager e estoura RAM em maquinas 16GB.
    """
    total = sum(len(f) for f in frames)
    if total <= max_train_rows:
        if len(frames) == 1:
            return frames[0]
        return pd.concat(frames, axis=0, ignore_index=True, copy=False)

    rng = np.random.RandomState(42)
    parts: List[pd.DataFrame] = []
    for f in frames:
        n = len(f)
        k = max(1, min(n, int(round(n * max_train_rows / total))))
        if k >= n:
            parts.append(f)
        else:
            idx = rng.choice(n, size=k, replace=False)
            parts.append(f.iloc[idx].reset_index(drop=True))
        gc.collect()

    out = pd.concat(parts, axis=0, ignore_index=True, copy=False)
    del parts
    gc.collect()
    if len(out) > max_train_rows:
        out = (
            out.sample(n=max_train_rows, random_state=42).reset_index(drop=True)
        )
    log.info(
        f"[selection] treino downsampled Camada A: {len(out)} linhas materializadas "
        f"(total bruto {total}; teto train_selection_max_rows={max_train_rows})"
    )
    return out


def _nan_fraction_and_high_nan_columns(
    X: pd.DataFrame, max_nan_fraction: float
) -> Tuple[pd.Series, List[str]]:
    """Fracao de NaN por coluna sem `X.isna()` na matriz inteira (evita pico bool n*m)."""
    nan_frac_dict: Dict[str, float] = {}
    to_drop: List[str] = []
    for c in X.columns:
        nf = float(X[c].isna().mean())
        nan_frac_dict[c] = nf
        if nf > max_nan_fraction:
            to_drop.append(c)
    return pd.Series(nan_frac_dict), to_drop


def _load_train_tsf_plus_target(
    method_dir: Path,
    train_files: Iterable[Tuple[int, Path]],
    log,
    max_train_rows: int,
) -> Tuple[pd.DataFrame, pd.Series, int]:
    """Concatena anos de treino carregando somente colunas tsf_* + HAS_FOCO.

    Usa PyArrow column projection para manter a RAM baixa.
    Retorna (X, y, n_linhas_treino_total_parquet) — o terceiro valor e a soma
    dos num_rows dos parquets (antes de qualquer downsample).
    """
    train_list = list(train_files)
    n_rows_full = sum(_parquet_row_count(p) for _, p in train_list)

    frames: List[pd.DataFrame] = []
    for year, path in train_list:
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
        ni = len(df)
        # Downsample por ano ANTES de sanitizar: evita picos de RAM ao acumular
        # dezenas de milhoes de linhas em `frames` + copias float32 na sanitizacao.
        denom = max(n_rows_full, 1)
        target_k = max(1, min(ni, int(round(ni * max_train_rows / denom))))
        if ni > target_k:
            df = df.sample(
                n=target_k,
                random_state=(42 + int(year) * 10007) & 0x7FFFFFFF,
            ).reset_index(drop=True)
            gc.collect()  # libera o dataframe completo do ano antes da sanitizacao

        tsf_cols = [c for c in wanted if c.startswith("tsf_")]
        if tsf_cols:
            # Sanitizacao defensiva: infinities e magnitudes extremas podem
            # quebrar mutual_info_classif ao converter para float32.
            _sanitize_tsf_columns_inplace(df, tsf_cols)
        frames.append(df)
        log.info(
            f"[selection] carregado {path.name} ({year}) rows={len(df)} "
            f"(bruto parquet {ni}) cols_tsf={len(wanted) - 1}"
        )

    if not frames:
        raise RuntimeError(
            f"Nenhum parquet valido em {method_dir}. "
            "Rode a etapa de fusion antes da Camada A."
        )

    df_all = _concat_train_frames(frames, max_train_rows, log)
    del frames
    gc.collect()

    # pop evita reindex interno de drop(), que duplicaria blocos na RAM cheia.
    y = df_all.pop(TARGET_COL).astype(np.int8)
    X = df_all
    gc.collect()
    return X, y, n_rows_full


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
    train_selection_max_rows = int(fs_cfg.get("train_selection_max_rows", 2_500_000))
    mi_sample_cutoff = int(fs_cfg.get("mi_sample_cutoff", 2_000_000))
    mi_sample_size = int(fs_cfg.get("mi_sample_size", 500_000))
    stratify_by = fs_cfg.get("stratify_by", "HAS_FOCO")
    redundancy_threshold = float(fs_cfg.get("redundancy_threshold", 0.85))
    redundancy_max_sample_rows = int(fs_cfg.get("redundancy_max_sample_rows", 200_000))

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
    method_X_kept: Dict[str, pd.DataFrame] = {}
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

        X, y, n_full = _load_train_tsf_plus_target(
            method_dir, train_files, log, train_selection_max_rows
        )
        n_train_rows_total = max(n_train_rows_total, n_full)
        log.info(f"[selection] {method}: dataframe de treino {X.shape}")

        # Filtro de features com NaN > max_nan_fraction (sem isna() na matriz inteira).
        nan_frac, to_drop = _nan_fraction_and_high_nan_columns(X, max_nan_fraction)
        if to_drop:
            log.warning(
                f"[selection] {method}: {len(to_drop)} features descartadas "
                f"por NaN>{max_nan_fraction:.0%} (ex.: {to_drop[:3]}...)"
            )
            X.drop(columns=to_drop, inplace=True)
        X_kept = X
        gc.collect()

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
        method_X_kept[method] = X_kept

        del X, y, sp_df, mi_df, meth_ranking
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

    accepted, red_flags = _greedy_non_redundant(
        ranking,
        method_X_kept,
        redundancy_threshold=redundancy_threshold,
        top_k=top_k,
        max_sample=redundancy_max_sample_rows,
        log=log,
    )
    ranking["is_redundant"] = red_flags.astype(bool)

    # Colunas finais (ordem amigavel).
    final_cols = [
        "feature_name", "method", "target_var",
        "spearman_r", "spearman_p", "spearman_abs_norm",
        "mi_score", "mi_norm", "score_composite", "rank",
        "n_obs", "pct_nan", "is_redundant",
    ]
    ranking = ranking[[c for c in final_cols if c in ranking.columns]]

    ranking_path = eda_dir / "method_ranking_article.csv"
    ranking.to_csv(ranking_path, index=False)
    log.info(f"[selection] ranking salvo em {ranking_path} ({len(ranking)} linhas)")

    selected_rows: List[Dict[str, Any]] = []
    for fname, meth in accepted:
        sub = ranking[
            (ranking["feature_name"] == fname) & (ranking["method"] == meth)
        ]
        if sub.empty:
            continue
        r = sub.iloc[0]
        selected_rows.append(
            {
                "name": str(r["feature_name"]),
                "method": str(r.get("method", "")),
                "target_var": str(r.get("target_var", "")),
                "score": float(r["score_composite"]),
                "rank": int(r["rank"]),
            }
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
        "redundancy_threshold": redundancy_threshold,
        "selected_features": selected_rows,
    }
    with open(selected_json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    log.info(
        f"[selection] {len(selected_rows)} features nao redundantes salvas em {selected_json_path}"
    )

    del method_X_kept
    gc.collect()

    return {
        "ranking_csv": str(ranking_path),
        "selected_json": str(selected_json_path),
        "n_features_total": int(len(ranking)),
        "n_features_selected": int(len(selected_rows)),
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
