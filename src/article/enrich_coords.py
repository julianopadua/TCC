# src/article/enrich_coords.py
# =============================================================================
# Etapa 0 — Enriquecimento espacial dos Parquets de modelagem.
#
# Para cada cenário (D, E, F calculated) e cada ano:
#   1. Carrega o Parquet original (preserva todas as colunas).
#   2. Faz left join com BDQueimadas (focos_br_ref_{ANO}.csv) em FOCO_ID →
#      produz lat_foco, lon_foco.
#   3. Faz lookup em station_year_locations.csv por (station_uid, ano) →
#      produz lat_station, lon_station, geo_version.
#   4. Aplica fallback: onde HAS_FOCO==0 ou merge BDQ falhou, as coordenadas
#      do foco recebem as coordenadas da estação naquele ano.
#   5. Gera colunas auxiliares (coord_source_foco, foco_coords_from_bdq).
#   6. Grava Parquet em data/_article/0_datasets_with_coords/{scenario}/.
#
# Todas as anomalias são registradas via IssueLogger.
# =============================================================================
from __future__ import annotations

import gc
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set

import pandas as pd

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.article.config import ArticlePipelineConfig, load_article_config
from src.article.processing_issues import IssueLogger
from src.utils import ensure_dir, get_logger

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
STAGE = "article_0_coords"
PARQUET_TEMPLATE = "inmet_bdq_{year}_cerrado.parquet"
BDQ_TEMPLATE = "focos_br_ref_{year}/focos_br_ref_{year}.csv"

LABEL_COL = "HAS_FOCO"
FOCO_ID_COL = "FOCO_ID"

# Colunas adicionadas por este módulo
NEW_COLS = [
    "lat_foco",
    "lon_foco",
    "lat_station",
    "lon_station",
    "geo_version",
    "coord_source_foco",
    "foco_coords_from_bdq",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _discover_years(scenario_dir: Path) -> List[int]:
    """Descobre anos disponíveis a partir dos Parquets em um diretório."""
    years = []
    for p in sorted(scenario_dir.glob("inmet_bdq_*_cerrado.parquet")):
        m = re.search(r"inmet_bdq_(\d{4})_cerrado", p.stem)
        if m:
            years.append(int(m.group(1)))
    return years


def _load_station_locations(csv_path: Path) -> pd.DataFrame:
    """Carrega station_year_locations.csv com tipos corretos."""
    df = pd.read_csv(csv_path, encoding="utf-8")
    df["ano"] = df["ano"].astype(int)
    df["lat_median"] = pd.to_numeric(df["lat_median"], errors="coerce")
    df["lon_median"] = pd.to_numeric(df["lon_median"], errors="coerce")
    df["geo_version"] = pd.to_numeric(df["geo_version"], errors="coerce").fillna(1).astype(int)
    return df


def _load_bdq_year(bdq_dir: Path, year: int, issues: IssueLogger) -> Optional[pd.DataFrame]:
    """Carrega CSV de referência BDQueimadas para um ano, deduplicando por foco_id."""
    csv_path = bdq_dir / BDQ_TEMPLATE.format(year=year)
    if not csv_path.exists():
        issues.log(
            STAGE, "BDQ_CSV_MISSING",
            f"Arquivo BDQueimadas não encontrado: {csv_path}",
            "SKIPPED_YEAR",
            year=year,
        )
        return None

    df = pd.read_csv(csv_path, encoding="utf-8", usecols=["foco_id", "lat", "lon"])
    df["foco_id"] = df["foco_id"].astype(str).str.strip()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    n_before = len(df)
    df = df.drop_duplicates(subset=["foco_id"], keep="first")
    n_dupes = n_before - len(df)
    if n_dupes > 0:
        issues.log(
            STAGE, "DUPLICATE_FOCO_ID_BDQ",
            f"{n_dupes} foco_id duplicados removidos (keep=first) no ano {year}.",
            "USED_DEDUP_FIRST",
            year=year,
        )
    return df


# ---------------------------------------------------------------------------
# Core: enriquecer um Parquet
# ---------------------------------------------------------------------------
def _enrich_year(
    parquet_path: Path,
    year: int,
    bdq_df: Optional[pd.DataFrame],
    station_locs: pd.DataFrame,
    station_id_cols: List[str],
    issues: IssueLogger,
    log: logging.Logger,
) -> pd.DataFrame:
    """Enriquece um Parquet anual com coordenadas de foco e de estação."""
    df = pd.read_parquet(parquet_path)
    n_original = len(df)
    log.info("  Ano %d — %d linhas lidas de %s", year, n_original, parquet_path.name)

    # --- station_uid ----------------------------------------------------------
    if len(station_id_cols) == 1:
        df["_station_uid"] = df[station_id_cols[0]].astype(str).str.strip()
    else:
        df["_station_uid"] = df[station_id_cols].astype(str).agg("_".join, axis=1)

    # --- Merge BDQueimadas (lat_foco, lon_foco) -------------------------------
    if FOCO_ID_COL in df.columns and bdq_df is not None:
        df[FOCO_ID_COL] = df[FOCO_ID_COL].astype(str).str.strip()
        df = df.merge(
            bdq_df.rename(columns={"lat": "lat_foco", "lon": "lon_foco"}),
            left_on=FOCO_ID_COL,
            right_on="foco_id",
            how="left",
        )
        if "foco_id" in df.columns and "foco_id" != FOCO_ID_COL:
            df.drop(columns=["foco_id"], inplace=True)
    else:
        df["lat_foco"] = pd.NA
        df["lon_foco"] = pd.NA
        if FOCO_ID_COL not in df.columns:
            issues.log(
                STAGE, "FOCO_ID_COL_MISSING",
                f"Coluna {FOCO_ID_COL} ausente no Parquet.",
                "LOGGED_ONLY",
                year=year,
            )

    assert len(df) == n_original, (
        f"Merge BDQ inflou linhas: {n_original} → {len(df)} no ano {year}"
    )

    # --- Lookup estação por ano -----------------------------------------------
    year_locs = station_locs[station_locs["ano"] == year].copy()
    year_locs = year_locs.rename(columns={
        "lat_median": "lat_station",
        "lon_median": "lon_station",
    })
    year_locs["_station_uid"] = year_locs["station_uid"].astype(str).str.strip()

    df = df.merge(
        year_locs[["_station_uid", "lat_station", "lon_station", "geo_version"]],
        on="_station_uid",
        how="left",
    )
    assert len(df) == n_original, (
        f"Merge station_year_locations inflou linhas: {n_original} → {len(df)} no ano {year}"
    )

    n_missing_station = df["lat_station"].isna().sum()
    if n_missing_station > 0:
        uids = df.loc[df["lat_station"].isna(), "_station_uid"].unique()
        issues.log(
            STAGE, "STATION_META_MISSING",
            f"{n_missing_station} linhas sem correspondência em station_year_locations "
            f"(UIDs: {', '.join(uids[:10])}{'...' if len(uids) > 10 else ''}).",
            "LOGGED_ONLY",
            year=year,
        )

    # --- Fallback para LATITUDE/LONGITUDE da linha ----------------------------
    if "LATITUDE" in df.columns and "LONGITUDE" in df.columns:
        needs_row_fb = df["lat_station"].isna()
        if needs_row_fb.any():
            df.loc[needs_row_fb, "lat_station"] = pd.to_numeric(
                df.loc[needs_row_fb, "LATITUDE"], errors="coerce"
            )
            df.loc[needs_row_fb, "lon_station"] = pd.to_numeric(
                df.loc[needs_row_fb, "LONGITUDE"], errors="coerce"
            )
            n_row_fb = needs_row_fb.sum()
            issues.log(
                STAGE, "FALLBACK_ROW_LEVEL_COORDS",
                f"{n_row_fb} linhas usaram LATITUDE/LONGITUDE da própria linha como fallback de estação.",
                "IMPUTED_ROW_COORDS",
                year=year,
            )

    # --- Imputação de lat_foco/lon_foco (regras de fallback) ------------------
    has_foco = df[LABEL_COL] if LABEL_COL in df.columns else pd.Series(0, index=df.index)

    # coord_source_foco: inicializar
    df["coord_source_foco"] = pd.NA
    df["foco_coords_from_bdq"] = pd.NA

    # Caso 1: positivo com coords BDQ reais
    bdq_ok = has_foco.eq(1) & df["lat_foco"].notna() & df["lon_foco"].notna()
    df.loc[bdq_ok, "coord_source_foco"] = "BDQ"
    df.loc[bdq_ok, "foco_coords_from_bdq"] = True

    # Caso 2: positivo sem coords BDQ (merge falhou)
    positive_no_bdq = has_foco.eq(1) & (df["lat_foco"].isna() | df["lon_foco"].isna())
    if positive_no_bdq.any():
        n_failed = positive_no_bdq.sum()
        sample_ids = df.loc[positive_no_bdq, FOCO_ID_COL].head(5).tolist() if FOCO_ID_COL in df.columns else []
        issues.log(
            STAGE, "MERGE_FAILED",
            f"{n_failed} linhas com HAS_FOCO=1 não encontraram foco_id no BDQ "
            f"(amostra: {sample_ids}).",
            "IMPUTED_STATION_COORDS",
            year=year,
        )
        df.loc[positive_no_bdq, "lat_foco"] = df.loc[positive_no_bdq, "lat_station"]
        df.loc[positive_no_bdq, "lon_foco"] = df.loc[positive_no_bdq, "lon_station"]
        df.loc[positive_no_bdq, "coord_source_foco"] = "STATION_FALLBACK_POSITIVE"
        df.loc[positive_no_bdq, "foco_coords_from_bdq"] = False

    # Caso 3: negativo (HAS_FOCO == 0) — sempre imputa com a estação
    negative = has_foco.eq(0)
    df.loc[negative, "lat_foco"] = df.loc[negative, "lat_station"]
    df.loc[negative, "lon_foco"] = df.loc[negative, "lon_station"]
    df.loc[negative, "coord_source_foco"] = "STATION_IMPUTED"
    df.loc[negative, "foco_coords_from_bdq"] = False

    # Limpar coluna auxiliar
    df.drop(columns=["_station_uid"], inplace=True)

    log.info(
        "  Ano %d — enriquecido: %d BDQ ok, %d fallback positivo, %d negativos imputados",
        year,
        bdq_ok.sum(),
        positive_no_bdq.sum() if positive_no_bdq.any() else 0,
        negative.sum(),
    )
    return df


# ---------------------------------------------------------------------------
# Orquestração por cenário
# ---------------------------------------------------------------------------
def enrich_scenario(
    scenario_key: str,
    scenario_folder: str,
    acfg: ArticlePipelineConfig,
    issues: IssueLogger,
    log: logging.Logger,
    years: Optional[List[int]] = None,
    skip_years: Optional[List[int]] = None,
) -> List[Path]:
    """Enriquece todos os anos de um cenário. Retorna paths dos Parquets gerados."""
    source_dir = acfg.modeling_dir / scenario_folder
    if not source_dir.exists():
        log.warning("Diretório do cenário %s não existe: %s", scenario_key, source_dir)
        return []

    out_dir = ensure_dir(acfg.output_root / "0_datasets_with_coords" / scenario_folder)

    available_years = _discover_years(source_dir)
    if years:
        available_years = [y for y in available_years if y in years]
    if skip_years:
        sk = set(skip_years)
        available_years = [y for y in available_years if y not in sk]
    if not available_years:
        log.warning("Nenhum ano a processar para cenário %s", scenario_key)
        return []

    station_locs = _load_station_locations(acfg.station_locations_csv)
    outputs: List[Path] = []

    for year in sorted(available_years):
        pq_src = source_dir / PARQUET_TEMPLATE.format(year=year)
        if not pq_src.exists():
            log.warning("Parquet não encontrado: %s", pq_src)
            continue

        bdq_df = _load_bdq_year(acfg.bdq_processed_dir, year, issues)

        df_enriched = _enrich_year(
            pq_src, year, bdq_df, station_locs,
            acfg.station_id_columns, issues, log,
        )

        out_path = out_dir / PARQUET_TEMPLATE.format(year=year)
        df_enriched.to_parquet(out_path, index=False, engine="pyarrow")
        log.info("  → Gravado: %s (%d linhas)", out_path.name, len(df_enriched))
        outputs.append(out_path)

        del df_enriched, bdq_df
        gc.collect()

    issues.flush()
    return outputs


# ---------------------------------------------------------------------------
# Ponto de entrada
# ---------------------------------------------------------------------------
def run_all(
    years: Optional[List[int]] = None,
    skip_years: Optional[List[int]] = None,
) -> Dict[str, List[Path]]:
    """Executa enriquecimento para todos os cenários configurados."""
    acfg = load_article_config()
    log = get_logger("article.enrich_coords", kind="article", per_run_file=True)
    issues = IssueLogger(ensure_dir(acfg.output_root / "logs") / "processing_issues.csv")

    log.info("=" * 72)
    log.info("ARTICLE PIPELINE — Etapa 0: Enriquecimento de coordenadas")
    log.info("=" * 72)

    effective_years = years or acfg.years or None

    results: Dict[str, List[Path]] = {}
    for key, folder in acfg.scenarios.items():
        log.info("--- Cenário %s (%s) ---", key, folder)
        paths = enrich_scenario(
            key, folder, acfg, issues, log, effective_years, skip_years=skip_years,
        )
        results[key] = paths
        log.info("Cenário %s concluído: %d arquivos gerados.\n", key, len(paths))

    issues.flush()
    log.info("Pipeline de coordenadas finalizado.")
    return results


if __name__ == "__main__":
    run_all()
