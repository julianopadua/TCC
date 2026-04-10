# src/article/gee_biomass.py
# =============================================================================
# Etapa 1 — Extração semanal de biomassa GEE (buffer + ponto) e propagação.
#
# Fluxo:
#   1. Extrair série semanal (MOD13Q1) APENAS na base canônica (E ou F).
#   2. Fazer merge_asof (backward) para alinhar ts_hour → composto semanal.
#   3. Aplicar ffill() por estação para preencher todas as horas.
#   4. Propagar colunas de biomassa para as outras bases (E↔F e D) via
#      merge em (cidade_norm, ts_hour) — sem reextrair no GEE.
#
# A extração GEE propriamente dita é um stub; a implementação real depende
# de credenciais e quota. A arquitetura está pronta para ser conectada.
# =============================================================================
from __future__ import annotations

import gc
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.article.config import ArticlePipelineConfig, load_article_config
from src.article.processing_issues import IssueLogger
from src.utils import ensure_dir, get_logger

STAGE = "gee_biomass"
PARQUET_TEMPLATE = "inmet_bdq_{year}_cerrado.parquet"


# ---------------------------------------------------------------------------
# GEE extraction stub
# ---------------------------------------------------------------------------
def extract_weekly_biomass_gee(
    station_points: pd.DataFrame,
    year: int,
    cfg: ArticlePipelineConfig,
    log: logging.Logger,
) -> Optional[pd.DataFrame]:
    """
    Extrai série semanal de NDVI/EVI do GEE para buffer e ponto.

    Espera `station_points` com colunas:
        station_uid, lat_station, lon_station, lat_foco, lon_foco

    Retorna DataFrame com colunas:
        station_uid, composite_start (datetime), NDVI_buffer, EVI_buffer,
        NDVI_point, EVI_point

    Implementação real: usar ee.ImageCollection(cfg.gee.image_collection),
    filtrar por ano, ee.Geometry.Point / buffer, reduceRegion.
    Este stub retorna None para permitir dry-run sem credenciais GEE.
    """
    log.warning(
        "GEE extraction stub — credenciais ou API não disponíveis. "
        "Retornando None para ano %d. Conecte ee.Initialize() e implemente "
        "a extração real em extract_weekly_biomass_gee().",
        year,
    )
    return None


# ---------------------------------------------------------------------------
# Alinhamento semanal → horário via merge_asof + ffill
# ---------------------------------------------------------------------------
def align_weekly_to_hourly(
    df_hourly: pd.DataFrame,
    df_weekly: pd.DataFrame,
    biomass_cols: List[str],
    station_col: str = "cidade_norm",
    ts_col: str = "ts_hour",
) -> pd.DataFrame:
    """
    Alinha valores semanais de biomassa às linhas horárias de um Parquet.

    Usa merge_asof backward (por estação, ordenado por tempo) para atribuir
    cada ts_hour ao composto semanal mais recente, depois aplica ffill()
    agrupado por estação para propagar o valor por todas as horas.
    """
    df = df_hourly.copy()
    df["_ts"] = pd.to_datetime(df[ts_col])

    weekly = df_weekly.copy()
    weekly["_ts"] = pd.to_datetime(weekly["composite_start"])
    weekly = weekly.sort_values(["_ts"])

    df = df.sort_values([station_col, "_ts"])
    weekly = weekly.sort_values([station_col, "_ts"])

    merged = pd.merge_asof(
        df,
        weekly[[station_col, "_ts"] + biomass_cols],
        on="_ts",
        by=station_col,
        direction="backward",
    )

    for col in biomass_cols:
        merged[col] = merged.groupby(station_col)[col].ffill()

    merged.drop(columns=["_ts"], inplace=True)
    return merged


# ---------------------------------------------------------------------------
# Propagação canônica → outras bases
# ---------------------------------------------------------------------------
def propagate_biomass(
    canonical_df: pd.DataFrame,
    target_parquet: Path,
    biomass_cols: List[str],
    issues: IssueLogger,
    log: logging.Logger,
    year: int,
    station_col: str = "cidade_norm",
    ts_col: str = "ts_hour",
) -> pd.DataFrame:
    """
    Propaga colunas de biomassa da base canônica para outra base (E↔F ou D).

    Faz left merge na chave (cidade_norm, ts_hour). Valida que não haja
    perda/explosão de linhas.
    """
    target_df = pd.read_parquet(target_parquet)
    n_target = len(target_df)

    canon_subset = canonical_df[[station_col, ts_col] + biomass_cols].drop_duplicates(
        subset=[station_col, ts_col]
    )

    result = target_df.merge(canon_subset, on=[station_col, ts_col], how="left")

    if len(result) != n_target:
        issues.log(
            STAGE, "ROWCOUNT_MISMATCH",
            f"Propagação alterou contagem de linhas: {n_target} → {len(result)} "
            f"(ano {year}, arquivo {target_parquet.name}).",
            "LOGGED_ONLY",
            year=year,
        )
        log.warning("ROWCOUNT_MISMATCH no merge de biomassa: %s", target_parquet.name)

    n_missing = result[biomass_cols[0]].isna().sum() if biomass_cols else 0
    if n_missing > 0:
        issues.log(
            STAGE, "BIOMASS_KEY_MISSING",
            f"{n_missing} linhas sem match de biomassa no merge "
            f"(ano {year}, arquivo {target_parquet.name}).",
            "LOGGED_ONLY",
            year=year,
        )

    return result


# ---------------------------------------------------------------------------
# Orquestração por ano
# ---------------------------------------------------------------------------
def _discover_years(directory: Path) -> List[int]:
    years = []
    for p in sorted(directory.glob("inmet_bdq_*_cerrado.parquet")):
        m = re.search(r"inmet_bdq_(\d{4})_cerrado", p.stem)
        if m:
            years.append(int(m.group(1)))
    return years


def run_gee_pipeline(years: Optional[List[int]] = None) -> None:
    """
    Orquestra: extração GEE semanal na canônica → ffill → propagação para D/E/F.

    Se a extração GEE retornar None (stub), registra o aviso e pula o
    alinhamento, sem quebrar o pipeline.
    """
    acfg = load_article_config()
    log = get_logger("article.gee_biomass", kind="article", per_run_file=True)
    issues = IssueLogger(ensure_dir(acfg.output_root / "logs") / "processing_issues.csv")

    log.info("=" * 72)
    log.info("ARTICLE PIPELINE — Etapa 1: GEE Biomassa (semanal → horária)")
    log.info("=" * 72)

    canonical = acfg.gee.canonical_scenario
    canonical_dir = acfg.output_root / "0_datasets_with_coords" / canonical
    if not canonical_dir.exists():
        log.error("Diretório canônico não existe: %s. Execute enrich_coords primeiro.", canonical_dir)
        return

    available = _discover_years(canonical_dir)
    if years:
        available = [y for y in available if y in years]

    bands = acfg.gee.bands
    biomass_cols = [f"{b}_buffer" for b in bands] + [f"{b}_point" for b in bands]

    other_scenarios = {k: v for k, v in acfg.scenarios.items() if v != canonical}

    for year in sorted(available):
        log.info("=== Ano %d ===", year)

        canon_path = canonical_dir / PARQUET_TEMPLATE.format(year=year)
        df_canon = pd.read_parquet(canon_path)

        # Extrair pontos únicos por estação para a chamada GEE
        if "cidade_norm" in df_canon.columns:
            station_points = df_canon.groupby("cidade_norm").agg(
                lat_station=("lat_station", "first"),
                lon_station=("lon_station", "first"),
                lat_foco=("lat_foco", "first"),
                lon_foco=("lon_foco", "first"),
            ).reset_index().rename(columns={"cidade_norm": "station_uid"})
        else:
            station_points = pd.DataFrame()

        weekly = extract_weekly_biomass_gee(station_points, year, acfg, log)

        if weekly is not None:
            weekly = weekly.rename(columns={"station_uid": "cidade_norm"})
            df_canon = align_weekly_to_hourly(
                df_canon, weekly, biomass_cols, station_col="cidade_norm",
            )
            df_canon.to_parquet(canon_path, index=False, engine="pyarrow")
            log.info("  Canônica atualizada com biomassa: %s", canon_path.name)

            # Propagar para as outras bases
            for key, folder in other_scenarios.items():
                target_dir = acfg.output_root / "0_datasets_with_coords" / folder
                target_path = target_dir / PARQUET_TEMPLATE.format(year=year)
                if not target_path.exists():
                    log.warning("  Target não encontrado para cenário %s: %s", key, target_path)
                    continue

                result = propagate_biomass(
                    df_canon, target_path, biomass_cols, issues, log, year,
                )
                result.to_parquet(target_path, index=False, engine="pyarrow")
                log.info("  Propagado para %s: %s (%d linhas)", key, target_path.name, len(result))
                del result
                gc.collect()
        else:
            log.info(
                "  GEE retornou None — biomassa não disponível para ano %d. "
                "Colunas de biomassa não serão adicionadas nesta execução.",
                year,
            )

        del df_canon
        gc.collect()

    issues.flush()
    log.info("Pipeline GEE finalizado.")


if __name__ == "__main__":
    run_gee_pipeline()
