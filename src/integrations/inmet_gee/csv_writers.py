# src/integrations/inmet_gee/csv_writers.py
# =============================================================================
# ESCRITORES CSV ATÔMICOS — station_year_locations, spatial_drift_events,
#                           gee_point_validation
# Cada função faz append incremental por ano com escrita atômica (.tmp → replace).
# =============================================================================
# SCHEMA COMPLETO DOS TRÊS ARQUIVOS:
#
# station_year_locations.csv
#   station_uid, ano, lat_median, lon_median, n_obs, n_distinct_coord_pairs,
#   ambiguous_intra_year_coords, cidade_norm, CIDADE, geo_version
#
# spatial_drift_events.csv
#   station_uid, year_from, year_to, lat_from, lon_from, lat_to, lon_to,
#   distance_m, geo_version
#
# gee_point_validation.csv
#   station_uid, year, geo_version, lat, lon, status, message, bands,
#   validated_at
# =============================================================================
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

_STATION_YEAR_COLS = [
    "station_uid", "ano", "lat_median", "lon_median", "n_obs",
    "n_distinct_coord_pairs", "ambiguous_intra_year_coords",
    "cidade_norm", "CIDADE", "geo_version",
]

_DRIFT_COLS = [
    "station_uid", "year_from", "year_to",
    "lat_from", "lon_from", "lat_to", "lon_to",
    "distance_m", "geo_version",
]

_GEE_COLS = [
    "station_uid", "year", "geo_version", "lat", "lon",
    "status", "message", "bands", "validated_at",
]


def _atomic_csv_append(path: Path, new_rows: pd.DataFrame, expected_cols: List[str]) -> None:
    """
    Adiciona novas linhas ao CSV de forma atômica:
      1. Lê o arquivo existente (se houver).
      2. Concatena as novas linhas.
      3. Escreve em .tmp e renomeia atomicamente.
    Garante que o cabeçalho de coluna esteja sempre presente.
    """
    if path.exists():
        existing = pd.read_csv(path, encoding="utf-8", dtype=str)
    else:
        existing = pd.DataFrame(columns=expected_cols)

    combined = pd.concat([existing, new_rows.astype(str)], ignore_index=True)
    # Reordena colunas; colunas extras (não esperadas) ficam no final
    extra = [c for c in combined.columns if c not in expected_cols]
    ordered = [c for c in expected_cols if c in combined.columns] + extra
    combined = combined[ordered]

    tmp = path.with_suffix(".tmp.csv")
    combined.to_csv(tmp, index=False, encoding="utf-8")
    if path.exists():
        bak = path.with_suffix(".bak.csv")
        path.replace(bak)
    os.replace(tmp, path)


def append_station_year(
    path: Path,
    station_df: pd.DataFrame,
    geo_versions: Optional[dict] = None,
) -> None:
    """
    Grava linhas de station_year_locations.csv para um ano processado.
    `geo_versions` é um dict {station_uid: int} com a versão geográfica atual.
    """
    df = station_df.copy()
    if geo_versions:
        df["geo_version"] = df["station_uid"].map(geo_versions).fillna(1).astype(int)
    elif "geo_version" not in df.columns:
        df["geo_version"] = 1

    for col in _STATION_YEAR_COLS:
        if col not in df.columns:
            df[col] = ""

    _atomic_csv_append(path, df[_STATION_YEAR_COLS], _STATION_YEAR_COLS)


def append_drift_events(path: Path, events_df: pd.DataFrame) -> None:
    """Grava eventos de deriva espacial novos (incremental)."""
    if events_df.empty:
        return
    for col in _DRIFT_COLS:
        if col not in events_df.columns:
            events_df = events_df.copy()
            events_df[col] = ""
    _atomic_csv_append(path, events_df[_DRIFT_COLS], _DRIFT_COLS)


def append_gee_validations(path: Path, validation_rows: List[dict]) -> None:
    """Grava resultados de validação GEE (lista de dicts retornados por GeeSampler)."""
    if not validation_rows:
        return
    df = pd.DataFrame(validation_rows)
    df["validated_at"] = datetime.now(timezone.utc).isoformat()
    # Converte lista de bandas para string
    if "bands" in df.columns:
        df["bands"] = df["bands"].apply(
            lambda x: "|".join(x) if isinstance(x, list) else (x or "")
        )
    for col in _GEE_COLS:
        if col not in df.columns:
            df[col] = ""
    _atomic_csv_append(path, df[_GEE_COLS], _GEE_COLS)
