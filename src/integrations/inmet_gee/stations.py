# src/integrations/inmet_gee/stations.py
# =============================================================================
# METADADOS DE ESTAÇÕES — Agregação, station_uid, sanitização
# =============================================================================
from __future__ import annotations

import logging
from typing import Dict, List, Optional, Tuple

import pandas as pd
import numpy as np

# Colunas de coordenadas e identificação esperadas nos parquets
COL_LAT = "LATITUDE"
COL_LON = "LONGITUDE"
COL_ANO = "ANO"
COL_CIDADE = "CIDADE"
COL_CIDADE_NORM = "cidade_norm"

# Limites geográficos aproximados do Brasil (para QC básico de coordenadas)
_LAT_MIN, _LAT_MAX = -35.0, 6.0
_LON_MIN, _LON_MAX = -75.0, -28.0


def build_station_uid(row: pd.Series, id_columns: List[str]) -> str:
    """
    Constrói o identificador lógico da estação a partir das colunas configuradas.
    Por padrão usa 'cidade_norm'. Se a chave for ambígua (homônimos de cidade),
    o pipeline enriquece com coordenadas medianas via enrich_uid_with_coords().
    """
    return "|".join(str(row[c]) for c in id_columns if c in row.index)


def enrich_uid_with_coords(uid: str, lat: float, lon: float) -> str:
    """
    Enriquece o station_uid com coordenadas medianas do primeiro ano observado,
    para desambiguar homônimos de cidade (ex.: duas 'brasilia' em estados diferentes).
    """
    return f"{uid}|{lat:.4f}|{lon:.4f}"


def _sanitize_coords(df: pd.DataFrame, log: logging.Logger, year: int) -> pd.DataFrame:
    """
    Remove linhas com coordenadas inválidas (NaN ou fora dos limites do Brasil).
    Loga WARNING se taxa de descarte for alta (> 1%).
    """
    n_before = len(df)
    mask = (
        df[COL_LAT].notna()
        & df[COL_LON].notna()
        & df[COL_LAT].between(_LAT_MIN, _LAT_MAX)
        & df[COL_LON].between(_LON_MIN, _LON_MAX)
    )
    df_clean = df[mask].copy()
    n_removed = n_before - len(df_clean)

    if n_before > 0 and n_removed / n_before > 0.01:
        log.warning(
            "Ano %d: %d linhas (%.2f%%) removidas por coordenadas inválidas ou fora do Brasil.",
            year, n_removed, 100 * n_removed / n_before,
        )
    elif n_removed > 0:
        log.info(
            "Ano %d: %d linhas removidas por coordenadas inválidas (< 1%% do total).",
            year, n_removed,
        )
    return df_clean


def aggregate_station_year(
    df: pd.DataFrame,
    id_columns: List[str],
    year: int,
    log: logging.Logger,
    jitter_max_m: float = 50.0,
) -> pd.DataFrame:
    """
    Agrega linhas horárias para obter um único registro (mediana lat/lon) por
    (station_uid, ano). Retorna DataFrame com colunas:
      station_uid, ano, lat_median, lon_median, n_obs, n_distinct_coord_pairs,
      ambiguous_intra_year_coords, cidade_norm (primeira ocorrência), CIDADE (primeira)
    """
    from .spatial_drift import haversine_m

    df = _sanitize_coords(df, log, year)
    if df.empty:
        log.warning("Ano %d: DataFrame vazio após sanitização de coordenadas.", year)
        return pd.DataFrame()

    # Parquets podem trazer nomes de coluna duplicados; groupby exige chaves 1-D.
    if df.columns.duplicated().any():
        dup = df.columns[df.columns.duplicated(keep=False)].unique().tolist()
        log.warning(
            "Ano %d: colunas duplicadas no schema (%s). Mantendo a primeira ocorrência de cada nome.",
            year,
            dup[:10],
        )
        df = df.loc[:, ~df.columns.duplicated(keep="first")].copy()

    # Garante coluna de ano
    if COL_ANO not in df.columns:
        df = df.copy()
        df[COL_ANO] = year

    gb_keys = [c for c in id_columns if c in df.columns]
    if not gb_keys:
        log.error(
            "Ano %d: nenhuma coluna de id válida entre %s. Colunas disponíveis: %s.",
            year,
            id_columns,
            list(df.columns)[:30],
        )
        return pd.DataFrame()

    records = []
    for key, group in df.groupby(gb_keys, sort=False):
        uid_base = "|".join(str(k) for k in (key if isinstance(key, tuple) else [key]))

        lat_med = float(np.median(group[COL_LAT].dropna()))
        lon_med = float(np.median(group[COL_LON].dropna()))

        # Conta pares de coordenadas distintos acima do limiar de jitter
        coord_pairs = group[[COL_LAT, COL_LON]].dropna().drop_duplicates()
        n_distinct = 0
        if len(coord_pairs) > 1:
            # Conta pares que estão a mais de jitter_max_m do par mediano
            for _, row in coord_pairs.iterrows():
                if haversine_m(lat_med, lon_med, row[COL_LAT], row[COL_LON]) > jitter_max_m:
                    n_distinct += 1
        ambiguous = n_distinct > 0
        if ambiguous:
            log.warning(
                "Ano %d | Estação '%s': %d par(es) de coordenadas distintos além do jitter "
                "(%.0f m). Flag ambiguous_intra_year_coords=True.",
                year, uid_base, n_distinct, jitter_max_m,
            )

        cidade_norm_val = group[COL_CIDADE_NORM].iloc[0] if COL_CIDADE_NORM in group.columns else ""
        cidade_val = group[COL_CIDADE].iloc[0] if COL_CIDADE in group.columns else ""

        records.append({
            "station_uid": uid_base,
            "ano": year,
            "lat_median": lat_med,
            "lon_median": lon_med,
            "n_obs": len(group),
            "n_distinct_coord_pairs": n_distinct,
            "ambiguous_intra_year_coords": ambiguous,
            "cidade_norm": cidade_norm_val,
            "CIDADE": cidade_val,
        })

    result = pd.DataFrame(records)
    log.info(
        "Ano %d: %d estações agregadas (mediana lat/lon).",
        year, len(result),
    )
    return result
