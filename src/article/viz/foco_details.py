# src/article/viz/foco_details.py
"""Contagem e tabela de eventos com foco (HAS_FOCO)."""
from __future__ import annotations

from typing import List

import pandas as pd

from src.article.viz.variables import CITY_COL, LABEL_COL, TS_COL, VIZ_YEAR_COL

_OPTIONAL_DETAIL_COLS = ("FOCO_ID", "lat_foco", "lon_foco")


def count_focos(df: pd.DataFrame) -> int:
    if df.empty or LABEL_COL not in df.columns:
        return 0
    s = df[LABEL_COL]
    if s.dtype == bool:
        return int(s.sum())
    sn = pd.to_numeric(s, errors="coerce")
    return int((sn == 1).sum())


def build_foco_events_table(df: pd.DataFrame, max_rows: int = 2000) -> pd.DataFrame:
    if df.empty or LABEL_COL not in df.columns:
        return pd.DataFrame()
    s = df[LABEL_COL]
    if s.dtype == bool:
        mask = s
    else:
        sn = pd.to_numeric(s, errors="coerce")
        mask = sn == 1
    sub = df.loc[mask].copy()
    if sub.empty:
        return pd.DataFrame()
    sub = sub.sort_values(TS_COL)
    cols: List[str] = [TS_COL]
    if CITY_COL in sub.columns and sub[CITY_COL].nunique() > 1:
        cols.append(CITY_COL)
    if VIZ_YEAR_COL in sub.columns:
        cols.append(VIZ_YEAR_COL)
    for c in _OPTIONAL_DETAIL_COLS:
        if c in sub.columns:
            cols.append(c)
    out = sub[cols].head(max_rows).copy()
    out[TS_COL] = pd.to_datetime(out[TS_COL], errors="coerce")
    return out
