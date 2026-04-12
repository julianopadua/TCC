# src/article/viz/data_loader.py
"""Leitura em cache de Parquets do artigo (subconjunto de colunas)."""
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Sequence, Tuple

import pandas as pd
import pyarrow.parquet as pq
import streamlit as st


@st.cache_data(show_spinner="A ler schema…")
def parquet_column_names(parquet_path_str: str) -> Tuple[str, ...]:
    p = Path(parquet_path_str)
    if not p.is_file():
        return tuple()
    try:
        return tuple(pq.ParquetFile(p).schema_arrow.names)
    except Exception:
        return tuple()


@st.cache_data(show_spinner="A carregar dados…")
def list_cities_in_parquet(parquet_path_str: str) -> List[str]:
    p = Path(parquet_path_str)
    if not p.is_file():
        return []
    df = pd.read_parquet(p, columns=["cidade_norm"])
    return sorted(df["cidade_norm"].dropna().unique().tolist())


@st.cache_data(show_spinner="A carregar dados…")
def load_parquet_columns(
    parquet_path_str: str,
    columns: Tuple[str, ...],
) -> pd.DataFrame:
    """Lê só colunas pedidas (tupla para hash estável no cache)."""
    p = Path(parquet_path_str)
    if not p.is_file():
        return pd.DataFrame()
    cols = list(columns)
    try:
        return pd.read_parquet(p, columns=cols)
    except Exception:
        # coluna em falta — ler o que existir
        import pyarrow.parquet as pq

        schema = pq.ParquetFile(p).schema_arrow
        names = set(schema.names)
        ok = [c for c in cols if c in names]
        if not ok:
            return pd.DataFrame()
        return pd.read_parquet(p, columns=ok)


def filter_cities(
    df: pd.DataFrame,
    city_col: str,
    cities: Optional[Sequence[str]],
    all_cities: bool,
) -> pd.DataFrame:
    if df.empty:
        return df
    if all_cities or cities is None:
        return df
    if not cities:
        return df.iloc[0:0].copy()
    return df[df[city_col].isin(cities)].copy()


def estimate_rows_warning(n_rows: int, threshold: int = 1_000_000) -> Optional[str]:
    if n_rows > threshold:
        return (
            f"Aviso: ~{n_rows:,} linhas carregadas. Reduza cidades ou anos se o browser ficar lento."
        )
    return None


def concat_years(
    frames: List[pd.DataFrame],
) -> pd.DataFrame:
    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    return out
