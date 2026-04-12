# src/integrations/inmet_gee/io_parquet.py
# =============================================================================
# I/O PARQUET — Descoberta de anos e leitura colunar robusta com retry
# =============================================================================
from __future__ import annotations

import re
import time
import logging
from pathlib import Path
from typing import List, Optional

import pandas as pd
import pyarrow.parquet as pq

PARQUET_PATTERN = re.compile(r"inmet_bdq_(\d{4})_cerrado\.parquet")


def discover_years(parquet_dir: Path) -> List[int]:
    """
    Lista os anos disponíveis em um diretório de parquets, ordenados
    de forma crescente. Retorna lista vazia se o diretório não existir.
    """
    if not parquet_dir.exists():
        return []
    years = []
    for f in parquet_dir.iterdir():
        m = PARQUET_PATTERN.match(f.name)
        if m:
            years.append(int(m.group(1)))
    return sorted(years)


def parquet_path_for_year(parquet_dir: Path, year: int) -> Path:
    return parquet_dir / f"inmet_bdq_{year}_cerrado.parquet"


def read_parquet_columns(
    path: Path,
    columns: List[str],
    log: logging.Logger,
    max_attempts: int = 3,
    base_delay: float = 5.0,
    max_delay: float = 120.0,
    cast_float32: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Lê colunas selecionadas de um parquet com retry exponencial.
    Retorna None após esgotar tentativas (erro já logado).
    Colunas ausentes no arquivo são ignoradas com WARNING.
    """
    attempt = 0
    delay = base_delay
    while attempt < max_attempts:
        attempt += 1
        try:
            schema = pq.read_schema(path)
            available = set(schema.names)
            missing = [c for c in columns if c not in available]
            cols_to_read = [c for c in columns if c in available]

            if missing:
                log.warning(
                    "Arquivo '%s': %d coluna(s) ausente(s) no schema — ignoradas: %s",
                    path.name, len(missing), missing,
                )

            if not cols_to_read:
                log.error(
                    "Arquivo '%s': nenhuma coluna solicitada existe. Leitura cancelada.",
                    path.name,
                )
                return None

            df = pd.read_parquet(path, columns=cols_to_read)

            if cast_float32:
                num_cols = df.select_dtypes(include="number").columns
                df[num_cols] = df[num_cols].astype("float32")

            log.info(
                "Arquivo '%s' lido com sucesso: %d linhas, %d colunas.",
                path.name, len(df), len(df.columns),
            )
            return df

        except (OSError, FileNotFoundError, Exception) as exc:
            if attempt < max_attempts:
                log.warning(
                    "Falha ao ler '%s' (tentativa %d/%d): %s. "
                    "Iniciando Exponential Backoff: aguardando %.0fs.",
                    path.name, attempt, max_attempts, exc, delay,
                )
                time.sleep(delay)
                delay = min(delay * 2, max_delay)
            else:
                log.error(
                    "Falha irrecuperável ao ler '%s' após %d tentativas: %s",
                    path.name, max_attempts, exc,
                )
    return None
