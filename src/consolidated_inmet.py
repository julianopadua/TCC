# src/consolidate_inmet.py
# =============================================================================
# INMET — CONSOLIDAÇÃO incremental (processed/INMET/inmet_{ano}.csv -> consolidated/INMET)
# Junta os CSVs em lotes de 3 arquivos por vez, anexando ao arquivo final.
# Dep.: pandas, utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import re
import pandas as pd
import csv
import sys

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
)

# Permite campos muito longos quando o fallback usa engine="python"
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# -----------------------------------------------------------------------------
# [SEÇÃO 1] PATHS
# -----------------------------------------------------------------------------
def get_inmet_processed_dir() -> Path:
    return get_path("paths", "providers", "inmet", "processed")

def get_consolidated_root() -> Path:
    return get_path("paths", "data", "external")

def get_inmet_consolidated_dir() -> Path:
    return ensure_dir(Path(get_consolidated_root()) / "INMET")

def default_output_path() -> Path:
    return get_inmet_consolidated_dir() / "inmet_all_years.csv"

# -----------------------------------------------------------------------------
# [SEÇÃO 2] DESCOBERTA
# -----------------------------------------------------------------------------
_INMET_FILE_RE = re.compile(r"^inmet_(\d{4})\.csv$", flags=re.IGNORECASE)

def parse_year_from_filename(filename: str) -> Optional[int]:
    m = _INMET_FILE_RE.match(filename)
    return int(m.group(1)) if m else None

def list_inmet_year_files(processed_dir: Path) -> List[Tuple[int, Path]]:
    pairs: List[Tuple[int, Path]] = []
    for p in sorted(processed_dir.glob("inmet_*.csv")):
        y = parse_year_from_filename(p.name)
        if y is not None and p.is_file():
            pairs.append((y, p))
    pairs.sort(key=lambda x: x[0])
    return pairs

# -----------------------------------------------------------------------------
# [SEÇÃO 3] IO & CONSISTÊNCIA
# -----------------------------------------------------------------------------
def _read_header_cols(csv_path: Path, encoding: str = "utf-8") -> List[str]:
    # usa engine C primeiro (rápido); se der ruim, cai para python
    try:
        return list(pd.read_csv(csv_path, nrows=0, encoding=encoding).columns)
    except Exception:
        return list(pd.read_csv(csv_path, nrows=0, encoding=encoding, engine="python").columns)

def _read_year_df(csv_path: Path, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Leitura resiliente:
      1) tenta engine padrão (C);
      2) fallback para engine="python" + on_bad_lines="skip".
    """
    try:
        return pd.read_csv(csv_path, encoding=encoding)
    except Exception:
        return pd.read_csv(
            csv_path,
            encoding=encoding,
            engine="python",
            on_bad_lines="skip",
        )

def _reindex_like(df: pd.DataFrame, ref_cols: List[str]) -> pd.DataFrame:
    for c in ref_cols:
        if c not in df.columns:
            df[c] = pd.NA
    # ignora colunas extras e ordena conforme ref
    return df[ref_cols]

def _batched(lst: List[Tuple[int, Path]], n: int = 3) -> Iterable[List[Tuple[int, Path]]]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# -----------------------------------------------------------------------------
# [SEÇÃO 4] CONSOLIDAÇÃO (LOTE DE 3)
# -----------------------------------------------------------------------------
def consolidate_inmet(
    output_filename: str = "inmet_all_years.csv",
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 3,
) -> Path:
    """
    Consolida CSVs anuais (processed/INMET/inmet_{ano}.csv) em um único CSV
    (consolidated/INMET/<output_filename>), processando em lotes de `batch_size`.
    """
    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    cfg = loadConfig()

    processed_dir = get_inmet_processed_dir()
    out_dir = get_inmet_consolidated_dir()
    out_path = out_dir / output_filename

    all_year_files = list_inmet_year_files(processed_dir)
    if years:
        yrs = {int(y) for y in years}
        year_files = [(y, p) for (y, p) in all_year_files if y in yrs]
    else:
        year_files = all_year_files

    if not year_files:
        raise FileNotFoundError("Nenhum inmet_{ano}.csv encontrado para consolidar.")

    # Colunas de referência
    _, first_path = year_files[0]
    ref_cols = _read_header_cols(first_path, encoding=encoding)
    log.info(f"[REFCOLS] {len(ref_cols)} colunas")

    # Saída
    ensure_dir(out_path.parent)
    if out_path.exists():
        if overwrite:
            out_path.unlink()
        else:
            log.info(f"[SKIP] {out_path.name} já existe. Use overwrite=True para refazer.")
            return out_path

    total_rows = 0
    first_write = True
    log.info(f"[CONSOLIDATE] {len(year_files)} arquivo(s) em lotes de {batch_size} -> {out_path}")

    for batch in _batched(year_files, batch_size):
        anos = [y for y, _ in batch]
        log.info(f"[BATCH] anos={anos}")

        frames: List[pd.DataFrame] = []
        for y, path in batch:
            log.info(f"  [READ] {path.name}")
            df = _read_year_df(path, encoding=encoding)
            if list(df.columns) != ref_cols:
                log.warning(f"  [WARN] colunas diferentes em {path.name}; reindexando.")
                df = _reindex_like(df, ref_cols)
            frames.append(df)

        if not frames:
            continue

        batch_df = pd.concat(frames, ignore_index=True)
        batch_rows = len(batch_df)

        batch_df.to_csv(
            out_path,
            mode="w" if first_write else "a",
            index=False,
            header=first_write,
            encoding=encoding,
        )
        first_write = False
        total_rows += batch_rows
        log.info(f"  [APPEND] +{batch_rows} linhas (acumulado: {total_rows})")

        # libera memória cedo
        del batch_df, frames

    log.info(f"[DONE] {out_path} (linhas totais: {total_rows})")
    return out_path

# -----------------------------------------------------------------------------
# [SEÇÃO 5] MAIN
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    try:
        out = consolidate_inmet(
            output_filename="inmet_all_years.csv",
            years=None,        # ou [2000, 2001, ...] para filtrar
            overwrite=True,    # sobrescreve se já existir
            encoding="utf-8",
            batch_size=3,      # junta de 3 em 3
        )
        log.info(f"[DONE] Consolidado em: {out}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na consolidação: {e}")
