# src/consolidate_inmet.py
# =============================================================================
# INMET — CONSOLIDAÇÃO incremental (processed/INMET/inmet_{ano}.csv -> consolidated/INMET)
# Junta os CSVs em lotes de 3 arquivos por vez, anexando ao arquivo final.
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import re
import csv
import sys

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    normalize_key,
)

# Permite campos muito longos para o parser csv do Python (caso necessário)
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
# [SEÇÃO 3] HELPERS (STREAMING SEM PANDAS)
# -----------------------------------------------------------------------------
def _batched(lst: List[Tuple[int, Path]], n: int = 3) -> Iterable[List[Tuple[int, Path]]]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def _read_first_line(p: Path, encoding: str = "utf-8") -> str:
    """Lê a 1ª linha (header) de um CSV exatamente como está no arquivo."""
    with p.open("r", encoding=encoding, errors="replace", newline="") as fh:
        return fh.readline()

def _append_csv_skip_header(src: Path, dst_fh, encoding: str = "utf-8") -> int:
    """
    Copia o conteúdo de `src` para um arquivo de destino já aberto, pulando a 1ª linha (header).
    Retorna o nº de linhas copiadas.
    """
    rows = 0
    with src.open("r", encoding=encoding, errors="replace", newline="") as s:
        _ = s.readline()  # skip header
        for line in s:
            dst_fh.write(line)
            rows += 1
    return rows

def _normalize_dates_text_inplace(csv_path: Path, encoding: str = "utf-8") -> None:
    """
    Passada final: substitui '/' por '-' SOMENTE no primeiro campo (coluna DATA) de cada linha.
    Não usa parser CSV (robusto a aspas malformadas). Mantém o header intocado.
    """
    tmp = csv_path.with_suffix(".tmp")
    with csv_path.open("r", encoding=encoding, errors="replace", newline="") as r, \
         tmp.open("w", encoding=encoding, newline="") as w:
        first = True
        for line in r:
            if first:
                w.write(line)      # preserva o header original do 1º arquivo
                first = False
                continue
            idx = line.find(",")   # assume DATA é a 1ª coluna
            if idx > 0:
                token = line[:idx]
                if "/" in token:
                    token = token.replace("/", "-")
                line = token + line[idx:]
            w.write(line)
    tmp.replace(csv_path)

# -----------------------------------------------------------------------------
# [SEÇÃO 4] CONSOLIDAÇÃO (LOTE DE 3, HEADER DO 1º ARQUIVO)
# -----------------------------------------------------------------------------
def consolidate_inmet(
    output_filename: str = "inmet_all_years.csv",
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 3,
    normalize_dates: bool = True,  # padroniza DATA para YYYY-MM-DD no final
) -> Path:
    """
    Consolida CSVs anuais (processed/INMET/inmet_{ano}.csv) em um único CSV
    (consolidated/INMET/<output_filename>), processando em lotes de `batch_size`.
    - Usa o header do 1º arquivo apenas.
    - Demais arquivos são "colados" sem header (linha 1 é pulada).
    - Sem reindex/rename; é colagem pura.
    """
    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    _ = loadConfig()  # garante paths resolvidos

    processed_dir = get_inmet_processed_dir()
    out_dir = get_inmet_consolidated_dir()
    out_path = out_dir / output_filename

    year_files = list_inmet_year_files(processed_dir)
    if years:
        yrs = {int(y) for y in years}
        year_files = [(y, p) for (y, p) in year_files if y in yrs]

    if not year_files:
        raise FileNotFoundError("Nenhum inmet_{ano}.csv encontrado para consolidar.")

    # Header do 1º arquivo
    _, first_path = year_files[0]
    header = _read_first_line(first_path, encoding=encoding)

    # Saída
    ensure_dir(out_path.parent)
    if out_path.exists():
        if overwrite:
            out_path.unlink()
        else:
            log.info(f"[SKIP] {out_path.name} já existe. Use overwrite=True para refazer.")
            return out_path

    total_rows = 0
    log.info(f"[CONSOLIDATE] {len(year_files)} arquivo(s) em lotes de {batch_size} -> {out_path}")

    # Abre o destino uma vez, grava header do 1º arquivo
    with out_path.open("w", encoding=encoding, newline="") as out_fh:
        out_fh.write(header)

        for batch in _batched(year_files, batch_size):
            anos = [y for y, _ in batch]
            log.info(f"[BATCH] anos={anos}")
            for y, path in batch:
                log.info(f"  [APPEND] {path.name}")
                added = _append_csv_skip_header(path, out_fh, encoding=encoding)
                total_rows += added
                log.info(f"    +{added} linhas (acumulado: {total_rows})")

    if normalize_dates:
        log.info("[NORMALIZE] Padronizando DATA para YYYY-MM-DD (primeira coluna)...")
        _normalize_dates_text_inplace(out_path, encoding=encoding)

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
            normalize_dates=True,
        )
        log.info(f"[DONE] Consolidado em: {out}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na consolidação: {e}")
