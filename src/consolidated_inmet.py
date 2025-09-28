# src/consolidate_inmet.py
# =============================================================================
# INMET — CONSOLIDAÇÃO (processed/INMET/inmet_{ano}.csv -> consolidated/INMET)
# Depende de: pandas, utils.py (loadConfig, get_logger, get_path, ensure_dir)
# -----------------------------------------------------------------------------
# Este script NÃO limpa/transforma a base: apenas consolida todos os CSVs anuais
# já gerados em processed/INMET em um único arquivo final.
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Dict
import re
import pandas as pd
import csv

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
)

# -----------------------------------------------------------------------------
# [SEÇÃO 1] PARÂMETROS E PATHS
# -----------------------------------------------------------------------------
def get_inmet_processed_dir() -> Path:
    """Retorna o diretório processed/INMET conforme config.yaml (resolvido)."""
    return get_path("paths", "providers", "inmet", "processed")

def get_consolidated_root() -> Path:
    """Retorna o diretório base de consolidação (paths.data.external)."""
    return get_path("paths", "data", "external")

def get_inmet_consolidated_dir() -> Path:
    """Retorna o diretório consolidated/INMET e o garante existente."""
    base = get_consolidated_root()
    return ensure_dir(Path(base) / "INMET")

def default_output_path() -> Path:
    """Caminho padrão do CSV consolidado do INMET."""
    return get_inmet_consolidated_dir() / "inmet_all_years.csv"

# -----------------------------------------------------------------------------
# [SEÇÃO 2] DESCOBERTA DE ARQUIVOS E ANOS
# -----------------------------------------------------------------------------
_INMET_FILE_RE = re.compile(r"^inmet_(\d{4})\.csv$", flags=re.IGNORECASE)

def parse_year_from_filename(filename: str) -> Optional[int]:
    """Extrai o ano de um nome de arquivo 'inmet_YYYY.csv'. Retorna None se não bater."""
    m = _INMET_FILE_RE.match(filename)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None

def list_inmet_year_files(processed_dir: Path) -> List[Tuple[int, Path]]:
    """
    Varre processed/INMET e retorna [(ano, path), ...] para todos os inmet_{ano}.csv.
    Apenas arquivos que batem exatamente o padrão são considerados.
    """
    pairs: List[Tuple[int, Path]] = []
    for p in sorted(processed_dir.glob("inmet_*.csv")):
        y = parse_year_from_filename(p.name)
        if y is not None and p.is_file():
            pairs.append((y, p))
    # Ordena por ano crescente
    pairs.sort(key=lambda x: x[0])
    return pairs

# -----------------------------------------------------------------------------
# [SEÇÃO 3] LEITURA E CONSOLIDAÇÃO
# -----------------------------------------------------------------------------
def _columns_of_csv(csv_path: Path, encoding: str = "utf-8") -> List[str]:
    """Lê apenas o cabeçalho de um CSV para obter a lista de colunas."""
    return list(pd.read_csv(csv_path, nrows=0, encoding=encoding).columns)

def _reindex_like(df: pd.DataFrame, ref_cols: List[str]) -> pd.DataFrame:
    """
    Reordena/adapta colunas de df para ficar compatível com ref_cols.
    - Adiciona colunas ausentes como vazias
    - Ignora colunas extras (não esperadas) mantendo a ordem referencial
    """
    for c in ref_cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df[ref_cols]

def consolidate_inmet_stream(
    year_files: List[Tuple[int, Path]],
    out_path: Path,
    overwrite: bool = False,
    encoding: str = "utf-8",
    chunksize: int = 200_000,
) -> Path:
    """
    Consolidação em modo streaming: lê cada CSV anual em chunks e escreve
    diretamente no arquivo final. Ideal para grandes volumes.

    year_files: lista [(ano, path), ...] em ordem.
    out_path: caminho do CSV consolidado de saída.
    overwrite: se False e out_path existir, apenas registra e retorna.
    chunksize: número de linhas por chunk na leitura de cada CSV.
    """
    log = get_logger("inmet.consolidate")

    if out_path.exists() and not overwrite:
        log.info(f"[SKIP] {out_path.name} já existe. Use overwrite=True para refazer.")
        return out_path

    if not year_files:
        raise FileNotFoundError("Nenhum arquivo inmet_{ano}.csv encontrado em processed/INMET.")

    ensure_dir(out_path.parent)

    # Obtém colunas referenciais a partir do primeiro arquivo
    _, first_path = year_files[0]
    ref_cols = _columns_of_csv(first_path, encoding=encoding)
    log.info(f"[REFCOLS] {len(ref_cols)} colunas")

    # Abre arquivo de saída e streama
    written_any = False
    total_rows = 0

    # Para garantir header único no consolidado
    for year, csv_path in year_files:
        log.info(f"[READ] {csv_path.name}")
        # Primeiro, garantimos que colunas batem
        cols_this = _columns_of_csv(csv_path, encoding=encoding)
        if cols_this != ref_cols:
            log.warning(
                f"[WARN] Colunas diferentes no ano {year} ({csv_path.name}). "
                "Será feito reindex para colunas referenciais."
            )
            # Vamos ler em chunks e reindexar chunk a chunk

        # Lê por chunks
        chunk_iter = pd.read_csv(
            csv_path,
            encoding=encoding,
            sep=",",              # nossos processed são CSV comum (vírgula)
            engine="python",      # mais tolerante
            on_bad_lines="skip",  # pula linhas malformadas
            chunksize=chunksize,
            low_memory=False      # melhora inferência de tipos e reduz DtypeWarning
            # quotechar='"',      # padrão já é '"'; deixe explícito se preferir
            # escapechar="\\",    # só se algum dia precisar escapar manual
        )

        for i, chunk in enumerate(chunk_iter, start=1):
            if list(chunk.columns) != ref_cols:
                chunk = _reindex_like(chunk, ref_cols)

            # Escreve no CSV final (append), header só no 1º write
            chunk.to_csv(
                out_path,
                mode="a",
                index=False,
                header=not written_any,
                encoding=encoding,
            )
            written_any = True
            total_rows += len(chunk)
            if i % 10 == 0:
                log.info(f"  - {csv_path.name} chunks escritos: {i} (+{len(chunk)} linhas)")

    log.info(f"[WRITE] {out_path} (linhas totais: {total_rows})")
    return out_path

def consolidate_inmet_memory(
    year_files: List[Tuple[int, Path]],
    out_path: Path,
    overwrite: bool = False,
    encoding: str = "utf-8",
) -> Path:
    """
    Consolidação carregando cada ano inteiro em memória para depois concatenar.
    Útil para bases moderadas; pode estourar memória em bases grandes.
    """
    log = get_logger("inmet.consolidate")

    if out_path.exists() and not overwrite:
        log.info(f"[SKIP] {out_path.name} já existe. Use overwrite=True para refazer.")
        return out_path

    if not year_files:
        raise FileNotFoundError("Nenhum arquivo inmet_{ano}.csv encontrado em processed/INMET.")

    ensure_dir(out_path.parent)

    frames: List[pd.DataFrame] = []
    ref_cols: Optional[List[str]] = None
    total_rows = 0

    for year, csv_path in year_files:
        log.info(f"[READ] {csv_path.name}")
        df = pd.read_csv(csv_path, encoding=encoding, low_memory=False)
        if ref_cols is None:
            ref_cols = list(df.columns)
        elif list(df.columns) != ref_cols:
            log.warning(f"[WARN] Colunas diferentes no ano {year}. Ajustando via reindex.")
            df = _reindex_like(df, ref_cols)

        frames.append(df)
        total_rows += len(df)

    final = pd.concat(frames, ignore_index=True)
    final.to_csv(out_path, index=False, encoding=encoding)
    log.info(f"[WRITE] {out_path} (linhas totais: {total_rows})")
    return out_path

# -----------------------------------------------------------------------------
# [SEÇÃO 4] ORQUESTRAÇÃO (APIs de alto nível)
# -----------------------------------------------------------------------------
def consolidate_inmet(
    output_filename: str = "inmet_all_years.csv",
    years: Optional[Iterable[int]] = None,
    mode: str = "stream",                  # "stream" (padrão) ou "memory"
    overwrite: bool = False,
    encoding: str = "utf-8",
    chunksize: int = 200_000,
) -> Path:
    """
    Consolida todos (ou um subconjunto) dos inmet_{ano}.csv em processed/INMET
    para um arquivo único em consolidated/INMET/<output_filename>.

    years: se informado, filtra apenas os anos na lista; se None, usa todos encontrados.
    mode: "stream" (recomendado) ou "memory".
    """
    log = get_logger("inmet.consolidate")
    cfg = loadConfig()

    processed_dir = get_inmet_processed_dir()
    out_dir = get_inmet_consolidated_dir()
    out_path = out_dir / output_filename

    all_year_files = list_inmet_year_files(processed_dir)
    if years:
        years_set = {int(y) for y in years}
        year_files = [(y, p) for (y, p) in all_year_files if y in years_set]
    else:
        year_files = all_year_files

    if not year_files:
        raise FileNotFoundError("Nenhum inmet_{ano}.csv correspondente aos filtros.")

    log.info(f"[CONSOLIDATE] {len(year_files)} arquivo(s) | modo={mode} | destino={out_path}")

    if mode == "stream":
        return consolidate_inmet_stream(
            year_files=year_files,
            out_path=out_path,
            overwrite=overwrite,
            encoding=encoding,
            chunksize=chunksize,
        )
    elif mode == "memory":
        return consolidate_inmet_memory(
            year_files=year_files,
            out_path=out_path,
            overwrite=overwrite,
            encoding=encoding,
        )
    else:
        raise ValueError("Parâmetro 'mode' deve ser 'stream' ou 'memory'.")

# -----------------------------------------------------------------------------
# [SEÇÃO 5] MAIN (executável opcional)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # Exemplo: consolida tudo que existir, em modo streaming, sem sobrescrever se já existir
    log = get_logger("inmet.consolidate")
    try:
        out = consolidate_inmet(
            output_filename="inmet_all_years.csv",
            years=None,         # ou [2000, 2001, 2005, ...] para filtrar
            mode="stream",      # "stream" recomendado p/ grandes volumes
            overwrite=False,    # True para refazer
            encoding="utf-8",
            chunksize=200_000,
        )
        log.info(f"[DONE] Consolidado em: {out}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na consolidação: {e}")
