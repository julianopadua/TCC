# src/inmet_bdqueimadas_consolidated.py
# =============================================================================
# INMET - CONSOLIDACAO incremental (processed/INMET/inmet_{ano}.csv -> consolidated/INMET)
# Agora com filtro opcional por BIOMA, usando dicionario estado-municipio-bioma.
# Saida e nomeada automaticamente com base em anos e bioma.
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir, normalize_key)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple, Set
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

# Permite campos muito longos para o parser csv do Python (caso necessario)
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# -----------------------------------------------------------------------------
# [SECAO 1] PATHS
# -----------------------------------------------------------------------------
def get_inmet_processed_dir() -> Path:
    return get_path("paths", "providers", "inmet", "processed")

def get_consolidated_root() -> Path:
    return get_path("paths", "data", "external")

def get_inmet_consolidated_dir() -> Path:
    return ensure_dir(Path(get_consolidated_root()) / "INMET")

def default_output_path() -> Path:
    return get_inmet_consolidated_dir() / "inmet_all_years.csv"

def get_dictionary_csv_path() -> Path:
    # requer arquivo gerado pelo builder do dicionario: data/dictionarys/bdq_municipio_bioma.csv
    return Path(get_path("paths", "data","dictionarys")) / "bdq_municipio_bioma.csv"

# -----------------------------------------------------------------------------
# [SECAO 2] DESCOBERTA
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
# [SECAO 3] HELPERS DE I/O E NOMES
# -----------------------------------------------------------------------------
def _batched(lst: List[Tuple[int, Path]], n: int = 3) -> Iterable[List[Tuple[int, Path]]]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def _read_header_fields(p: Path, encoding: str = "utf-8") -> List[str]:
    with p.open("r", encoding=encoding, errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        try:
            return next(reader)
        except StopIteration:
            return []

def _resolve_output_filename(years: Optional[Iterable[int]], biome: Optional[str]) -> str:
    if not biome:
        return "inmet_all_years.csv" if (not years or len(list(years)) != 1) else f"inmet_{list(years)[0]}.csv"
    b = str(biome).strip().lower().replace(" ", "_")
    if not years or len(list(years)) == 0:
        return f"inmet_all_years_{b}.csv"
    yrs = sorted({int(y) for y in years})
    if len(yrs) == 1:
        return f"inmet_{yrs[0]}_{b}.csv"
    return f"inmet_{yrs[0]}_{yrs[-1]}_{b}.csv"

def _load_allowed_municipios_for_biome(biome: str, encoding: str = "utf-8") -> Set[str]:
    """
    Carrega de data/dictionarys/bdq_municipio_bioma.csv o conjunto de municipios
    normalizados cujo bioma casa com o alvo.
    """
    dic_path = get_dictionary_csv_path()
    if not dic_path.exists():
        raise FileNotFoundError(f"Arquivo do dicionario inexistente: {dic_path}. Gere-o antes.")
    allowed: Set[str] = set()
    with dic_path.open("r", encoding=encoding, errors="replace", newline="") as fh:
        reader = csv.DictReader(fh)
        fns = set(reader.fieldnames or [])
        if not {"municipio", "bioma"}.issubset(fns):
            raise ValueError("bdq_municipio_bioma.csv precisa conter colunas municipio e bioma.")
        has_norm = "municipio_norm" in fns
        tgt = biome.casefold()
        for row in reader:
            if str(row["bioma"]).casefold() == tgt:
                mun_n = str(row["municipio_norm"]).strip() if has_norm else normalize_key(row["municipio"])
                if mun_n:
                    allowed.add(mun_n)
    return allowed

def _append_csv_filtered_by_municipio(
    src: Path,
    dst_fh,
    municipio_idx: int,
    allowed_municipios: Set[str],
    encoding: str = "utf-8",
) -> int:
    """
    Copia apenas as linhas de src cujo municipio normalizado perteneca a allowed_municipios.
    Pula o header. Retorna numero de linhas escritas.
    """
    rows = 0
    with src.open("r", encoding=encoding, errors="replace", newline="") as s:
        reader = csv.reader(s)
        try:
            next(reader)  # header
        except StopIteration:
            return 0
        writer = csv.writer(dst_fh, lineterminator="\n")
        for row in reader:
            try:
                mun = normalize_key(row[municipio_idx])
            except IndexError:
                continue
            if mun in allowed_municipios:
                writer.writerow(row)
                rows += 1
    return rows

def _append_csv_skip_header(src: Path, dst_fh, encoding: str = "utf-8") -> int:
    """
    Copia o conteudo de src para um arquivo de destino ja aberto, pulando a primeira linha (header).
    Retorna o numero de linhas copiadas.
    """
    rows = 0
    with src.open("r", encoding=encoding, errors="replace", newline="") as s:
        _ = s.readline()
        for line in s:
            dst_fh.write(line)
            rows += 1
    return rows

def _normalize_dates_text_inplace(csv_path: Path, encoding: str = "utf-8") -> None:
    """
    Passada final: substitui "/" por "-" somente no primeiro campo de cada linha.
    Mantem o header intocado. Implementacao simples, robusta a arquivos grandes.
    """
    tmp = csv_path.with_suffix(".tmp")
    with csv_path.open("r", encoding=encoding, errors="replace", newline="") as r, \
         tmp.open("w", encoding=encoding, newline="") as w:
        first = True
        for line in r:
            if first:
                w.write(line)
                first = False
                continue
            idx = line.find(",")
            if idx > 0:
                token = line[:idx]
                if "/" in token:
                    token = token.replace("/", "-")
                line = token + line[idx:]
            w.write(line)
    tmp.replace(csv_path)

# -----------------------------------------------------------------------------
# [SECAO 4] CONSOLIDACAO
# -----------------------------------------------------------------------------
def consolidate_inmet(
    output_filename: Optional[str] = None,
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 3,
    normalize_dates: bool = True,
    biome: Optional[str] = None,
    municipio_col: str = "CIDADE",
) -> Path:
    """
    Consolida CSVs anuais (processed/INMET/inmet_{ano}.csv) em um unico CSV
    (consolidated/INMET/<output_filename>), processando em lotes de batch_size.

    Se biome for fornecido, filtra por municipio usando o dicionario de bioma
    e o nome de saida e ajustado automaticamente.
    """
    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    _ = loadConfig()

    processed_dir = get_inmet_processed_dir()
    out_dir = get_inmet_consolidated_dir()

    year_files = list_inmet_year_files(processed_dir)
    if years:
        yrs = {int(y) for y in years}
        year_files = [(y, p) for (y, p) in year_files if y in yrs]

    if not year_files:
        raise FileNotFoundError("Nenhum inmet_{ano}.csv encontrado para consolidar.")

    # Header do primeiro arquivo e indices de colunas
    _, first_path = year_files[0]
    header_fields = _read_header_fields(first_path, encoding=encoding)
    if not header_fields:
        raise ValueError(f"Header vazio em {first_path}")

    try:
        municipio_idx = header_fields.index(municipio_col)
    except ValueError as e:
        raise ValueError(
            f"Coluna de municipio nao encontrada no header: {e}. "
            f"Use --municipio-col para ajustar."
        )

    # Nome do arquivo de saida
    auto_name = _resolve_output_filename(years, biome)
    out_name = output_filename or auto_name
    out_path = out_dir / out_name

    # Conjunto permitido para bioma, se aplicavel
    allowed_municipios: Optional[Set[str]] = None
    if biome:
        allowed_municipios = _load_allowed_municipios_for_biome(biome, encoding=encoding)
        if not allowed_municipios:
            log.warning(f"[WARN] Nenhum municipio encontrado para bioma='{biome}'. Saida pode ficar vazia.")
    else:
        log.info("[INFO] Sem filtro de bioma. Consolidando todos os registros.")

    # Saida
    ensure_dir(out_path.parent)
    if out_path.exists():
        if overwrite:
            out_path.unlink()
        else:
            log.info(f"[SKIP] {out_path.name} ja existe. Use --overwrite para refazer ou mude --output-filename.")
            return out_path

    total_rows = 0
    log.info(
        f"[CONSOLIDATE] {len(year_files)} arquivo(s) em lotes de {batch_size} -> {out_path.name} "
        f"{'(filtrando por bioma=' + biome + ')' if biome else ''}"
    )

    # Abre destino, grava header original do primeiro arquivo
    with out_path.open("w", encoding=encoding, newline="") as out_fh:
        out_fh.write(",".join(header_fields) + "\n")

        for batch in _batched(year_files, batch_size):
            anos = [y for y, _ in batch]
            log.info(f"[BATCH] anos={anos}")
            for y, path in batch:
                if allowed_municipios is None:
                    log.info(f"  [APPEND] {path.name} (sem filtro)")
                    added = _append_csv_skip_header(path, out_fh, encoding=encoding)
                else:
                    log.info(f"  [APPEND] {path.name} (filtrado por bioma via municipio)")
                    added = _append_csv_filtered_by_municipio(
                        path,
                        out_fh,
                        municipio_idx=municipio_idx,
                        allowed_municipios=allowed_municipios,
                        encoding=encoding,
                    )
                total_rows += added
                log.info(f"    +{added} linhas (acumulado: {total_rows})")

    if normalize_dates:
        log.info("[NORMALIZE] Padronizando DATA para YYYY-MM-DD na primeira coluna...")
        _normalize_dates_text_inplace(out_path, encoding=encoding)

    log.info(f"[DONE] {out_path} (linhas totais: {total_rows})")
    return out_path

# -----------------------------------------------------------------------------
# [SECAO 5] CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Consolida INMET processed -> consolidated, com filtro opcional por bioma."
    )
    p.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=None,
        help="Lista de anos a consolidar. Se omitido, consolida todos os disponiveis.",
    )
    p.add_argument(
        "--biome",
        type=str,
        default=None,
        help="Nome do bioma para filtrar (ex.: Cerrado). Case-insensitive.",
    )
    p.add_argument(
        "--municipio-col",
        type=str,
        default="CIDADE",
        help="Nome da coluna de municipio no CSV do INMET. Default: 'CIDADE'.",
    )
    p.add_argument(
        "--output-filename",
        type=str,
        default=None,
        help="Nome do arquivo de saida. Se omitido, e inferido de anos e bioma.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Sobrescreve se o arquivo de saida ja existir.",
    )
    p.add_argument(
        "--batch-size",
        type=int,
        default=3,
        help="Tamanho do lote de processamento. Default: 3.",
    )
    p.add_argument(
        "--no-normalize-dates",
        action="store_true",
        help="Nao padronizar a primeira coluna de DATA para YYYY-MM-DD.",
    )

    args = p.parse_args()

    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    try:
        out = consolidate_inmet(
            output_filename=args.output_filename,
            years=args.years,
            overwrite=args.overwrite,
            encoding="utf-8",
            batch_size=args.batch_size,
            normalize_dates=not args.no_normalize_dates,
            biome=args.biome,
            municipio_col=args.municipio_col,
        )
        log.info(f"[DONE] Consolidado em: {out}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na consolidacao: {e}")
