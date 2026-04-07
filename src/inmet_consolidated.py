# src/inmet_consolidated.py
# =============================================================================
# INMET - CONSOLIDACAO incremental (processed/INMET/inmet_{ano}.csv -> consolidated/INMET)
# Modos de saida:
#   - split  (default): gera um CSV por ano
#   - combine: gera um unico CSV com todos os anos selecionados
#   - both: gera split e combine
#
# Opcoes:
#   - filtro opcional por BIOMA via municipio normalizado usando dicionario BDQueimadas
#   - normalizacao da primeira coluna de DATA para YYYY-MM-DD
#   - remocao de linhas com sentinelas (-9999, -999) nas colunas de medidas
#
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

# Permite campos muito longos para o parser csv do Python
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
    # seu mapeamento pode apontar "external" para data/consolidated
    return get_path("paths", "data", "external")

def get_inmet_consolidated_dir() -> Path:
    return ensure_dir(Path(get_consolidated_root()) / "INMET")

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
# [SECAO 3] HELPERS
# -----------------------------------------------------------------------------
def _batched(lst: List[Tuple[int, Path]], n: int = 3) -> Iterable[List[Tuple[int, Path]]]:
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

def _read_header_line_raw(p: Path, encoding: str = "utf-8") -> str:
    """Le a linha do header exatamente como esta (preserva aspas e virgulas internas)."""
    with p.open("r", encoding=encoding, errors="replace", newline="") as fh:
        return fh.readline().rstrip("\n\r")

def _parse_header_fields_from_line(header_line: str) -> List[str]:
    """Parseia os campos do header preservando semantica CSV."""
    reader = csv.reader([header_line])
    return next(reader, [])

def _resolve_combined_output_filename(years: Optional[Iterable[int]], biome: Optional[str]) -> str:
    """Resolve nome do arquivo combinado a partir de anos e bioma."""
    if biome:
        b = str(biome).strip().lower().replace(" ", "_")
        if not years:
            return f"inmet_all_years_{b}.csv"
        yrs = sorted({int(y) for y in years})
        return f"inmet_{yrs[0]}_{yrs[-1]}_{b}.csv" if len(yrs) > 1 else f"inmet_{yrs[0]}_{b}.csv"
    else:
        if not years:
            return "inmet_all_years.csv"
        yrs = sorted({int(y) for y in years})
        return f"inmet_{yrs[0]}_{yrs[-1]}.csv" if len(yrs) > 1 else f"inmet_{yrs[0]}.csv"

def _resolve_year_output_filename(year: int, biome: Optional[str]) -> str:
    """Resolve nome do arquivo por ano a partir do bioma."""
    if biome:
        b = str(biome).strip().lower().replace(" ", "_")
        return f"inmet_{int(year)}_{b}.csv"
    return f"inmet_{int(year)}.csv"

def _load_allowed_municipios_for_biome(biome: str, encoding: str = "utf-8") -> Set[str]:
    """Carrega municipios normalizados cujo bioma casa com o alvo."""
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
    """Copia apenas linhas cujo municipio normalizado esteja em allowed_municipios. Header e pulado."""
    rows = 0
    with src.open("r", encoding=encoding, errors="replace", newline="") as s:
        reader = csv.reader(s)
        try:
            next(reader)  # header
        except StopIteration:
            return 0
        writer = csv.writer(dst_fh, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            try:
                mun = normalize_key(row[municipio_idx])
            except IndexError:
                continue
            if mun in allowed_municipios:
                writer.writerow(row)
                rows += 1
    return rows

def _append_csv_skip_header_raw(src: Path, dst_fh, encoding: str = "utf-8") -> int:
    """Copia linhas cruas (sem tocar no CSV), pulando a primeira linha (header)."""
    rows = 0
    with src.open("r", encoding=encoding, errors="replace", newline="") as s:
        _ = s.readline()  # skip header
        for line in s:
            dst_fh.write(line)
            rows += 1
    return rows

def _normalize_dates_text_inplace(csv_path: Path, encoding: str = "utf-8") -> None:
    """Substitui '/' por '-' SOMENTE na 1a coluna; preserva header."""
    tmp = csv_path.with_suffix(".tmp")
    with csv_path.open("r", encoding=encoding, errors="replace", newline="") as r, \
         tmp.open("w", encoding=encoding, newline="") as w:
        first = True
        for line in r:
            if first:
                w.write(line)  # header intacto
                first = False
                continue
            idx = line.find(",")  # separador da 1a coluna
            if idx > 0:
                token = line[:idx]
                if "/" in token:
                    token = token.replace("/", "-")
                line = token + line[idx:]
            w.write(line)
    tmp.replace(csv_path)

def _drop_rows_with_sentinels_inplace(
    csv_path: Path,
    encoding: str = "utf-8",
    drop_policy: str = "all",  # "all" ou "any"
) -> None:
    """
    Remove linhas com sentinelas (-9999, -999) nas colunas de MEDIDAS.
    Medidas = todas as colunas exceto: DATA (YYYY-MM-DD), HORA (UTC), ANO, CIDADE, LATITUDE, LONGITUDE.
    """
    SENTINELS = {"-9999", "-999"}
    tmp = csv_path.with_suffix(".clean.tmp")

    with csv_path.open("r", encoding=encoding, errors="replace", newline="") as r, \
         tmp.open("w", encoding=encoding, newline="") as w:
        reader = csv.reader(r)
        writer = csv.writer(w, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)

        try:
            header = next(reader)
        except StopIteration:
            # arquivo vazio
            return

        writer.writerow(header)

        keep_as_meta = {"DATA (YYYY-MM-DD)", "HORA (UTC)", "ANO", "CIDADE", "LATITUDE", "LONGITUDE"}
        meta_idx = {i for i, name in enumerate(header) if name in keep_as_meta}
        measure_idx = [i for i in range(len(header)) if i not in meta_idx]

        for row in reader:
            if len(row) != len(header):
                continue
            values = [row[i] for i in measure_idx]
            if drop_policy == "any":
                if any(v in SENTINELS for v in values):
                    continue
            else:
                meas_non_empty = [v for v in values if v != ""]
                if meas_non_empty and all(v in SENTINELS for v in meas_non_empty):
                    continue
            writer.writerow(row)

    tmp.replace(csv_path)

# -----------------------------------------------------------------------------
# [SECAO 4] CONSOLIDACAO - MODOS
# -----------------------------------------------------------------------------
def consolidate_inmet(
    mode: str = "split",                 # "split", "combine", "both"
    output_filename: Optional[str] = None,
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 3,                 # usado no combine
    normalize_dates: bool = True,
    biome: Optional[str] = None,
    municipio_col: str = "CIDADE",
    drop_policy: str = "all",            # "all" ou "any"
) -> List[Path]:
    """
    Consolida INMET para arquivos no diretorio consolidated/INMET conforme o modo.

    mode:
      - "split": gera um CSV por ano
      - "combine": gera um unico CSV com todos os anos selecionados
      - "both": gera split e combine

    Retorna lista de caminhos dos arquivos gerados.
    """
    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    _ = loadConfig()

    if mode not in {"split", "combine", "both"}:
        raise ValueError("mode deve ser 'split', 'combine' ou 'both'.")

    processed_dir = get_inmet_processed_dir()
    out_dir = get_inmet_consolidated_dir()

    year_files = list_inmet_year_files(processed_dir)
    if years:
        yrs = {int(y) for y in years}
        year_files = [(y, p) for (y, p) in year_files if y in yrs]

    if not year_files:
        raise FileNotFoundError("Nenhum inmet_{ano}.csv encontrado para consolidar.")

    # allowed municipios para filtro por bioma
    allowed_municipios: Optional[Set[str]] = None
    if biome:
        allowed_municipios = _load_allowed_municipios_for_biome(biome, encoding=encoding)
        if not allowed_municipios:
            log.warning(f"[WARN] Nenhum municipio encontrado para bioma='{biome}'. Saidas podem ficar vazias.")
    else:
        log.info("[INFO] Sem filtro de bioma. Consolidando todos os registros.")

    outputs: List[Path] = []

    # ---------- MODO SPLIT ----------
    if mode in {"split", "both"}:
        log.info("[MODE] split: gerando um arquivo por ano")
        for y, path in year_files:
            # header e indice de municipio para ESTE arquivo
            header_line = _read_header_line_raw(path, encoding=encoding)
            header_fields = _parse_header_fields_from_line(header_line)
            if not header_fields:
                log.warning(f"[SKIP] Header vazio em {path.name}")
                continue
            try:
                municipio_idx = header_fields.index(municipio_col)
            except ValueError:
                log.warning(f"[SKIP] Coluna de municipio '{municipio_col}' nao encontrada em {path.name}")
                continue

            out_name = _resolve_year_output_filename(y, biome)
            out_path = out_dir / out_name

            if out_path.exists() and not overwrite:
                log.info(f"[SKIP] {out_path.name} ja existe. Use --overwrite para refazer.")
                outputs.append(out_path)
                continue

            log.info(f"[WRITE] {out_path.name} a partir de {path.name} {'(filtrado)' if allowed_municipios else '(sem filtro)'}")

            with out_path.open("w", encoding=encoding, newline="") as out_fh:
                out_fh.write(header_line + "\n")
                if allowed_municipios is None:
                    # copia crua sem header
                    _ = _append_csv_skip_header_raw(path, out_fh, encoding=encoding)
                else:
                    _ = _append_csv_filtered_by_municipio(
                        path,
                        out_fh,
                        municipio_idx=municipio_idx,
                        allowed_municipios=allowed_municipios,
                        encoding=encoding,
                    )

            if normalize_dates:
                _normalize_dates_text_inplace(out_path, encoding=encoding)

            _drop_rows_with_sentinels_inplace(out_path, encoding=encoding, drop_policy=drop_policy)
            outputs.append(out_path)

    # ---------- MODO COMBINE ----------
    if mode in {"combine", "both"}:
        log.info("[MODE] combine: gerando um arquivo unico")
        auto_name = _resolve_combined_output_filename([y for y, _ in year_files] if years else None, biome)
        out_name = output_filename or auto_name
        out_path = out_dir / out_name

        if out_path.exists() and not overwrite:
            log.info(f"[SKIP] {out_path.name} ja existe. Use --overwrite para refazer.")
            outputs.append(out_path)
        else:
            # header base do primeiro arquivo
            _, first_path = year_files[0]
            header_line_raw = _read_header_line_raw(first_path, encoding=encoding)
            header_fields = _parse_header_fields_from_line(header_line_raw)
            if not header_fields:
                raise ValueError(f"Header vazio ou invalido em {first_path}")
            try:
                municipio_idx_first = header_fields.index(municipio_col)
            except ValueError as e:
                raise ValueError(
                    f"Coluna de municipio '{municipio_col}' nao encontrada no header do primeiro arquivo: {e}."
                )

            total_rows = 0
            with out_path.open("w", encoding=encoding, newline="") as out_fh:
                out_fh.write(header_line_raw + "\n")

                for batch in _batched(year_files, batch_size):
                    anos = [y for y, _ in batch]
                    log.info(f"[BATCH] anos={anos}")
                    for y, path in batch:
                        # para cada arquivo, recomputar indice de municipio
                        header_line_this = _read_header_line_raw(path, encoding=encoding)
                        header_fields_this = _parse_header_fields_from_line(header_line_this)
                        if not header_fields_this:
                            log.warning(f"[SKIP] Header vazio em {path.name}")
                            continue
                        try:
                            municipio_idx = header_fields_this.index(municipio_col)
                        except ValueError:
                            # fallback: tenta usar o indice do primeiro se o header for identico
                            municipio_idx = municipio_idx_first

                        if allowed_municipios is None:
                            added = _append_csv_skip_header_raw(path, out_fh, encoding=encoding)
                        else:
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
                _normalize_dates_text_inplace(out_path, encoding=encoding)

            _drop_rows_with_sentinels_inplace(out_path, encoding=encoding, drop_policy=drop_policy)
            outputs.append(out_path)

    return outputs

# -----------------------------------------------------------------------------
# [SECAO 5] CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Consolida INMET processed -> consolidated em modo split, combine ou both; suporta filtro por bioma."
    )
    p.add_argument(
        "--mode",
        choices=("split", "combine", "both"),
        default="split",
        help="Modo de saida: split=um arquivo por ano; combine=um arquivo unico; both=ambos. Default: split.",
    )
    p.add_argument("--years", nargs="*", type=int, default=None, help="Lista de anos. Se omitido, usa todos os disponiveis.")
    p.add_argument("--biome", type=str, default=None, help="Bioma para filtrar (ex.: Cerrado). Case-insensitive.")
    p.add_argument("--municipio-col", type=str, default="CIDADE", help="Nome da coluna de municipio. Default: 'CIDADE'.")
    p.add_argument("--output-filename", type=str, default=None, help="Nome do arquivo combinado. Se omitido, e inferido.")
    p.add_argument("--overwrite", action="store_true", help="Sobrescreve arquivos de saida se existirem.")
    p.add_argument("--batch-size", type=int, default=3, help="Tamanho do lote no combine. Default: 3.")
    p.add_argument("--no-normalize-dates", action="store_true", help="Nao normalizar a 1a coluna de DATA.")
    p.add_argument(
        "--drop-policy",
        choices=("all", "any"),
        default="all",
        help="Regra de remocao por sentinelas: 'all' remove se todas as medidas forem -9999/-999; 'any' remove se qualquer medida for -9999/-999.",
    )

    args = p.parse_args()

    log = get_logger("inmet.consolidate", kind="load", per_run_file=True)
    try:
        outs = consolidate_inmet(
            mode=args.mode,
            output_filename=args.output_filename,
            years=args.years,
            overwrite=args.overwrite,
            encoding="utf-8",
            batch_size=args.batch_size,
            normalize_dates=not args.no_normalize_dates,
            biome=args.biome,
            municipio_col=args.municipio_col,
            drop_policy=args.drop_policy,
        )
        for pth in outs:
            log.info(f"[DONE] {pth}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na consolidacao: {e}")
