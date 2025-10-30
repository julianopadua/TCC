# src/audit_city_coverage.py
# =============================================================================
# AUDITORIA DE COBERTURA DE CIDADES: BDQUEIMADAS × INMET (por ano e bioma)
# - Lê data/consolidated/BDQUEIMADAS/bdq_targets_{YYYY}_{biome}.csv
# - Lê data/consolidated/INMET/inmet_{YYYY}_{biome}.csv
# - Normaliza nomes de cidades e calcula interseção/partições por ano
# Saídas:
#   1) data/dictionarys/city_coverage_summary_{biome}.csv
#   2) data/dictionarys/city_coverage_details/{biome}/year_{YYYY}_{biome}.md
#   3) data/dictionarys/city_coverage_details/{biome}/year_{YYYY}_{biome}.csv
# Depende de: pandas, utils.py (loadConfig, get_logger, get_path, ensure_dir, normalize_key)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple
import re
import collections

import pandas as pd

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    normalize_key,
)

# -----------------------------------------------------------------------------
# [SEÇÃO 1] PATHS, DISCOVERY E PADRÕES
# -----------------------------------------------------------------------------
def _inmet_consolidated_dir() -> Path:
    return Path(get_path("paths", "data", "external")) / "INMET"

def _bdq_consolidated_dir() -> Path:
    return Path(get_path("paths", "data", "external")) / "BDQUEIMADAS"

def _dict_root() -> Path:
    return ensure_dir(get_path("paths", "data", "dictionarys"))

def _details_dir(biome: str) -> Path:
    root = _dict_root()
    return ensure_dir(Path(root) / "city_coverage_details" / biome.lower())

_INMET_RE = re.compile(r"^inmet_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)
_BDQ_RE   = re.compile(r"^bdq_targets_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)

def _list_inmet_years_for_biome(biome: str) -> List[int]:
    root = _inmet_consolidated_dir()
    years: List[int] = []
    for p in root.glob(f"inmet_*_{biome}.csv"):
        m = _INMET_RE.match(p.name)
        if m:
            years.append(int(m.group(1)))
    return sorted(set(years))

def _list_bdq_years_for_biome(biome: str) -> List[int]:
    root = _bdq_consolidated_dir()
    years: List[int] = []
    for p in root.glob(f"bdq_targets_*_{biome}.csv"):
        m = _BDQ_RE.match(p.name)
        if m and (m.group("biome").lower() == biome.lower()):
            years.append(int(m.group(1)))
    return sorted(set(years))

# -----------------------------------------------------------------------------
# [SEÇÃO 2] LEITURA E NORMALIZAÇÃO DE CIDADES
# -----------------------------------------------------------------------------
def _read_inmet_cities(year: int, biome: str, encoding: str = "utf-8") -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Retorna:
      - DataFrame com colunas ['source','year','city_raw','city_norm','count']
      - Contador city_raw para referência (pode ilustrar variantes)
    """
    path = _inmet_consolidated_dir() / f"inmet_{year}_{biome}.csv"
    if not path.exists():
        raise FileNotFoundError(f"INMET não encontrado: {path}")

    df = pd.read_csv(path, dtype=str, encoding=encoding, usecols=["CIDADE"])
    df["city_raw"] = df["CIDADE"].astype(str)
    df["city_norm"] = df["city_raw"].map(normalize_key)

    counts_raw = collections.Counter(df["city_raw"].dropna().tolist())

    agg = (
        df.groupby(["city_raw", "city_norm"], dropna=False)
          .size().reset_index(name="count")
    )
    agg.insert(0, "source", "INMET")
    agg.insert(1, "year", year)
    return agg[["source","year","city_raw","city_norm","count"]], counts_raw

def _read_bdq_cities(year: int, biome: str, encoding: str = "utf-8") -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    Retorna:
      - DataFrame com colunas ['source','year','city_raw','city_norm','count']
      - Contador city_raw
    """
    path = _bdq_consolidated_dir() / f"bdq_targets_{year}_{biome}.csv"
    if not path.exists():
        raise FileNotFoundError(f"BDQ não encontrado: {path}")

    usecols = ["MUNICIPIO"]
    df = pd.read_csv(path, dtype=str, encoding=encoding, usecols=usecols)
    df["city_raw"] = df["MUNICIPIO"].astype(str)
    df["city_norm"] = df["city_raw"].map(normalize_key)

    counts_raw = collections.Counter(df["city_raw"].dropna().tolist())

    agg = (
        df.groupby(["city_raw", "city_norm"], dropna=False)
          .size().reset_index(name="count")
    )
    agg.insert(0, "source", "BDQ")
    agg.insert(1, "year", year)
    return agg[["source","year","city_raw","city_norm","count"]], counts_raw

# -----------------------------------------------------------------------------
# [SEÇÃO 3] CÁLCULO DE COBERTURA POR ANO
# -----------------------------------------------------------------------------
def _coverage_for_year(year: int, biome: str, encoding: str = "utf-8") -> Tuple[Dict[str, int], pd.DataFrame, str]:
    """
    Computa métricas de cobertura por ano.
    Retorna:
      - resumo: dict com as métricas agregadas
      - detalhes_df: long format (source, year, city_raw, city_norm, count)
      - details_md: string Markdown com listas exatas de cidades por partição
    """
    inmet_df, inmet_raw_counts = _read_inmet_cities(year, biome, encoding=encoding)
    bdq_df,   bdq_raw_counts   = _read_bdq_cities(year, biome, encoding=encoding)

    # conjuntos por 'city_norm'
    inmet_set = set(inmet_df["city_norm"].dropna().tolist())
    bdq_set   = set(bdq_df["city_norm"].dropna().tolist())

    common = sorted(inmet_set & bdq_set)
    bdq_only = sorted(bdq_set - inmet_set)
    inmet_only = sorted(inmet_set - bdq_set)

    n_inmet = len(inmet_set)
    n_bdq   = len(bdq_set)
    n_common = len(common)
    n_bdq_only = len(bdq_only)
    n_inmet_only = len(inmet_only)

    prop_inmet_cobre_bdq = float(n_common) / float(n_bdq) if n_bdq > 0 else 0.0
    prop_bdq_cobre_inmet = float(n_common) / float(n_inmet) if n_inmet > 0 else 0.0

    resumo = {
        "year": year,
        "n_cidades_bdq": n_bdq,
        "n_cidades_inmet": n_inmet,
        "n_comuns": n_common,
        "n_bdq_exclusivas": n_bdq_only,
        "n_inmet_exclusivas": n_inmet_only,
        "prop_inmet_cobre_bdq": round(prop_inmet_cobre_bdq, 6),
        "prop_bdq_cobre_inmet": round(prop_bdq_cobre_inmet, 6),
    }

    detalhes_df = pd.concat([bdq_df, inmet_df], ignore_index=True)

    # Monta Markdown detalhado com as listas exatas
    def _mk_list(title: str, items: List[str]) -> str:
        if not items:
            return f"### {title}\n\n(nenhum)\n"
        body = "\n".join(f"- {it}" for it in items)
        return f"### {title}\n\n{body}\n"

    md_lines = []
    md_lines.append(f"# City coverage {year} — Biome: {biome}\n")
    md_lines.append(f"- n_cidades_bdq: **{n_bdq}**")
    md_lines.append(f"- n_cidades_inmet: **{n_inmet}**")
    md_lines.append(f"- n_comuns: **{n_common}**")
    md_lines.append(f"- n_bdq_exclusivas: **{n_bdq_only}**")
    md_lines.append(f"- n_inmet_exclusivas: **{n_inmet_only}**")
    md_lines.append(f"- prop_inmet_cobre_bdq: **{resumo['prop_inmet_cobre_bdq']:.4f}**")
    md_lines.append(f"- prop_bdq_cobre_inmet: **{resumo['prop_bdq_cobre_inmet']:.4f}**\n")

    # Listas por partição (norm)
    md_lines.append(_mk_list("COMMON (city_norm)", common))
    md_lines.append(_mk_list("BDQ_only (city_norm)", bdq_only))
    md_lines.append(_mk_list("INMET_only (city_norm)", inmet_only))

    # Algumas amostras de variantes raw (opcional e útil quando há divergências)
    def _top_raw_examples(counter: collections.Counter, n: int = 10) -> List[Tuple[str, int]]:
        return counter.most_common(n)

    bdq_raw_top = _top_raw_examples(bdq_raw_counts, 20)
    inmet_raw_top = _top_raw_examples(inmet_raw_counts, 20)

    if bdq_raw_top:
        md_lines.append("### BDQ raw examples (top 20)\n")
        md_lines.extend([f"- {name} ({cnt})" for name, cnt in bdq_raw_top])
        md_lines.append("")

    if inmet_raw_top:
        md_lines.append("### INMET raw examples (top 20)\n")
        md_lines.extend([f"- {name} ({cnt})" for name, cnt in inmet_raw_top])
        md_lines.append("")

    details_md = "\n".join(md_lines)
    return resumo, detalhes_df, details_md

# -----------------------------------------------------------------------------
# [SEÇÃO 4] PIPELINE: GERAR TABELA E LOGS
# -----------------------------------------------------------------------------
def audit_city_coverage(
    biome: str = "cerrado",
    years: Optional[Iterable[int]] = None,
    min_year: int = 2003,
    encoding: str = "utf-8",
) -> Tuple[pd.DataFrame, List[int]]:
    """
    Executa a auditoria:
      - Seleciona anos elegíveis (interseção de INMET e BDQ, aplicando min_year)
      - Para cada ano: gera CSV detalhado e MD detalhado
      - Consolida a tabela-resumo e salva em data/dictionarys/city_coverage_summary_{biome}.csv
    Retorna: (summary_df, years_processed)
    """
    log = get_logger("city_coverage.audit", kind="dataset", per_run_file=True)
    _ = loadConfig()

    inmet_years = _list_inmet_years_for_biome(biome)
    bdq_years   = _list_bdq_years_for_biome(biome)

    candidate_years = sorted(set(inmet_years).intersection(bdq_years))
    candidate_years = [y for y in candidate_years if y >= min_year]

    if years:
        wanted = sorted({int(y) for y in years})
        years_to_run = [y for y in wanted if y in candidate_years]
    else:
        years_to_run = candidate_years

    if not years_to_run:
        raise RuntimeError(
            f"Sem anos elegíveis. INMET({biome})={inmet_years}  BDQ({biome})={bdq_years}  "
            f"Interseção>={min_year}={candidate_years}"
        )

    details_root = _details_dir(biome)
    summary_rows: List[Dict[str, int]] = []
    processed_years: List[int] = []

    for y in years_to_run:
        try:
            log.info(f"[YEAR] Auditando {y} ({biome})...")
            resumo, detalhes_df, details_md = _coverage_for_year(y, biome, encoding=encoding)

            # escreve detalhado CSV
            det_csv = details_root / f"year_{y}_{biome}.csv"
            detalhes_df.to_csv(det_csv, index=False, encoding=encoding)
            log.info(f"[WRITE] {det_csv}")

            # escreve detalhado MD
            det_md = details_root / f"year_{y}_{biome}.md"
            det_md.write_text(details_md, encoding=encoding)
            log.info(f"[WRITE] {det_md}")

            summary_rows.append(resumo)
            processed_years.append(y)

        except Exception as e:
            log.exception(f"[ERROR] Falha ao auditar ano {y}: {e}")

    if not summary_rows:
        raise RuntimeError("Nenhuma linha de resumo gerada.")

    summary_df = pd.DataFrame(summary_rows).sort_values("year").reset_index(drop=True)

    # salva tabela-resumo
    out_summary = _dict_root() / f"city_coverage_summary_{biome}.csv"
    summary_df.to_csv(out_summary, index=False, encoding=encoding)
    log.info(f"[WRITE] {out_summary} (linhas: {len(summary_df)})")

    return summary_df, processed_years

# -----------------------------------------------------------------------------
# [SEÇÃO 5] CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Audita a cobertura de cidades BDQueimadas × INMET por ano e bioma."
    )
    p.add_argument("--biome", type=str, default="cerrado", help="Bioma (default: 'cerrado').")
    p.add_argument("--years", nargs="*", type=int, default=None, help="Lista de anos (ex.: --years 2003 2004).")
    p.add_argument("--min-year", type=int, default=2003, help="Ano mínimo (default: 2003).")
    p.add_argument("--encoding", type=str, default="utf-8", help="Encoding (default: utf-8).")
    args = p.parse_args()

    log = get_logger("city_coverage.audit", kind="dataset", per_run_file=True)
    try:
        summary_df, years_processed = audit_city_coverage(
            biome=args.biome,
            years=args.years,
            min_year=args.min_year,
            encoding=args.encoding,
        )
        log.info(f"[DONE] anos={years_processed}  linhas_resumo={len(summary_df)}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na auditoria: {e}")
