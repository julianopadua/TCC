# src/bdq_build_biome_dictionary.py
# =============================================================================
# BDQUEIMADAS - Construção do dicionário estado-municipio-bioma a partir dos
# CSVs anuais focos_br_ref_YYYY, para anos de 2003 a 2024.
#
# Saídas em paths.dictionarys:
#   - bdq_municipio_bioma.csv
#   - bdq_municipio_bioma.parquet
#   - bdq_municipio_bioma.json
#   - municipios_cerrado.csv  -> pares (estado, municipio) que possuem bioma Cerrado
#
# O dicionário resultante permite filtrar futuramente outras bases por municípios
# pertencentes ao bioma Cerrado de forma simples e reprodutível.
#
# Dependências: utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, List, Tuple, Dict, Set

import pandas as pd

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    normalize_key
)

# -----------------------------------------------------------------------------
# [SEÇÃO 1] CONFIGURAÇÕES E PARÂMETROS
# -----------------------------------------------------------------------------
cfg = loadConfig()
log = get_logger("bdq.dictionary", kind="dictionary", per_run_file=True)

DEFAULT_FOLDER = "ID_BDQUEIMADAS"
DEFAULT_YEARS = list(range(2003, 2025))

ENCODING = (cfg.get("io", {}) or {}).get("encoding", "utf-8")
OUT_DIR = ensure_dir(get_path("paths", "data", "dictionarys"))

OUT_CSV = Path(OUT_DIR) / "bdq_municipio_bioma.csv"
OUT_PARQUET = Path(OUT_DIR) / "bdq_municipio_bioma.parquet"
OUT_JSON = Path(OUT_DIR) / "bdq_municipio_bioma.json"
OUT_CERRADO = Path(OUT_DIR) / "municipios_cerrado.csv"


# -----------------------------------------------------------------------------
# [SEÇÃO 2] FUNÇÕES DE SUPORTE
# -----------------------------------------------------------------------------
def _year_paths(processed_root: Path, years: Iterable[int]) -> List[Path]:
    paths: List[Path] = []
    for y in years:
        p = processed_root / f"focos_br_ref_{y}" / f"focos_br_ref_{y}.csv"
        if p.exists():
            paths.append(p)
        else:
            log.warning(f"[SKIP] CSV inexistente para {y}: {p}")
    return paths

def _read_minimal_columns(csv_path: Path) -> pd.DataFrame:
    usecols = ["pais", "estado", "municipio", "bioma"]
    df = pd.read_csv(csv_path, usecols=usecols, dtype=str, encoding=ENCODING)
    for c in usecols:
        df[c] = df[c].astype(str).str.strip()
    # normaliza chaves de matching
    df["estado_norm"] = df["estado"].map(normalize_key)
    df["municipio_norm"] = df["municipio"].map(normalize_key)
    # remove linhas sem município ou bioma
    df = df[(df["municipio"] != "") & (df["bioma"] != "")]
    return df

def _aggregate_years(df_concat: pd.DataFrame) -> pd.DataFrame:
    df_concat["year"] = df_concat["year"].astype(int)
    group_cols = ["pais", "estado", "municipio", "estado_norm", "municipio_norm", "bioma"]
    agg = (
        df_concat.groupby(group_cols, as_index=False)["year"]
        .apply(lambda s: ";".join(str(x) for x in sorted(set(s.tolist()))))
        .rename(columns={"year": "anos_origem"})
    )
    return agg

def _to_nested_mapping(df: pd.DataFrame) -> Dict[str, Dict[str, List[str]]]:
    nested: Dict[str, Dict[str, Set[str]]] = {}
    for estado, municipio, bioma in df[["estado", "municipio", "bioma"]].itertuples(index=False, name=None):
        nested.setdefault(estado, {}).setdefault(municipio, set()).add(bioma)
    nested_sorted: Dict[str, Dict[str, List[str]]] = {
        est: {mun: sorted(list(biomas)) for mun, biomas in sorted(muns.items(), key=lambda x: x[0])}
        for est, muns in sorted(nested.items(), key=lambda x: x[0])
    }
    return nested_sorted

def _extract_cerrado_pairs(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["bioma"].str.casefold() == "cerrado".casefold()
    cerrado = (
        df.loc[mask, ["estado", "municipio", "estado_norm", "municipio_norm"]]
        .drop_duplicates()
        .sort_values(["estado", "municipio"])
    )
    return cerrado


# -----------------------------------------------------------------------------
# [SEÇÃO 3] PIPELINE PRINCIPAL
# -----------------------------------------------------------------------------
def build_dictionary(
    folder_name: str = DEFAULT_FOLDER,
    years: Iterable[int] = DEFAULT_YEARS,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    processed_root = Path(get_path("paths", "data", "processed")) / folder_name
    csv_paths = _year_paths(processed_root, years)
    if not csv_paths:
        log.error("Nenhum CSV encontrado. Verifique o path e os anos.")
        return pd.DataFrame(), pd.DataFrame()

    parts: List[pd.DataFrame] = []
    for p in csv_paths:
        try:
            df = _read_minimal_columns(p)
            df["year"] = int(p.stem.split("_")[-1])
            parts.append(df)
            log.info(f"[OK] Lido: {p.name}  linhas={len(df)}")
        except Exception as e:
            log.error(f"[ERROR] Falha ao ler {p}: {e}")

    if not parts:
        log.error("Falha ao ler todos os CSVs de entrada.")
        return pd.DataFrame(), pd.DataFrame()

    df_concat = pd.concat(parts, ignore_index=True)
    df_concat = df_concat.drop_duplicates(["pais", "estado", "municipio", "bioma", "estado_norm", "municipio_norm", "year"])

    df_full = _aggregate_years(df_concat)
    df_full = df_full.sort_values(["estado", "municipio", "bioma"]).reset_index(drop=True)

    df_full.to_csv(OUT_CSV, index=False, encoding=ENCODING)
    df_full.to_parquet(OUT_PARQUET, index=False)
    log.info(f"[WRITE] {OUT_CSV}")
    log.info(f"[WRITE] {OUT_PARQUET}")

    nested = _to_nested_mapping(df_full)
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(nested, f, ensure_ascii=False, indent=2)
    log.info(f"[WRITE] {OUT_JSON}")

    df_cerrado = _extract_cerrado_pairs(df_full)
    df_cerrado.to_csv(OUT_CERRADO, index=False, encoding=ENCODING)
    log.info(f"[WRITE] {OUT_CERRADO}  total={len(df_cerrado)}")

    log.info(
        f"[SUMMARY] registros únicos estado-municipio-bioma={len(df_full)}  "
        f"municipios com Cerrado={len(df_cerrado)}"
    )
    return df_full, df_cerrado


# -----------------------------------------------------------------------------
# [SEÇÃO 4] UTILITÁRIOS PARA USO POSTERIOR
# -----------------------------------------------------------------------------
def load_cerrado_pairs() -> Set[Tuple[str, str]]:
    if not OUT_CERRADO.exists():
        raise FileNotFoundError(f"Arquivo {OUT_CERRADO} inexistente. Gere o dicionário primeiro.")
    df = pd.read_csv(OUT_CERRADO, dtype=str, encoding=ENCODING)
    # já vem com colunas normalizadas, mas preserva fallback
    if {"estado_norm", "municipio_norm"}.issubset(df.columns):
        est = df["estado_norm"].map(str).map(str.strip)
        mun = df["municipio_norm"].map(str).map(str.strip)
    else:
        est = df["estado"].map(lambda x: normalize_key(str(x)))
        mun = df["municipio"].map(lambda x: normalize_key(str(x)))
    return set(zip(est, mun))

def filter_df_by_cerrado(df: pd.DataFrame, estado_col: str = "estado", municipio_col: str = "municipio") -> pd.DataFrame:
    pairs = load_cerrado_pairs()
    mask = df.apply(lambda r: (normalize_key(r.get(estado_col)), normalize_key(r.get(municipio_col))) in pairs, axis=1)
    return df.loc[mask].copy()


# -----------------------------------------------------------------------------
# [SEÇÃO 5] CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Constrói dicionário estado-municipio-bioma a partir dos focos_br_ref_YYYY"
    )
    p.add_argument(
        "--folder",
        required=False,
        default=DEFAULT_FOLDER,
        help=f"Pasta sob data/processed onde estão focos_br_ref_YYYY (default: {DEFAULT_FOLDER}).",
    )
    p.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=None,
        help="Lista de anos para considerar, ex.: --years 2003 2004 ... Se omitido, usa 2003..2024.",
    )

    args = p.parse_args()
    folder = args.folder or DEFAULT_FOLDER
    years = args.years or DEFAULT_YEARS

    log.info(f"[INPUT] folder={folder}  anos={years[0]}..{years[-1] if years else 'n/a'}")
    build_dictionary(folder_name=folder, years=years)
