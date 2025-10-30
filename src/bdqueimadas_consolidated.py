# src/consolidated_bdqueimadas.py
# =============================================================================
# BDQUEIMADAS — Consolidação (RAW exportador_*_ref_YYYY.csv × PROCESSADO focos_br_ref_YYYY.csv)
# Regra: junção 1:1 por (HORA + PAIS + ESTADO + MUNICIPIO) com filtro opcional por Bioma.
# Saídas:
#   - data/consolidated/BDQUEIMADAS/bdq_targets_{YYYY}[_<bioma>].csv        (por ano)
#   - data/consolidated/BDQUEIMADAS/bdq_targets_all_years[_<bioma>].csv     (multi-anos)
#   - OU bdq_targets_{Y1}_{Y2}[_<bioma>].csv se especificar um intervalo explícito
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir, normalize_key)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Iterable, Tuple, Set
import re
import time
import unicodedata

import pandas as pd
import numpy as np

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    normalize_key,
)

# =============================================================================
# CONFIG/LOG
# =============================================================================
cfg = loadConfig()
log = get_logger("bdqueimadas.consolidate", kind="load", per_run_file=True)

# =============================================================================
# PATHS
# =============================================================================
RAW_BDQ_DIR  = Path(get_path("paths", "data", "raw")) / "BDQUEIMADAS"
PROC_BDQ_DIR = Path(get_path("paths", "data", "processed")) / "ID_BDQUEIMADAS"
OUT_DIR      = ensure_dir(Path(get_path("paths", "data", "external")) / "BDQUEIMADAS")

# =============================================================================
# HELPERS — ENCODING, NORMALIZAÇÃO, DATAS
# =============================================================================
_CTRL_RE = re.compile(r"[\x00-\x1F\x7F-\x9F]")
_WS_RE   = re.compile(r"[\u00A0\u200B\u200C\u200D\uFEFF]")

def _strip_controls(x: str) -> str:
    if x is None:
        return ""
    s = str(x)
    s = _WS_RE.sub(" ", s)
    s = _CTRL_RE.sub("", s)
    return s

def _ascii_upper_no_diacritics(x: str) -> str:
    """
    Para colunas de saída (PAIS/ESTADO/MUNICIPIO): remove apenas diacríticos e sobe para UPPER,
    sem 'ignore' de encoding (não perde letras: "Uberlândia" -> "UBERLANDIA").
    """
    if x is None:
        return ""
    s = _strip_controls(str(x))
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = " ".join(s.split())
    return s.upper()

def _read_csv_smart(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False, on_bad_lines="skip")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="latin1", low_memory=False, on_bad_lines="skip")

def _parse_manual_datetime(s: pd.Series) -> pd.Series:
    # Ex.: "2012/01/01 16:14:00"
    return pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S", errors="coerce")

def _parse_proc_datetime(s: pd.Series) -> pd.Series:
    # Ex.: "2012-08-24 16:41:00"
    return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")

def _floor_hour(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("h")

# =============================================================================
# DESCOBERTA DE ARQUIVOS
# =============================================================================
_MANUAL_FILE_RE = re.compile(r"^exportador_(\d{4}-\d{2}-\d{2})_ref_(\d{4})\.csv$", flags=re.IGNORECASE)
def _year_from_manual(p: Path) -> Optional[int]:
    m = _MANUAL_FILE_RE.match(p.name)
    return int(m.group(2)) if m else None

def list_manual_year_files(raw_dir: Path = RAW_BDQ_DIR) -> List[Tuple[int, Path]]:
    cand = []
    for p in sorted(raw_dir.glob("exportador_*_ref_*.csv")):
        y = _year_from_manual(p)
        if y:
            cand.append((y, p))
    cand.sort(key=lambda t: t[0])
    return cand

def processed_file_for_year(year: int, proc_root: Path = PROC_BDQ_DIR) -> Optional[Path]:
    subdir = proc_root / f"focos_br_ref_{year}"
    p = subdir / f"focos_br_ref_{year}.csv"
    return p if p.exists() else None

# =============================================================================
# NOMEAÇÃO DE SAÍDAS (compatível com INMET)
# =============================================================================
def _resolve_output_filename(
    years: Optional[Iterable[int]],
    biome: Optional[str],
    prefix: str = "bdq_targets",
) -> str:
    """
    Regra:
      - sem years   -> {prefix}_all_years[_bioma].csv
      - 1 ano       -> {prefix}_{YYYY}[_bioma].csv
      - multi-anos  -> {prefix}_{Y1}_{YN}[_bioma].csv
    bioma: normalizado para snake-case lower.
    """
    b = ""
    if biome:
        bnorm = str(biome).strip().lower().replace(" ", "_")
        if bnorm:
            b = f"_{bnorm}"

    if not years:
        return f"{prefix}_all_years{b}.csv"

    yrs = sorted({int(y) for y in years})
    if len(yrs) == 1:
        return f"{prefix}_{yrs[0]}{b}.csv"
    return f"{prefix}_{yrs[0]}_{yrs[-1]}{b}.csv"

# =============================================================================
# CARREGAMENTO E NORMALIZAÇÃO
# =============================================================================
MANUAL_DT_COL = "DataHora"  # no MANUAL
PROC_DT_COL   = "data_pas"  # no PROCESSADO

def _maybe_filter_biome(df: pd.DataFrame, biome: Optional[str]) -> Tuple[pd.DataFrame, int]:
    total = len(df)
    if biome and "Bioma" in df.columns:
        tgt = normalize_key(biome)
        df = df.loc[df["__BIO_KEY"] == tgt].copy()
        log.info(f"  filtro Bioma={biome} (key={tgt}): {len(df):,} / {total:,}")
    else:
        log.info(f"  sem filtro de Bioma: {total:,} linhas")
    return df, len(df)

def load_manual(path: Path, biome: Optional[str] = None, validation: bool = False) -> Tuple[pd.DataFrame, int]:
    log.info(f"[PHASE] Lendo MANUAL: {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (manual): {len(df):,} | tempo={time.time()-t0:,.2f}s")

    # Datas (hora cheia)
    log.info("[PHASE] Normalizando MANUAL (datas, strings)")
    t1 = time.time()
    df["__DT"]   = _parse_manual_datetime(df[MANUAL_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])

    # Chaves de junção (robustas) — usam utils.normalize_key (minúsculo, sem diacríticos)
    df["__PAIS_KEY"]  = df["Pais"].map(normalize_key)
    df["__UF_KEY"]    = df["Estado"].map(normalize_key)
    df["__MUN_KEY"]   = df["Municipio"].map(normalize_key)
    df["__BIO_KEY"]   = df["Bioma"].map(normalize_key) if "Bioma" in df.columns else ""

    # Colunas de saída (legíveis) — sem diacríticos, UPPER (sem perder letras)
    df["PAIS_OUT"]      = df["Pais"].map(_ascii_upper_no_diacritics)
    df["ESTADO_OUT"]    = df["Estado"].map(_ascii_upper_no_diacritics)
    df["MUNICIPIO_OUT"] = df["Municipio"].map(_ascii_upper_no_diacritics)

    # Conversões numéricas opcionais
    for c in ("Latitude", "Longitude", "FRP"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Filtro opcional por Bioma
    df, expected_rows = _maybe_filter_biome(df, biome)

    # Chave exata de junção (hora+local) — carimbo em int64 garante unicidade temporal
    df["__KEY"] = (
        df["__DT_H"].astype("int64").astype("string") + "|" +
        df["__PAIS_KEY"].astype(str) + "|" +
        df["__UF_KEY"].astype(str)   + "|" +
        df["__MUN_KEY"].astype(str)
    )

    # Redução às colunas necessárias + preservação de atributos
    keep = [
        "__KEY", "__DT_H",
        "PAIS_OUT", "ESTADO_OUT", "MUNICIPIO_OUT",
        "RiscoFogo", "FRP",
    ]
    df = df[keep].copy()

    if validation:
        df = df.head(100).copy()
        expected_rows = len(df)
        log.info("  [VALIDATION] MANUAL reduzido para 100 linhas.")

    log.info(f"  normalização (manual) ok | tempo={time.time()-t1:,.2f}s")
    return df, expected_rows

def load_processed(path: Path, restrict_pairs: Optional[pd.DataFrame] = None, validation: bool = False) -> pd.DataFrame:
    log.info(f"[PHASE] Lendo PROCESSADO: {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (proc): {len(df):,} | tempo={time.time()-t0:,.2f}s")

    log.info("[PHASE] Normalizando PROCESSADO (datas, strings)")
    t1 = time.time()
    df["__DT"]   = _parse_proc_datetime(df[PROC_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])

    # Chaves de junção (mesma normalização)
    df["__PAIS_KEY"] = df["pais"].map(normalize_key)
    df["__UF_KEY"]   = df["estado"].map(normalize_key)
    df["__MUN_KEY"]  = df["municipio"].map(normalize_key)

    # Chave exata
    df["__KEY"] = (
        df["__DT_H"].astype("int64").astype("string") + "|" +
        df["__PAIS_KEY"].astype(str) + "|" +
        df["__UF_KEY"].astype(str)   + "|" +
        df["__MUN_KEY"].astype(str)
    )

    # Redução de colunas
    keep = ["__KEY", "foco_id", "id_bdq", "lat", "lon"]
    df = df[keep].copy()

    # Deduplicar por __KEY (1:1) — mantém a primeira ocorrência
    before = len(df)
    df = df.drop_duplicates(subset="__KEY", keep="first")
    log.info(f"  dedup PROCESSADO por __KEY: {len(df):,} (removidas {before - len(df):,})")

    if validation:
        df = df.head(500_000).copy()
        log.info("  [VALIDATION] PROCESSADO truncado a 500k linhas (após dedup).")

    log.info(f"  normalização (proc) ok | tempo={time.time()-t1:,.2f}s")
    return df

# =============================================================================
# MATCHING — 1:1 (MANUAL × PROCESSADO)
# =============================================================================
def merge_manual_processed(df_m: pd.DataFrame, df_p: pd.DataFrame) -> pd.DataFrame:
    log.info("[PHASE] MERGE 1:1 (left) por __KEY (hora+país+UF+município)")
    t0 = time.time()
    cols_p = ["__KEY", "id_bdq", "foco_id", "lat", "lon"]
    merged = df_m.merge(df_p[cols_p], on="__KEY", how="left", validate="m:1", copy=False)
    log.info(f"  linhas pós-merge: {len(merged):,} | tempo={time.time()-t0:,.2f}s")
    return merged

# =============================================================================
# BUILD OUTPUT E ESCRITA
# =============================================================================
def build_output(merged: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "DATAHORA":   merged["__DT_H"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        "PAIS":       merged["PAIS_OUT"],
        "ESTADO":     merged["ESTADO_OUT"],
        "MUNICIPIO":  merged["MUNICIPIO_OUT"],
        "RISCO_FOGO": merged["RiscoFogo"],
        "FRP":        merged["FRP"],
        "ID_BDQ":     merged.get("id_bdq"),
        "FOCO_ID":    merged.get("foco_id"),
    })
    out = out.sort_values(["DATAHORA", "ESTADO", "MUNICIPIO"], kind="stable").reset_index(drop=True)
    return out

def write_output(df: pd.DataFrame, path: Path, encoding: str = "utf-8") -> Path:
    ensure_dir(path.parent)
    df.to_csv(path, index=False, encoding=encoding)
    log.info(f"[WRITE] {path} (linhas: {len(df):,})")
    return path

# =============================================================================
# PIPELINE POR ANO
# =============================================================================
def consolidate_year(
    year: int,
    overwrite: bool = False,
    validation: bool = False,
    biome: Optional[str] = None,
    encoding: str = "utf-8",
) -> Optional[Path]:
    manual_files = [p for (y, p) in list_manual_year_files(RAW_BDQ_DIR) if y == year]
    proc_file = processed_file_for_year(year, PROC_BDQ_DIR)

    if not manual_files:
        log.warning(f"[{year}] Nenhum exportador_*_ref_{year}.csv em {RAW_BDQ_DIR}")
        return None
    if not proc_file:
        log.warning(f"[{year}] Processado focos_br_ref_{year}.csv não encontrado em {PROC_BDQ_DIR}")
        return None

    manual_path = sorted(manual_files)[-1]  # usa o mais recente
    log.info(f"[{year}] MANUAL: {manual_path.name}")
    log.info(f"[{year}] PROC:   {proc_file.name}")

    out_name = _resolve_output_filename([year], biome, prefix="bdq_targets")
    out_path = OUT_DIR / out_name
    if out_path.exists() and not overwrite:
        log.info(f"[{year}] [SKIP] {out_path.name} já existe. Use --overwrite para refazer.")
        return out_path

    # 1) MANUAL (+ filtro opcional de bioma)
    df_m, expected_rows = load_manual(manual_path, biome=biome, validation=validation)
    if expected_rows == 0:
        log.warning(f"[{year}] Após filtro de Bioma, nenhuma linha no MANUAL. Abortando ano.")
        return None

    # 2) PROCESSADO (dedup por __KEY)
    df_p = load_processed(proc_file, restrict_pairs=None, validation=validation)

    # 3) MERGE 1:1
    merged = merge_manual_processed(df_m, df_p)

    # 4) Métricas
    matched_rows   = int(merged["id_bdq"].notna().sum())
    unmatched_rows = int((merged["id_bdq"].isna()).sum())
    log.info(f"[{year}] EXPECTED (MANUAL após filtro) = {expected_rows:,}")
    log.info(f"[{year}] RESULT len(merge)            = {len(merged):,}  (deve == EXPECTED)")
    log.info(f"[{year}] MATCHED (com ID_BDQ)         = {matched_rows:,}")
    log.info(f"[{year}] UNMATCHED                    = {unmatched_rows:,}")

    # 5) Saída
    out_df = build_output(merged)
    return write_output(out_df, out_path, encoding=encoding)

# =============================================================================
# ORQUESTRAÇÃO (MÚLTIPLOS ANOS) + ALL_YEARS
# =============================================================================
def run(
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    validation: bool = False,
    biome: Optional[str] = None,
    output_filename: Optional[str] = None,
    encoding: str = "utf-8",
) -> Optional[Path]:
    if years:
        years = sorted({int(y) for y in years})
    else:
        years = sorted({y for (y, _) in list_manual_year_files(RAW_BDQ_DIR)})

    if not years:
        log.warning("Nenhum ano encontrado para consolidar.")
        return None

    outs: List[Path] = []
    for y in years:
        try:
            p = consolidate_year(
                y,
                overwrite=overwrite,
                validation=validation,
                biome=biome,
                encoding=encoding,
            )
            if p:
                outs.append(p)
        except Exception as e:
            log.exception(f"[{y}] Falha na consolidação: {e}")

    if not outs:
        return None

    # Se houver mais de um ano, gerar um all_years (ou intervalo Y1_YN)
    if len(outs) > 1:
        final_name = output_filename or _resolve_output_filename(years, biome, prefix="bdq_targets")
        final_path = OUT_DIR / final_name

        if final_path.exists() and not overwrite:
            log.info(f"[ALL] [SKIP] {final_path.name} já existe.")
            return final_path

        frames = [pd.read_csv(p, encoding=encoding, low_memory=False) for p in outs]
        all_df = pd.concat(frames, ignore_index=True)
        all_df.sort_values(["DATAHORA", "ESTADO", "MUNICIPIO"], kind="stable", inplace=True)
        all_df.to_csv(final_path, index=False, encoding=encoding)
        log.info(f"[ALL] [DONE] {final_path} (linhas: {len(all_df):,})")
        return final_path

    return outs[0]

# =============================================================================
# CLI
# =============================================================================
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(
        description="Consolidação BDQueimadas (MANUAL × PROCESSADO) 1:1 por hora+local, com filtro opcional de bioma."
    )
    p.add_argument("--years", nargs="*", type=int, default=None,
                   help="Lista de anos (ex.: --years 2012 2013). Se omitir, roda para todos com MANUAL.")
    p.add_argument("--biome", type=str, default=None,
                   help="Filtrar por bioma (ex.: --biome 'Cerrado'). Se omitir, usa todos os biomas.")
    p.add_argument("--overwrite", action="store_true",
                   help="Sobrescreve saídas existentes.")
    p.add_argument("--validation", action="store_true",
                   help="Modo de validação rápida: MANUAL=100; PROCESSADO=500k (após dedup).")
    p.add_argument("--output-filename", type=str, default=None,
                   help="Nome do arquivo final multi-anos. Se omitido, usa regra de nomeação automática.")
    p.add_argument("--encoding", type=str, default="utf-8",
                   help="Encoding para leitura/escrita (default: utf-8).")
    args = p.parse_args()

    run(
        years=args.years,
        overwrite=args.overwrite,
        validation=args.validation,
        biome=args.biome,
        output_filename=args.output_filename,
        encoding=args.encoding,
    )
