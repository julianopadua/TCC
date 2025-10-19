# src/consolidated_bdqueimadas.py
# =============================================================================
# BDQUEIMADAS — Consolidação (RAW exportador_*_ref_YYYY.csv × PROCESSADO focos_br_ref_YYYY.csv)
# Regra: (HORA, PAÍS, UF, MUNICÍPIO) com saída 1:1 ao MANUAL (após filtro opcional de Bioma).
# Saída: data/consolidated/BDQUEIMADAS/bdq_targets_YYYY.csv  (+ all_years)
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Iterable, Tuple
import re
import unicodedata
import time

import pandas as pd
import numpy as np
import re as _re

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
)

cfg = loadConfig()
log = get_logger("bdqueimadas.consolidate", kind="load", per_run_file=True)

# =============================================================================
# PATHS
# =============================================================================
RAW_BDQ_DIR  = Path(get_path("paths", "data", "raw")) / "BDQUEIMADAS"
PROC_BDQ_DIR = Path(get_path("paths", "data", "processed")) / "ID_BDQUEIMADAS"
# >>> salva no MESMO lugar que seus logs mostraram:
OUT_DIR      = ensure_dir(Path(get_path("paths", "data", "external")) / "BDQUEIMADAS")

# =============================================================================
# HELPERS — NORMALIZAÇÃO, DATAS, LOG
# =============================================================================
_CTRL_RE = _re.compile(r"[\x00-\x1F\x7F-\x9F]")
_WS_RE   = _re.compile(r"[\u00A0\u200B\u200C\u200D\uFEFF]")

def _strip_controls(x: str) -> str:
    if x is None:
        return ""
    x = _WS_RE.sub(" ", str(x))
    x = _CTRL_RE.sub("", x)
    return x

def _repair_mojibake(x: str) -> str:
    if x is None:
        return ""
    s = str(x)
    if any(t in s for t in ("Ã", "Â", "Ê", "Ô", "Õ", "", "¢", "§", "ã", "õ", "ç")):
        try:
            s = s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            pass
    return s

def _norm_text(x: str) -> str:
    s = _repair_mojibake(x)
    s = _strip_controls(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper().strip()

def _read_csv_smart(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False, on_bad_lines="skip")
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="latin1", low_memory=False, on_bad_lines="skip")

def _parse_manual_datetime(s: str) -> pd.Timestamp:
    # Ex.: "2012/01/01 16:14:00" ou "2013/01/01 16:25:00"
    return pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S", errors="coerce")

def _parse_proc_datetime(s: str) -> pd.Timestamp:
    # Ex.: "2012-08-24 16:41:00"
    return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")

def _floor_hour(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("h")

def _log_phase(title: str):
    log.info(f"[PHASE] {title}")

# =============================================================================
# DISCOVERY
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
# LOADERS
# =============================================================================
MANUAL_DT_COL = "DataHora"
PROC_DT_COL   = "data_pas"

def _maybe_filter_biome(df: pd.DataFrame, biome: Optional[str]) -> Tuple[pd.DataFrame, int]:
    total = len(df)
    if biome:
        bnorm = _norm_text(biome)
        df = df.loc[df["__BIO"] == bnorm].copy()
        log.info(f"  filtro Bioma={bnorm}: {len(df):,} / {total:,} (esperado na saída ≈ este valor)")
    else:
        log.info(f"  sem filtro de Bioma: {total:,} linhas (esperado na saída ≈ este valor)")
    return df, len(df)

def load_manual(path: Path, biome: Optional[str], validation: bool = False) -> Tuple[pd.DataFrame, int]:
    _log_phase(f"Lendo MANUAL: {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (manual): {len(df):,}  | tempo={time.time()-t0:,.2f}s")

    _log_phase("Normalizando MANUAL (datas, strings)")
    t1 = time.time()
    df["__DT"]   = _parse_manual_datetime(df[MANUAL_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])
    df["__PAIS"] = df["Pais"].map(_norm_text)
    df["__UF"]   = df["Estado"].map(_norm_text)
    df["__MUN"]  = df["Municipio"].map(_norm_text)
    df["__BIO"]  = df["Bioma"].map(_norm_text)

    # Tipos enxutos e ID de linha para garantir 1:1
    for c in ("Latitude","Longitude","FRP"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ["__PAIS","__UF","__MUN","__BIO"]:
        df[c] = df[c].astype("category")

    # Filtro opcional por Bioma
    df, expected_rows = _maybe_filter_biome(df, biome)

    # Colunas necessárias + ID
    df = df.assign(__RID=np.arange(len(df), dtype="int64"))
    keep = ["__RID","__DT_H","__PAIS","__UF","__MUN","RiscoFogo","FRP","Latitude","Longitude"]
    df = df[keep].copy()

    # Chave exata de junção (hora+local)
    df["__KEY"] = (
        df["__DT_H"].astype("int64").astype("string") + "|" +
        df["__PAIS"].astype(str) + "|" +
        df["__UF"].astype(str)   + "|" +
        df["__MUN"].astype(str)
    )

    log.info(f"  normalização (manual) ok | tempo={time.time()-t1:,.2f}s")
    if validation:
        df = df.head(100).copy()
        expected_rows = len(df)
        log.info("  [VALIDATION] MANUAL reduzido para 100 linhas.")
    return df, expected_rows

def load_processed(path: Path, restrict_pairs: Optional[pd.DataFrame] = None, validation: bool = False) -> pd.DataFrame:
    _log_phase(f"Lendo PROCESSADO: {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (proc): {len(df):,}  | tempo={time.time()-t0:,.2f}s")

    _log_phase("Normalizando PROCESSADO (datas, strings)")
    t1 = time.time()
    df["__DT"]   = _parse_proc_datetime(df[PROC_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])
    df["__PAIS"] = df["pais"].map(_norm_text)
    df["__UF"]   = df["estado"].map(_norm_text)
    df["__MUN"]  = df["municipio"].map(_norm_text)

    for c in ("lat","lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    keep = ["__DT_H","__PAIS","__UF","__MUN","id_bdq","foco_id","lat","lon"]
    df = df[keep].copy()

    # (opcional) restringir por (UF,MUN) do manual — encurta memória
    if restrict_pairs is not None and not restrict_pairs.empty:
        _log_phase("Restringindo PROCESSADO aos (UF,MUN) presentes no MANUAL")
        pairs = restrict_pairs.drop_duplicates().copy()
        pairs["__UF"]  = pairs["__UF"].astype("category")
        pairs["__MUN"] = pairs["__MUN"].astype("category")
        df["_KEY_UF_M"]    = (df["__UF"].astype(str) + "|" + df["__MUN"].astype(str))
        pairs["_KEY_UF_M"] = (pairs["__UF"].astype(str) + "|" + pairs["__MUN"].astype(str))
        keep_set = set(pairs["_KEY_UF_M"].tolist())
        before = len(df)
        df = df.loc[df["_KEY_UF_M"].isin(keep_set)].drop(columns=["_KEY_UF_M"])
        log.info(f"  PROCESSADO reduzido por (UF,MUN): {len(df):,} / {before:,}")

    # Chave exata de junção (hora+local)
    df["__KEY"] = (
        df["__DT_H"].astype("int64").astype("string") + "|" +
        df["__PAIS"].astype(str) + "|" +
        df["__UF"].astype(str)   + "|" +
        df["__MUN"].astype(str)
    )

    # Deduplicar por __KEY para evitar multiplicação de linhas (escolhe primeira)
    dup_before = len(df)
    # Mantém ordem estável; se houver várias linhas da mesma __KEY, pega a 1ª
    df = df.sort_values(["__DT_H"]).drop_duplicates(["__KEY"], keep="first")
    dup_after = len(df)
    log.info(f"  dedup PROCESSADO por __KEY: {dup_after:,} (antes {dup_before:,}, removidas {dup_before-dup_after:,})")

    # Otimizações
    for c in ["__PAIS","__UF","__MUN"]:
        df[c] = df[c].astype("category")
    log.info(f"  normalização (proc) ok | tempo={time.time()-t1:,.2f}s")
    if validation:
        # já está 1 por chave; manter pequeno
        df = df.head(500_000).copy()
        log.info("  [VALIDATION] PROCESSADO truncado a 500k chaves (após dedup).")
    return df

# =============================================================================
# MATCHING — 1:1 ao MANUAL por __KEY
# =============================================================================
def match_strict_1to1(df_m: pd.DataFrame, df_p: pd.DataFrame) -> pd.DataFrame:
    """
    Left-join por __KEY, com PROCESSADO já deduplicado por __KEY.
    Garante: len(resultado) == len(df_m).
    """
    _log_phase("MERGE 1:1 (left) por __KEY (hora+país+UF+município)")
    t0 = time.time()
    cols_p = ["__KEY","id_bdq","foco_id","lat","lon"]
    mrg = df_m.merge(df_p[cols_p], on="__KEY", how="left", validate="m:1", copy=False)
    # validate="m:1" falha se df_p tiver chave repetida (proteção extra)
    log.info(f"  linhas pós-merge: {len(mrg):,} | tempo={time.time()-t0:,.2f}s")
    return mrg

# =============================================================================
# BUILD OUTPUT
# =============================================================================
def build_output(df_merged: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "DATAHORA":   df_merged["__DT_H"],
        "PAIS":       df_merged["__PAIS"],
        "ESTADO":     df_merged["__UF"],
        "MUNICIPIO":  df_merged["__MUN"],
        "RISCO_FOGO": df_merged.get("RiscoFogo"),
        "FRP":        df_merged.get("FRP"),
        "ID_BDQ":     df_merged.get("id_bdq"),
        "FOCO_ID":    df_merged.get("foco_id"),
    })
    out = out.sort_values(["DATAHORA","ESTADO","MUNICIPIO"], kind="stable").reset_index(drop=True)
    return out

# =============================================================================
# PIPELINE POR ANO
# =============================================================================
def consolidate_year(
    year: int,
    overwrite: bool = False,
    validation: bool = False,
    biome: Optional[str] = None,
) -> Optional[Path]:
    manual_files = [p for (y, p) in list_manual_year_files(RAW_BDQ_DIR) if y == year]
    proc_file = processed_file_for_year(year, PROC_BDQ_DIR)

    if not manual_files:
        log.warning(f"[{year}] Nenhum exportador_*_ref_{year}.csv em {RAW_BDQ_DIR}")
        return None
    if not proc_file:
        log.warning(f"[{year}] Processado focos_br_ref_{year}.csv não encontrado em {PROC_BDQ_DIR}")
        return None

    manual_path = sorted(manual_files)[-1]
    log.info(f"[{year}] MANUAL: {manual_path.name}")
    log.info(f"[{year}] PROC:    {proc_file.name}")

    out_path = OUT_DIR / f"bdq_targets_{year}.csv"
    if out_path.exists() and not overwrite:
        log.info(f"[{year}] [SKIP] {out_path.name} já existe. Use --overwrite para refazer.")
        return out_path

    # 1) MANUAL (+ filtro opcional por bioma) — mantém 1 linha por foco reportado
    df_m, expected_rows = load_manual(manual_path, biome=biome, validation=validation)
    if expected_rows == 0:
        log.warning(f"[{year}] Após filtro de Bioma, nenhuma linha no MANUAL. Abortando ano.")
        return None

    # 2) PROCESSADO restrito a (UF,MUN) do MANUAL e DEDUP por __KEY
    restrict_pairs = df_m[["__UF","__MUN"]]
    df_p = load_processed(proc_file, restrict_pairs=restrict_pairs, validation=validation)

    # 3) JOIN 1:1
    merged = match_strict_1to1(df_m, df_p)

    # 4) Métricas
    matched_rows   = int(merged["id_bdq"].notna().sum())
    unmatched_rows = int((merged["id_bdq"].isna()).sum())
    log.info(f"[{year}] EXPECTED (MANUAL após filtro) = {expected_rows:,}")
    log.info(f"[{year}] RESULT len(merge)            = {len(merged):,}  (deve == EXPECTED)")
    log.info(f"[{year}] MATCHED (com ID_BDQ)         = {matched_rows:,}")
    log.info(f"[{year}] UNMATCHED                    = {unmatched_rows:,}")

    # 5) Saída
    out_df = build_output(merged)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"[{year}] [DONE] {out_path}  (linhas: {len(out_df):,})")
    return out_path

# =============================================================================
# MAIN/CLI
# =============================================================================
def run(
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    validation: bool = False,
    biome: Optional[str] = None,
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
            )
            if p:
                outs.append(p)
        except Exception as e:
            log.exception(f"[{y}] Falha na consolidação: {e}")

    if len(outs) > 1:
        all_path = OUT_DIR / "bdq_targets_all_years.csv"
        if all_path.exists() and not overwrite:
            log.info(f"[ALL] [SKIP] {all_path.name} já existe.")
            return all_path
        frames = [pd.read_csv(p, encoding="utf-8") for p in outs]
        all_df = pd.concat(frames, ignore_index=True)
        all_df.sort_values(["DATAHORA","ESTADO","MUNICIPIO"], kind="stable", inplace=True)
        all_df.to_csv(all_path, index=False, encoding="utf-8")
        log.info(f"[ALL] [DONE] {all_path} (linhas: {len(all_df):,})")
        return all_path

    return outs[0] if outs else None

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(
        description="Consolidação BDQueimadas (MANUAL × PROCESSADO) 1:1 por hora+local, com filtro opcional de bioma."
    )
    p.add_argument("--years", nargs="*", type=int, default=None,
                help="Lista de anos (ex.: --years 2012 2013). Se omitir, roda para todos com MANUAL.")
    p.add_argument("--overwrite", action="store_true",
                help="Sobrescreve saídas existentes.")
    p.add_argument("--validation", action="store_true",
                help="Modo de validação rápida: MANUAL=100; PROCESSADO=500k (após dedup).")
    p.add_argument("--biome", type=str, default=None,
                help="Filtrar por bioma (ex.: --biome 'Cerrado'). Se omitido, usa todos os biomas.")
    args = p.parse_args()

    run(
        years=args.years,
        overwrite=args.overwrite,
        validation=args.validation,
        biome=args.biome,
    )
