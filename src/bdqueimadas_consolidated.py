# src/consolidated_bdqueimadas.py
# =============================================================================
# BDQUEIMADAS — Consolidação a partir dos CSV COIDS (data/processed/ID_BDQUEIMADAS)
#
# Modo default (integração atual):
#   - Lê focos_br_ref_{ANO}/focos_br_ref_{ANO}.csv (baixados pelo bdqueimadas_scraper.py).
#   - Normaliza datas/geo-chaves como antes; filtro opcional por bioma (coluna Bioma/bioma).
#   - Dedup por (__DT_H à hora + país + UF + município) — primeira ocorrência (compatível).
# Opção legado (--legacy-manual-merge):
#   - Cruza exportador_* manual em raw/BDQUEIMADAS com o processado (fluxo antigo).
#
# Saídas:
#   - data/consolidated/BDQUEIMADAS/bdq_targets_{YYYY}[_<bioma>].csv        (por ano)
#   - data/consolidated/BDQUEIMADAS/bdq_targets_all_years[_<bioma>].csv     (multi-anos)
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir, normalize_key)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple
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
    unzip_all_in_dir,
)

# =============================================================================
# CONFIG/LOG
# =============================================================================
cfg = loadConfig()
log = get_logger("bdqueimadas.consolidate", kind="load", per_run_file=True)

# =============================================================================
# PATHS
# =============================================================================
RAW_BDQ_DIR = Path(get_path("paths", "data", "raw")) / "BDQUEIMADAS"
# Zips COIDS (mesmo layout do bdqueimadas_scraper.py)
RAW_ID_BDQ_DIR = Path(get_path("paths", "data", "raw")) / "ID_BDQUEIMADAS"
PROC_BDQ_DIR = Path(get_path("paths", "data", "processed")) / "ID_BDQUEIMADAS"
OUT_DIR = ensure_dir(Path(get_path("paths", "data", "external")) / "BDQUEIMADAS")

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

_FOCOS_FILENAME_RE = re.compile(r"^focos_br_ref_(\d{4})\.csv$", re.IGNORECASE)


def resolve_processed_focos_csv(year: int, proc_root: Path = PROC_BDQ_DIR) -> Optional[Path]:
    """
    Localiza focos_br_ref_{ano}.csv sob proc_root.

    O zip do COIDS às vezes gera uma pasta extra (ex.:
    .../focos_br_ref_2003/focos_br_ref_2003/focos_br_ref_2003.csv),
    então não basta o path “plano” de um só nível.
    """
    if not proc_root.is_dir():
        return None

    stem = f"focos_br_ref_{year}"
    canonical = proc_root / stem / f"{stem}.csv"
    if canonical.is_file():
        return canonical

    fname = f"{stem}.csv"
    matches = [p for p in proc_root.rglob(fname) if p.is_file()]
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        matches.sort(key=lambda p: len(p.parts))
        log.warning(
            "Varios %s em %s — usando %s",
            fname,
            proc_root,
            matches[0],
        )
        return matches[0]
    return None


def discover_processed_years(proc_root: Path = PROC_BDQ_DIR) -> List[int]:
    """Anos com focos_br_ref_YYYY.csv em qualquer subpasta de proc_root."""
    if not proc_root.is_dir():
        return []
    years: set[int] = set()
    for p in proc_root.rglob("focos_br_ref_*.csv"):
        if not p.is_file():
            continue
        m = _FOCOS_FILENAME_RE.match(p.name)
        if m:
            years.add(int(m.group(1)))
    return sorted(years)


def _has_any_focos_csv(proc_root: Path = PROC_BDQ_DIR) -> bool:
    return proc_root.is_dir() and any(proc_root.rglob("focos_br_ref_*.csv"))


def maybe_extract_zips_from_raw(auto: bool = True) -> None:
    """
    Se não há CSV processado mas existem *.zip em RAW_ID_BDQ_DIR, extrai para PROC_BDQ_DIR
    (mesmo comportamento do bdqueimadas_scraper). Cada zip já é Brasil inteiro naquele ano.
    """
    if not auto:
        return
    if _has_any_focos_csv(PROC_BDQ_DIR):
        return
    if not RAW_ID_BDQ_DIR.is_dir():
        return
    zips = sorted(RAW_ID_BDQ_DIR.glob("*.zip"))
    if not zips:
        return
    log.info(
        "Sem focos_br_ref_*.csv em %s — extraindo %d zip(s) de %s (1 arquivo CSV por ano).",
        PROC_BDQ_DIR,
        len(zips),
        RAW_ID_BDQ_DIR,
    )
    ensure_dir(PROC_BDQ_DIR)
    unzip_all_in_dir(RAW_ID_BDQ_DIR, PROC_BDQ_DIR, make_subdir_from_zip=True, log=log)


def _hint_missing_focos_data() -> str:
    return (
        f"Ainda sem focos_br_ref_*.csv em {PROC_BDQ_DIR}. "
        f"Coloque os zips em {RAW_ID_BDQ_DIR} ou rode: python src/bdqueimadas_scraper.py"
    )


def _column_ci(df: pd.DataFrame, candidates: Tuple[str, ...]) -> Optional[str]:
    """Resolve nome real da coluna (case-insensitive)."""
    cmap = {str(c).strip().lower(): c for c in df.columns}
    for cand in candidates:
        key = cand.strip().lower()
        if key in cmap:
            return cmap[key]
    return None


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
    """Filtra por bioma usando coluna Bioma/bioma (aliases case-insensitive)."""
    total = len(df)
    if not biome:
        log.info(f"  sem filtro de Bioma: {total:,} linhas")
        return df, len(df)

    bio_src = _column_ci(df, ("Bioma", "bioma", "BIOMA"))
    if bio_src is None:
        log.warning(
            f"  bioma solicitado ({biome!r}) mas CSV sem coluna de bioma — mantendo todas as {total:,} linhas"
        )
        return df, len(df)

    tgt = normalize_key(biome)
    df = df.copy()
    df["__BIO_KEY"] = df[bio_src].map(normalize_key)
    df = df.loc[df["__BIO_KEY"] == tgt].copy()
    log.info(f"  filtro Bioma={biome} (key={tgt}): {len(df):,} / {total:,}")
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
    df["__PAIS_KEY"] = df["Pais"].map(normalize_key)
    df["__UF_KEY"] = df["Estado"].map(normalize_key)
    df["__MUN_KEY"] = df["Municipio"].map(normalize_key)

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
    """Somente IDs/coords para merge legado MANUAL × processado (schema fixo lowercase)."""
    log.info(f"[PHASE] Lendo PROCESSADO (merge legado): {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (proc): {len(df):,} | tempo={time.time()-t0:,.2f}s")

    log.info("[PHASE] Normalizando PROCESSADO (datas, strings)")
    t1 = time.time()
    if PROC_DT_COL not in df.columns:
        dt_col = _column_ci(df, ("data_pas", "datahora", "data_hora"))
        if dt_col is None:
            raise ValueError(f"[legacy merge] Sem coluna de data ({PROC_DT_COL}). Colunas: {list(df.columns)}")
        df["__DT"] = pd.to_datetime(df[dt_col], errors="coerce")
    else:
        df["__DT"] = _parse_proc_datetime(df[PROC_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])

    mun_col = _column_ci(df, ("municipio",))
    uf_col = _column_ci(df, ("estado",))
    pais_col = _column_ci(df, ("pais",))
    if not mun_col or not uf_col:
        raise ValueError(f"[legacy merge] CSV precisa estado/municipio. Colunas: {list(df.columns)}")

    if pais_col:
        df["__PAIS_KEY"] = df[pais_col].map(normalize_key)
    else:
        df["__PAIS_KEY"] = normalize_key("brasil")
    df["__UF_KEY"] = df[uf_col].map(normalize_key)
    df["__MUN_KEY"] = df[mun_col].map(normalize_key)

    df["__KEY"] = (
        df["__DT_H"].astype("int64").astype("string") + "|" +
        df["__PAIS_KEY"].astype(str) + "|" +
        df["__UF_KEY"].astype(str) + "|" +
        df["__MUN_KEY"].astype(str)
    )

    foco_col = _column_ci(df, ("foco_id",))
    id_col = _column_ci(df, ("id_bdq",))
    lat_col = _column_ci(df, ("lat", "latitude"))
    lon_col = _column_ci(df, ("lon", "longitude", "long"))

    slim = pd.DataFrame({"__KEY": df["__KEY"]})
    slim["foco_id"] = df[foco_col] if foco_col else pd.NA
    slim["id_bdq"] = df[id_col] if id_col else pd.NA
    slim["lat"] = pd.to_numeric(df[lat_col], errors="coerce") if lat_col else np.nan
    slim["lon"] = pd.to_numeric(df[lon_col], errors="coerce") if lon_col else np.nan

    before = len(slim)
    slim = slim.drop_duplicates(subset="__KEY", keep="first")
    log.info(f"  dedup PROCESSADO por __KEY: {len(slim):,} (removidas {before - len(slim):,})")

    if validation:
        slim = slim.head(500_000).copy()
        log.info("  [VALIDATION] PROCESSADO truncado a 500k linhas (após dedup).")

    log.info(f"  normalização (proc) ok | tempo={time.time()-t1:,.2f}s")
    return slim


def load_processed_sources_only(
    path: Path,
    biome: Optional[str] = None,
    validation: bool = False,
) -> Tuple[pd.DataFrame, int]:
    """
    Fonte única: CSV focos_br_ref_* do diretório ID_BDQUEIMADAS (COIDS).
    Detecta colunas de forma tolerante (maiúsc/minús e aliases).
    """
    log.info(f"[PHASE] Consolidação só COIDS: {path}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas brutas: {len(df):,} | tempo={time.time()-t0:,.2f}s")

    if validation:
        df = df.head(500_000).copy()
        log.info("  [VALIDATION] truncado a 500k linhas após leitura.")

    dt_col = _column_ci(df, ("data_pas", "datahora", "data_hora", "dat_passagem"))
    if dt_col is None:
        raise ValueError(f"Coluna de data/hora não encontrada. Colunas: {list(df.columns)}")

    mun_col = _column_ci(df, ("municipio",))
    uf_col = _column_ci(df, ("estado",))
    if not mun_col or not uf_col:
        raise ValueError(f"Colunas estado/municipio obrigatórias. Colunas: {list(df.columns)}")

    pais_col = _column_ci(df, ("pais", "país"))
    bio_src_dbg = _column_ci(df, ("bioma", "Bioma", "BIOMA"))
    frp_col = _column_ci(df, ("frp",))
    risk_col = _column_ci(
        df,
        ("risco_fogo", "riscofogo", "risco fog", "risco_incendio", "risco incêndio", "risco_incêndio"),
    )
    foco_col = _column_ci(df, ("foco_id", "id_foco"))
    idbdq_col = _column_ci(df, ("id_bdq", "id bdq"))

    log.info(
        "  colunas: dt=%s pais=%s uf=%s mun=%s bioma=%s frp=%s risco=%s foco=%s id_bdq=%s",
        dt_col,
        pais_col or "(default Brasil)",
        uf_col,
        mun_col,
        bio_src_dbg or "-",
        frp_col or "-",
        risk_col or "-",
        foco_col or "-",
        idbdq_col or "-",
    )

    t1 = time.time()
    df["__DT"] = pd.to_datetime(df[dt_col], errors="coerce")
    df["__DT_H"] = _floor_hour(df["__DT"])

    pais_series = df[pais_col] if pais_col else pd.Series(["Brasil"] * len(df))
    df["PAIS_OUT"] = pais_series.map(_ascii_upper_no_diacritics)
    df["ESTADO_OUT"] = df[uf_col].map(_ascii_upper_no_diacritics)
    df["MUNICIPIO_OUT"] = df[mun_col].map(_ascii_upper_no_diacritics)

    df["__PAIS_KEY"] = pais_series.map(normalize_key)
    df["__UF_KEY"] = df[uf_col].map(normalize_key)
    df["__MUN_KEY"] = df[mun_col].map(normalize_key)

    df, expected_rows = _maybe_filter_biome(df, biome)

    df["__KEY"] = (
        df["__DT_H"].astype("int64").astype("string") + "|" +
        df["__PAIS_KEY"].astype(str) + "|" +
        df["__UF_KEY"].astype(str) + "|" +
        df["__MUN_KEY"].astype(str)
    )

    df["RiscoFogo"] = df[risk_col] if risk_col else pd.NA
    df["FRP"] = pd.to_numeric(df[frp_col], errors="coerce") if frp_col else pd.NA
    df["foco_id"] = df[foco_col] if foco_col else pd.NA
    df["id_bdq"] = df[idbdq_col] if idbdq_col else pd.NA

    keep = [
        "__KEY",
        "__DT_H",
        "PAIS_OUT",
        "ESTADO_OUT",
        "MUNICIPIO_OUT",
        "RiscoFogo",
        "FRP",
        "foco_id",
        "id_bdq",
    ]
    df = df[keep].copy()

    before = len(df)
    df = df.drop_duplicates(subset="__KEY", keep="first")
    log.info(f"  dedup por __KEY: {len(df):,} linhas (removidas {before - len(df):,})")

    log.info(f"  normalização COIDS ok | tempo={time.time()-t1:,.2f}s")
    return df, len(df)

# =============================================================================
# MATCHING — 1:1 (MANUAL × PROCESSADO)
# =============================================================================
def merge_manual_processed(df_m: pd.DataFrame, df_p: pd.DataFrame) -> pd.DataFrame:
    log.info("[PHASE] MERGE 1:1 (left) por __KEY (hora+país+UF+município)")
    t0 = time.time()
    cols_p = [c for c in ("__KEY", "id_bdq", "foco_id", "lat", "lon") if c in df_p.columns]
    merged = df_m.merge(df_p[cols_p], on="__KEY", how="left", validate="m:1", copy=False)
    log.info(f"  linhas pós-merge: {len(merged):,} | tempo={time.time()-t0:,.2f}s")
    return merged

# =============================================================================
# BUILD OUTPUT E ESCRITA
# =============================================================================
def build_output(merged: pd.DataFrame) -> pd.DataFrame:
    id_bdq = merged["id_bdq"] if "id_bdq" in merged.columns else pd.NA
    foco_id = merged["foco_id"] if "foco_id" in merged.columns else pd.NA
    out = pd.DataFrame({
        "DATAHORA":   merged["__DT_H"].dt.strftime("%Y-%m-%d %H:%M:%S"),
        "PAIS":       merged["PAIS_OUT"],
        "ESTADO":     merged["ESTADO_OUT"],
        "MUNICIPIO":  merged["MUNICIPIO_OUT"],
        "RISCO_FOGO": merged["RiscoFogo"],
        "FRP":        merged["FRP"],
        "ID_BDQ":     id_bdq,
        "FOCO_ID":    foco_id,
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
def _consolidate_year_legacy_manual_merge(
    year: int,
    proc_file: Path,
    out_path: Path,
    overwrite: bool,
    validation: bool,
    biome: Optional[str],
    encoding: str,
) -> Optional[Path]:
    """Fluxo antigo: exportador manual em raw/BDQUEIMADAS × CSV COIDS."""
    manual_files = [p for (y, p) in list_manual_year_files(RAW_BDQ_DIR) if y == year]
    if not manual_files:
        log.warning(f"[{year}] Nenhum exportador_*_ref_{year}.csv em {RAW_BDQ_DIR}")
        return None

    manual_path = sorted(manual_files)[-1]
    log.info(f"[{year}] [legacy] MANUAL: {manual_path.name}")
    log.info(f"[{year}] [legacy] PROC:   {proc_file.name}")

    if out_path.exists() and not overwrite:
        log.info(f"[{year}] [SKIP] {out_path.name} já existe. Use --overwrite para refazer.")
        return out_path

    df_m, expected_rows = load_manual(manual_path, biome=biome, validation=validation)
    if expected_rows == 0:
        log.warning(f"[{year}] Após filtro de Bioma, nenhuma linha no MANUAL. Abortando ano.")
        return None

    df_p = load_processed(proc_file, restrict_pairs=None, validation=validation)
    merged = merge_manual_processed(df_m, df_p)

    matched_rows = int(merged["id_bdq"].notna().sum())
    unmatched_rows = int(merged["id_bdq"].isna().sum())
    log.info(f"[{year}] EXPECTED (MANUAL após filtro) = {expected_rows:,}")
    log.info(f"[{year}] RESULT len(merge)            = {len(merged):,}  (deve == EXPECTED)")
    log.info(f"[{year}] MATCHED (com ID_BDQ)         = {matched_rows:,}")
    log.info(f"[{year}] UNMATCHED                    = {unmatched_rows:,}")

    out_df = build_output(merged)
    return write_output(out_df, out_path, encoding=encoding)


def consolidate_year(
    year: int,
    overwrite: bool = False,
    validation: bool = False,
    biome: Optional[str] = None,
    encoding: str = "utf-8",
    legacy_manual_merge: bool = False,
) -> Optional[Path]:
    proc_file = resolve_processed_focos_csv(year, PROC_BDQ_DIR)
    if not proc_file:
        log.warning(
            f"[{year}] focos_br_ref_{year}.csv não encontrado sob {PROC_BDQ_DIR} "
            "(nem em subpastas — zip COIDS pode aninhar uma pasta extra)."
        )
        return None

    out_name = _resolve_output_filename([year], biome, prefix="bdq_targets")
    out_path = OUT_DIR / out_name
    if out_path.exists() and not overwrite:
        log.info(f"[{year}] [SKIP] {out_path.name} já existe. Use --overwrite para refazer.")
        return out_path

    if legacy_manual_merge:
        return _consolidate_year_legacy_manual_merge(
            year, proc_file, out_path, overwrite, validation, biome, encoding
        )

    log.info(f"[{year}] Fonte: COIDS apenas → {proc_file}")
    merged, _n = load_processed_sources_only(proc_file, biome=biome, validation=validation)
    if len(merged) == 0:
        log.warning(f"[{year}] Nenhuma linha após filtro/dedup. Abortando ano.")
        return None

    na_id = int(merged["id_bdq"].isna().sum()) if "id_bdq" in merged.columns else len(merged)
    log.info(f"[{year}] linhas saída = {len(merged):,} | id_bdq ausente = {na_id:,}")

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
    legacy_manual_merge: bool = False,
    auto_extract_from_zips: bool = True,
) -> Optional[Path]:
    # Modo legado também precisa dos CSV COIDS em processed/
    maybe_extract_zips_from_raw(auto_extract_from_zips)

    if years:
        years = sorted({int(y) for y in years})
    elif legacy_manual_merge:
        years = sorted({y for (y, _) in list_manual_year_files(RAW_BDQ_DIR)})
    else:
        years = discover_processed_years(PROC_BDQ_DIR)

    if not years:
        log.warning("Nenhum ano encontrado para consolidar.")
        log.warning(_hint_missing_focos_data())
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
                legacy_manual_merge=legacy_manual_merge,
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
        description="Consolida BDQueimadas a partir dos CSV COIDS em ID_BDQUEIMADAS "
        "(opcionalmente cruza com exportador manual: --legacy-manual-merge)."
    )
    p.add_argument("--years", nargs="*", type=int, default=None,
                   help="Anos (ex.: --years 2012 2013). Se omitir, usa anos com focos_br_ref_* em ID_BDQUEIMADAS.")
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
    p.add_argument(
        "--legacy-manual-merge",
        action="store_true",
        help="Usa exportador_* em raw/BDQUEIMADAS cruzado com COIDS (fluxo antigo). "
        "Sem esta flag, só lê focos_br_ref_* em data/processed/ID_BDQUEIMADAS.",
    )
    p.add_argument(
        "--no-auto-extract",
        action="store_true",
        help="Não extrair automaticamente *.zip de data/raw/ID_BDQUEIMADAS "
        "quando não houver CSV em processed (default: extrai).",
    )
    args = p.parse_args()

    run(
        years=args.years,
        overwrite=args.overwrite,
        validation=args.validation,
        biome=args.biome,
        output_filename=args.output_filename,
        encoding=args.encoding,
        legacy_manual_merge=args.legacy_manual_merge,
        auto_extract_from_zips=not args.no_auto_extract,
    )
