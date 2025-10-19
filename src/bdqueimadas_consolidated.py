# src/consolidated_bdqueimadas.py
# =============================================================================
# BDQUEIMADAS — Consolidação (RAW exportador_*_ref_YYYY.csv × PROCESSADO focos_br_ref_YYYY.csv)
# Saída: data/external/BDQUEIMADAS/bdq_targets_YYYY.csv (e all_years)
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Iterable, Tuple, Dict
import re
import unicodedata
import math
import time

import pandas as pd
import numpy as np
import codecs
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
RAW_BDQ_DIR = Path(get_path("paths", "data", "raw")) / "BDQUEIMADAS"
PROC_BDQ_DIR = Path(get_path("paths", "data", "processed")) / "ID_BDQUEIMADAS"
OUT_DIR = ensure_dir(Path(get_path("paths", "data", "external")) / "BDQUEIMADAS")

# =============================================================================
# HELPERS — NORMALIZAÇÃO, DATAS, PROGRESSO
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

def _norm_loc(x: str) -> str:
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
    # Ex.: "2012/01/01 16:14:00"
    try:
        return pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S", errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def _parse_proc_datetime(s: str) -> pd.Timestamp:
    # Ex.: "2012-08-24 16:41:00"
    try:
        return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def _floor_hour(ts: pd.Series) -> pd.Series:
    return ts.dt.floor("h")

def _log_phase(title: str):
    log.info(f"[PHASE] {title}")

def _progress(i: int, total: int, every: int = 1_000, prefix: str = ""):
    if total <= 0:
        return
    if i % every == 0 or i == total:
        pct = (i / total) * 100.0
        log.info(f"{prefix} {pct:6.2f}% ({i:,}/{total:,})")

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
PROC_DT_COL = "data_pas"

def load_manual(path: Path) -> pd.DataFrame:
    _log_phase(f"Lendo MANUAL: {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (manual): {len(df):,}  | tempo={time.time()-t0:,.2f}s")

    _log_phase("Normalizando MANUAL (datas, chaves, strings)")
    t1 = time.time()
    df["__DT"] = _parse_manual_datetime(df[MANUAL_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])
    df["__PAIS"] = df["Pais"].map(_norm_loc)
    df["__UF"]   = df["Estado"].map(_norm_loc)
    df["__MUN"]  = df["Municipio"].map(_norm_loc)

    for c in ("Latitude","Longitude","FRP"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    keep = ["__DT_H","__PAIS","__UF","__MUN","RiscoFogo","FRP","Latitude","Longitude"]
    df = df[keep].copy()
    log.info(f"  normalização (manual) ok | tempo={time.time()-t1:,.2f}s")
    return df

def load_processed(path: Path) -> pd.DataFrame:
    _log_phase(f"Lendo PROCESSADO: {path.name}")
    t0 = time.time()
    df = _read_csv_smart(path)
    log.info(f"  linhas lidas (proc): {len(df):,}  | tempo={time.time()-t0:,.2f}s")

    _log_phase("Normalizando PROCESSADO (datas, chaves, strings)")
    t1 = time.time()
    df["__DT"] = _parse_proc_datetime(df[PROC_DT_COL])
    df["__DT_H"] = _floor_hour(df["__DT"])
    df["__PAIS"] = df["pais"].map(_norm_loc)
    df["__UF"]   = df["estado"].map(_norm_loc)
    df["__MUN"]  = df["municipio"].map(_norm_loc)

    for c in ("lat","lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    keep = ["__DT_H","__PAIS","__UF","__MUN","id_bdq","foco_id","lat","lon"]
    df = df[keep].copy()
    log.info(f"  normalização (proc) ok | tempo={time.time()-t1:,.2f}s")
    return df

# =============================================================================
# MATCHING — tolerância temporal e raio geográfico
# =============================================================================
def _haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p = math.pi/180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = (math.sin(dlat/2)**2) + math.cos(lat1*p)*math.cos(lat2*p)*(math.sin(dlon/2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def _explode_time_with_tolerance(df: pd.DataFrame, col: str, minutes: int) -> pd.DataFrame:
    """
    Duplica linhas para janelas de tolerância temporal: 0, ±minutes.
    Útil para compensar pequenos desalinhamentos de horário.
    """
    if minutes <= 0:
        out = df.copy()
        out["__DTHWIN"] = out[col]
        return out

    offs = [0, minutes, -minutes]
    frames = []
    for m in offs:
        tmp = df.copy()
        tmp["__DTHWIN"] = tmp[col] + pd.to_timedelta(m, unit="m")
        frames.append(tmp)
    return pd.concat(frames, ignore_index=True)

def merge_candidates(
    manual: pd.DataFrame,
    proc: pd.DataFrame,
    time_tolerance_min: int = 60,
) -> pd.DataFrame:
    """
    Cria pares candidatos por (PAIS, UF, janela_horária_com_tolerância).
    Não exige município igual nesse passo.
    """
    _log_phase(f"GERANDO candidatos (tolerância temporal = ±{time_tolerance_min} min)")
    m_exp = _explode_time_with_tolerance(manual, "__DT_H", time_tolerance_min)
    p_exp = _explode_time_with_tolerance(proc, "__DT_H", time_tolerance_min)

    # chaves para reduzir o espaço de busca
    m_exp["__JOIN_KEY"] = m_exp["__DTHWIN"].astype("int64").astype("string") + "|" + m_exp["__PAIS"] + "|" + m_exp["__UF"]
    p_exp["__JOIN_KEY"] = p_exp["__DTHWIN"].astype("int64").astype("string") + "|" + p_exp["__PAIS"] + "|" + p_exp["__UF"]

    # anexa índice original para seleção posterior
    m_exp = m_exp.reset_index().rename(columns={"index":"__IDX_M"})
    p_exp = p_exp.reset_index().rename(columns={"index":"__IDX_P"})

    t0 = time.time()
    cand = m_exp.merge(
        p_exp,
        on="__JOIN_KEY",
        how="left",
        suffixes=("_m","_p")
    )
    log.info(f"  candidatos: {len(cand):,} | tempo={time.time()-t0:,.2f}s")
    return cand

def pick_best_for_each_manual(
    cand: pd.DataFrame,
    geo_radius_m: float = 30000.0,
    prefer_same_mun: bool = True,
) -> Tuple[pd.DataFrame, Dict[str,int]]:
    """
    Para cada linha manual (__IDX_M), escolhe UM melhor par:
    1) Se houver município igual, seleciona o mais próximo (se coords disponíveis).
    2) Senão, seleciona o mais próximo dentro do raio (se coords).
    3) Na falta de coords, seleciona a primeira ocorrência.
    """
    _log_phase(f"SELECIONANDO melhor par por linha manual (raio={geo_radius_m:.0f} m)")
    total_manual = cand["__IDX_M"].nunique()

    # calcula distância quando possível
    lat_m = cand["Latitude"]
    lon_m = cand["Longitude"]
    lat_p = cand["lat"]
    lon_p = cand["lon"]
    have_coords = lat_m.notna() & lon_m.notna() & lat_p.notna() & lon_p.notna()
    dist = pd.Series(np.nan, index=cand.index, dtype="float64")
    dist.loc[have_coords] = [
        _haversine(la_m, lo_m, la_p, lo_p)
        for la_m, lo_m, la_p, lo_p in zip(
            lat_m[have_coords].astype(float),
            lon_m[have_coords].astype(float),
            lat_p[have_coords].astype(float),
            lon_p[have_coords].astype(float),
        )
    ]
    cand["__DIST_M"] = dist
    cand["__MUN_EQ"] = (cand["__MUN_m"] == cand["__MUN_p"])

    sel_idx = []
    gb = cand.groupby("__IDX_M", sort=False, dropna=False)
    for i, (k, sub) in enumerate(gb, start=1):
        # 1) prefer município igual
        if prefer_same_mun:
            sub_eq = sub[sub["__MUN_EQ"] & sub["id_bdq"].notna()]
            if not sub_eq.empty:
                # se tiver distância, pega o menor; senão, a primeira
                if sub_eq["__DIST_M"].notna().any():
                    j = sub_eq["__DIST_M"].idxmin()
                else:
                    j = sub_eq.index[0]
                sel_idx.append(j)
                _progress(i, total_manual, every=5000, prefix="  progresso seleção:")
                continue

        # 2) sem município: usa raio, se disponível
        sub_geo = sub[(sub["id_bdq"].notna()) & (sub["__DIST_M"].notna())]
        if geo_radius_m > 0 and not sub_geo.empty:
            sub_geo = sub_geo[sub_geo["__DIST_M"] <= geo_radius_m]
        if not sub_geo.empty:
            j = sub_geo["__DIST_M"].idxmin()
            sel_idx.append(j)
            _progress(i, total_manual, every=5000, prefix="  progresso seleção:")
            continue

        # 3) fallback: qualquer match disponível (sem coords)
        sub_any = sub[sub["id_bdq"].notna()]
        if not sub_any.empty:
            sel_idx.append(sub_any.index[0])
        # 4) se nada, ficará NaN
        _progress(i, total_manual, every=5000, prefix="  progresso seleção:")

    picked = cand.loc[sel_idx].copy() if sel_idx else cand.head(0).copy()

    matched = int(picked["id_bdq"].notna().sum())
    stats = {
        "total_manual_rows": int(total_manual),
        "matched_rows": matched,
        "unmatched_manual_rows": int(total_manual - matched),
    }
    log.info(f"  seleção concluída | matched={matched:,} / {total_manual:,}")
    return picked, stats

# =============================================================================
# BUILD OUTPUT
# =============================================================================
OUT_COLS = ["DATAHORA","PAIS","ESTADO","MUNICIPIO","RISCO_FOGO","FRP","ID_BDQ","FOCO_ID"]

def build_output(df_sel: pd.DataFrame) -> pd.DataFrame:
    out = pd.DataFrame({
        "DATAHORA": df_sel["__DTHWIN_m"],
        "PAIS": df_sel["__PAIS_m"],
        "ESTADO": df_sel["__UF_m"],
        "MUNICIPIO": df_sel["__MUN_m"],
        "RISCO_FOGO": df_sel.get("RiscoFogo"),
        "FRP": df_sel.get("FRP"),
        "ID_BDQ": df_sel.get("id_bdq"),
        "FOCO_ID": df_sel.get("foco_id"),
    })
    out = out.sort_values(["DATAHORA","ESTADO","MUNICIPIO"], kind="stable").reset_index(drop=True)
    return out

# =============================================================================
# PIPELINE POR ANO
# =============================================================================
def consolidate_year(
    year: int,
    overwrite: bool = False,
    geo_radius_m: float = 30000.0,
    validation: bool = False,
    time_tolerance_min: int = 60,
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

    df_m = load_manual(manual_path)
    df_p = load_processed(proc_file)
    if validation:
        df_m = df_m.head(100).copy()
        df_p = df_p.head(20000).copy()  # deixa mais candidatos no proc
        log.info(f"[{year}] [VALIDATION] Limitando MANUAL=100, PROC=20k linhas p/ validação rápida.")

    cand = merge_candidates(df_m, df_p, time_tolerance_min=time_tolerance_min)
    picked, stats = pick_best_for_each_manual(cand, geo_radius_m=geo_radius_m, prefer_same_mun=True)

    # Amostras de não-casados (para inspeção)
    n_manual = df_m.shape[0]
    not_matched_idx = sorted(set(range(n_manual)) - set(picked["__IDX_M"].unique()))
    if not_matched_idx:
        show = min(5, len(not_matched_idx))
        log.info(f"[{year}] Exemplos (até {show}) sem correspondência após seleção:")
        for i in not_matched_idx[:show]:
            r = df_m.iloc[i]
            log.info(f"   - {r['__DT_H']} | {r['__UF']}/{r['__MUN']} | FRP={r.get('FRP')} | lat={r.get('Latitude')} lon={r.get('Longitude')}")

    out_df = build_output(picked)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"[{year}] [DONE] {out_path}  (linhas: {len(out_df):,})")
    log.info(f"[{year}] STATS: {stats}")
    return out_path

# =============================================================================
# MAIN/CLI
# =============================================================================
def run(
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    geo_radius_m: float = 30000.0,
    validation: bool = False,
    time_tolerance_min: int = 60,
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
                geo_radius_m=geo_radius_m,
                validation=validation,
                time_tolerance_min=time_tolerance_min,
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
        description="Consolidação BDQueimadas (manual × processado) -> bdq_targets_YYYY.csv"
    )
    p.add_argument("--years", nargs="*", type=int, default=None,
                help="Lista de anos a consolidar (ex.: --years 2013 2019). Se omitido, roda para todos.")
    p.add_argument("--overwrite", action="store_true", help="Sobrescreve saídas existentes.")
    p.add_argument("--geo-radius-m", type=float, default=30000.0,
                help="Raio geográfico para validação (m). 0 desativa filtro por distância.")
    p.add_argument("--time-tolerance-min", type=int, default=60,
                help="Tolerância temporal (min) aplicada como janelas 0, ±t. Recom.: 60.")
    p.add_argument("--validation", action="store_true",
                help="Modo de validação rápida: limita MANUAL=100, PROC=20k.")
    args = p.parse_args()

    run(
        years=args.years,
        overwrite=args.overwrite,
        geo_radius_m=args.geo_radius_m,
        validation=args.validation,
        time_tolerance_min=args.time_tolerance_min,
    )
