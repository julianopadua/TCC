# src/consolidated_bdqueimadas.py
# =============================================================================
# BDQUEIMADAS — Consolidação (manual raw/exportador_*_ref_YYYY.csv  ×  processado focos_br_ref_YYYY.csv)
# Saída: data/external/BDQUEIMADAS/bdq_targets_YYYY.csv (e all_years, se aplicável)
# Dependências: utils.py (loadConfig, get_logger, get_path, ensure_dir)
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
def _norm_str(x: str) -> str:
    if x is None:
        return ""
    x = str(x).strip()
    x = unicodedata.normalize("NFKD", x)
    x = "".join(ch for ch in x if not unicodedata.combining(ch))
    return x.upper().strip()

def _parse_manual_datetime(s: str) -> pd.Timestamp:
    # Ex.: "2013/01/01 16:25:00"
    try:
        return pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S", errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def _parse_proc_datetime(s: str) -> pd.Timestamp:
    # Ex.: "2003-05-15 17:05:00"
    try:
        return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
    except Exception:
        return pd.to_datetime(s, errors="coerce")

def _floor_minute(ts: pd.Series) -> pd.Series:
    # padroniza para precisão de minuto (segundos=0)
    return ts.dt.floor("min")

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
    df = pd.read_csv(
        path,
        encoding="latin1",
        low_memory=False,
        on_bad_lines="skip"
    )
    log.info(f"  linhas lidas (manual): {len(df):,}  | tempo={time.time()-t0:,.2f}s")

    _log_phase("Normalizando MANUAL (datas, chaves, strings)")
    t1 = time.time()
    df["__DT"] = _parse_manual_datetime(df[MANUAL_DT_COL])
    df["__DT_MIN"] = _floor_minute(df["__DT"])
    df["__PAIS"] = df["Pais"].map(_norm_str)
    df["__UF"] = df["Estado"].map(_norm_str)
    df["__MUN"] = df["Municipio"].map(_norm_str)
    # chaves de merge
    df["__KEY"] = df["__DT_MIN"].astype("int64").astype("string") + "|" + df["__PAIS"] + "|" + df["__UF"] + "|" + df["__MUN"]
    # Coords numéricas (podem estar vazias)
    for c in ("Latitude","Longitude"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = ["__KEY","__DT_MIN","__PAIS","__UF","__MUN","RiscoFogo","FRP","Latitude","Longitude"]
    df = df[keep].copy()
    log.info(f"  normalização (manual) ok | tempo={time.time()-t1:,.2f}s")
    return df

def load_processed(path: Path) -> pd.DataFrame:
    _log_phase(f"Lendo PROCESSADO: {path.name}")
    t0 = time.time()
    df = pd.read_csv(
        path,
        encoding="latin1",
        low_memory=False,
        on_bad_lines="skip"
    )
    log.info(f"  linhas lidas (proc): {len(df):,}  | tempo={time.time()-t0:,.2f}s")

    _log_phase("Normalizando PROCESSADO (datas, chaves, strings)")
    t1 = time.time()
    df["__DT"] = _parse_proc_datetime(df[PROC_DT_COL])
    df["__DT_MIN"] = _floor_minute(df["__DT"])
    df["__PAIS"] = df["pais"].map(_norm_str)
    df["__UF"] = df["estado"].map(_norm_str)
    df["__MUN"] = df["municipio"].map(_norm_str)
    df["__KEY"] = df["__DT_MIN"].astype("int64").astype("string") + "|" + df["__PAIS"] + "|" + df["__UF"] + "|" + df["__MUN"]
    for c in ("lat","lon"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    keep = ["__KEY","__DT_MIN","__PAIS","__UF","__MUN","id_bdq","foco_id","lat","lon"]
    df = df[keep].copy()
    log.info(f"  normalização (proc) ok | tempo={time.time()-t1:,.2f}s")
    return df

# =============================================================================
# MATCHING — por chave e desambiguação com PROGRESSO (sem raio por padrão)
# =============================================================================
def _haversine(lat1, lon1, lat2, lon2):
    # metros (aprox)
    R = 6371000.0
    p = math.pi/180.0
    dlat = (lat2 - lat1) * p
    dlon = (lon2 - lon1) * p
    a = (math.sin(dlat/2)**2) + math.cos(lat1*p)*math.cos(lat2*p)*(math.sin(dlon/2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    return R * c

def _select_best_group(sub: pd.DataFrame) -> pd.DataFrame:
    # se não há múltiplas linhas para a mesma linha manual, simplesmente retorna
    if sub["id_bdq"].notna().sum() <= 1:
        return sub.iloc[[0]]

    # Se temos coords nos dois lados, escolhe menor distância
    lat_m = sub["Latitude"].iloc[0] if "Latitude" in sub.columns else np.nan
    lon_m = sub["Longitude"].iloc[0] if "Longitude" in sub.columns else np.nan

    if pd.notna(lat_m) and pd.notna(lon_m):
        distances = []
        for i, row in sub.iterrows():
            lat_p, lon_p = row.get("lat"), row.get("lon")
            if pd.notna(lat_p) and pd.notna(lon_p):
                d = _haversine(float(lat_m), float(lon_m), float(lat_p), float(lon_p))
            else:
                d = float("inf")
            distances.append((i, d))
        idx, _ = min(distances, key=lambda t: t[1])
        return sub.loc[[idx]]

    # Sem coordenadas, mantém a primeira linha
    return sub.iloc[[0]]

def merge_by_key_with_geo(
    manual: pd.DataFrame,
    proc: pd.DataFrame,
    geo_radius_m: float = 0.0,          # <- desativado por padrão
    log_every_groups: int = 5_000,
    radius_batch: int = 250_000,
) -> Tuple[pd.DataFrame, Dict[str, int]]:
    """
    1) Merge por __KEY (DataHora_min, País, UF, Município) — loga contagem.
    2) Desambiguação por grupos (KEY + atributos manuais) — loga % de grupos processados.
    3) (Opcional) Filtro por raio geográfico se geo_radius_m > 0.
    """
    _log_phase("MERGE bruto por __KEY")
    t0 = time.time()
    m = manual.merge(proc, on="__KEY", how="left", suffixes=("_m","_p"))
    casados_brutos = int(m["id_bdq"].notna().sum())
    log.info(f"  linhas pós-merge: {len(m):,} | hits (id_bdq!=NaN): {casados_brutos:,} | tempo={time.time()-t0:,.2f}s")

    total_manual = len(manual)

    _log_phase("DESAMBIGUAÇÃO por grupos (KEY+FRP+RiscoFogo+coords)")
    group_cols = ["__KEY","RiscoFogo","FRP","Latitude","Longitude"]
    gb = m.groupby(group_cols, dropna=False)
    n_groups = gb.ngroups
    log.info(f"  grupos a resolver: {n_groups:,}")

    sel_groups = []
    t1 = time.time()
    for idx, (_, sub) in enumerate(gb, start=1):
        sel_groups.append(_select_best_group(sub))
        _progress(idx, n_groups, every=log_every_groups, prefix="  progresso desambiguação:")
    mm = pd.concat(sel_groups, ignore_index=True)
    log.info(f"  desambiguação concluída | linhas={len(mm):,} | tempo={time.time()-t1:,.2f}s")

    # (Opcional) Filtro por raio — só roda se geo_radius_m > 0
    if geo_radius_m is not None and math.isfinite(geo_radius_m) and geo_radius_m > 0:
        _log_phase(f"VALIDAÇÃO por raio geográfico (<= {geo_radius_m:.0f} m)")
        t2 = time.time()
        mask = pd.Series(True, index=mm.index)
        idx_valid = mm["id_bdq"].notna()
        mm_valid = mm.loc[idx_valid].copy()
        n = len(mm_valid)
        if n > 0:
            for start in range(0, n, radius_batch):
                end = min(start + radius_batch, n)
                chunk = mm_valid.iloc[start:end]
                ok = []
                for _, row in chunk.iterrows():
                    lat_m, lon_m = row.get("Latitude"), row.get("Longitude")
                    lat_p, lon_p = row.get("lat"), row.get("lon")
                    if pd.notna(lat_m) and pd.notna(lon_m) and pd.notna(lat_p) and pd.notna(lon_p):
                        d = _haversine(float(lat_m), float(lon_m), float(lat_p), float(lon_p))
                        ok.append(d <= geo_radius_m)
                    else:
                        ok.append(True)
                mask.loc[chunk.index] = ok
                _progress(end, n, every=max(10_000, radius_batch//4), prefix="  progresso raio:")
        mm = mm.loc[mask].copy()
        log.info(f"  raio checado | mantidos: {len(mm):,} | tempo={time.time()-t2:,.2f}s")

    matched = int(mm["id_bdq"].notna().sum())
    unmatched_manual = int((mm["id_bdq"].isna()).sum())

    keys_manual = set(manual["__KEY"].unique())
    keys_proc = set(proc["__KEY"].unique())
    unmatched_proc_keys = int(len(keys_proc - keys_manual))

    stats = {
        "total_manual_rows": int(total_manual),
        "matched_rows": matched,
        "unmatched_manual_rows": unmatched_manual,
        "raw_merge_matches": int(casados_brutos),
        "unmatched_proc_keys": unmatched_proc_keys,
    }
    return mm, stats

# =============================================================================
# BUILD OUTPUT
# =============================================================================
OUT_COLS = ["DATAHORA","PAIS","ESTADO","MUNICIPIO","RISCO_FOGO","FRP","ID_BDQ","FOCO_ID"]

def _pickcol(df: pd.DataFrame, base: str):
    """Escolhe a coluna 'base' mesmo após merge (prefere _m, depois _p)."""
    if base in df.columns:
        return df[base]
    col_m = f"{base}_m"
    col_p = f"{base}_p"
    if col_m in df.columns:
        return df[col_m]
    if col_p in df.columns:
        return df[col_p]
    raise KeyError(base)

def build_output(df_merged: pd.DataFrame) -> pd.DataFrame:
    dt      = _pickcol(df_merged, "__DT_MIN")
    pais    = _pickcol(df_merged, "__PAIS")
    uf      = _pickcol(df_merged, "__UF")
    mun     = _pickcol(df_merged, "__MUN")

    out = pd.DataFrame({
        "DATAHORA": dt,
        "PAIS": pais,
        "ESTADO": uf,
        "MUNICIPIO": mun,
        "RISCO_FOGO": df_merged.get("RiscoFogo"),
        "FRP": df_merged.get("FRP"),
        "ID_BDQ": df_merged.get("id_bdq"),
        "FOCO_ID": df_merged.get("foco_id"),
    })
    out = out.sort_values(["DATAHORA","ESTADO","MUNICIPIO"], kind="stable").reset_index(drop=True)
    return out

# =============================================================================
# PIPELINE POR ANO
# =============================================================================
def consolidate_year(year: int, overwrite: bool = False, geo_radius_m: float = 0.0) -> Optional[Path]:
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

    merged, stats = merge_by_key_with_geo(df_m, df_p, geo_radius_m=geo_radius_m)

    n_show = min(5, int((merged["id_bdq"].isna()).sum()))
    if n_show > 0:
        sample_un = merged.loc[merged["id_bdq"].isna(), ["__DT_MIN_m","__DT_MIN_p","__PAIS_m","__UF_m","__MUN_m","FRP"]].head(n_show)
        log.info(f"[{year}] Exemplos (até {n_show}) sem correspondência:")
        for _, r in sample_un.iterrows():
            dt_show = r.get("__DT_MIN_m") if pd.notna(r.get("__DT_MIN_m")) else r.get("__DT_MIN_p")
            uf_show = r.get("__UF_m")
            mun_show = r.get("__MUN_m")
            frp_show = r.get("FRP")
            log.info(f"   - {dt_show} | {uf_show}/{mun_show} | FRP={frp_show}")

    out_df = build_output(merged)
    out_df.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"[{year}] [DONE] {out_path}  (linhas: {len(out_df):,})")
    log.info(f"[{year}] STATS: {stats}")
    return out_path

# =============================================================================
# MAIN/CLI
# =============================================================================
def run(years: Optional[Iterable[int]] = None, overwrite: bool = False, geo_radius_m: float = 0.0) -> Optional[Path]:
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
            p = consolidate_year(y, overwrite=overwrite, geo_radius_m=geo_radius_m)
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
    p.add_argument("--geo-radius-m", type=float, default=0.0,
                   help="0 = sem filtro geográfico; >0 valida por distância (m).")
    args = p.parse_args()

    run(years=args.years, overwrite=args.overwrite, geo_radius_m=args.geo_radius_m)
