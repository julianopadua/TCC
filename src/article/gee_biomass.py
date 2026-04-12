# src/article/gee_biomass.py
# =============================================================================
# Etapa 1 — Extração semanal de biomassa GEE (buffer + ponto) e propagação.
#
# Fluxo:
#   1. Extrair série semanal (MOD13Q1 por padrão) na base canônica, por
#      (cidade_norm, gee_site_key): buffer centrado em (lat_foco, lon_foco)
#      e amostra no ponto do foco.
#   2. merge_asof (backward) por (cidade_norm, gee_site_key) para alinhar
#      ts_hour ao composto semanal.
#   3. ffill por grupo (cidade_norm, gee_site_key).
#   4. Propagar colunas para outras bases via merge em
#      (cidade_norm, gee_site_key, ts_hour).
#
# Requer earthengine-api, credenciais (conta de serviço recomendada) e quota
# adequada — execução multi-ano com muitos focos é custosa.
# =============================================================================
from __future__ import annotations

import gc
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.article.config import ArticlePipelineConfig, load_article_config
from src.article.processing_issues import IssueLogger
from src.integrations.inmet_gee.ee_init import call_gee_with_retry, initialize_earth_engine
from src.utils import ensure_dir, get_logger

STAGE = "gee_biomass"
PARQUET_TEMPLATE = "inmet_bdq_{year}_cerrado.parquet"

FOCO_ID_COL = "FOCO_ID"
SITE_KEY_COL = "gee_site_key"


def compute_gee_site_key(df: pd.DataFrame) -> pd.Series:
    """
    Chave estável por foco: FOCO_ID quando válido; senão lat/lon do foco
    arredondados (5 casas).
    """
    lat = pd.to_numeric(df["lat_foco"], errors="coerce")
    lon = pd.to_numeric(df["lon_foco"], errors="coerce")
    coord_key = lat.round(5).astype(str) + "_" + lon.round(5).astype(str)

    if FOCO_ID_COL not in df.columns:
        return coord_key

    fid = df[FOCO_ID_COL].astype(str).str.strip()
    valid = fid.notna() & (fid != "") & (fid != "nan") & (fid != "<NA>")
    return np.where(valid, "foco_" + fid, coord_key)


def ensure_gee_site_key(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out[SITE_KEY_COL] = compute_gee_site_key(out)
    return out


# ---------------------------------------------------------------------------
# GEE — extração semanal MOD13Q1 (NDVI/EVI buffer + ponto)
# ---------------------------------------------------------------------------
def _mod13_prepare_image(img: Any, ee: Any, bands: List[str]) -> Any:
    """Escala 0.0001 e máscara de fill (-3000) por banda (MOD13Q1)."""
    acc = img.select(bands[0]).neq(-3000)
    for b in bands[1:]:
        acc = acc.And(img.select(b).neq(-3000))
    return img.select(bands).multiply(0.0001).updateMask(acc)


def _chunked(iterable: List[Any], size: int) -> List[List[Any]]:
    return [iterable[i : i + size] for i in range(0, len(iterable), size)]


def _features_to_map(fc_dict: Optional[Dict], value_keys: List[str]) -> Dict[str, Dict[str, Any]]:
    if not fc_dict or "features" not in fc_dict:
        return {}
    out: Dict[str, Dict[str, Any]] = {}
    for feat in fc_dict["features"]:
        props = feat.get("properties") or {}
        sk = props.get("gee_site_key")
        if sk is None:
            continue
        sk = str(sk)
        out[sk] = {k: props.get(k) for k in value_keys}
    return out


def extract_weekly_biomass_gee(
    sites_df: pd.DataFrame,
    year: int,
    cfg: ArticlePipelineConfig,
    log: logging.Logger,
    ee_mod: Any,
) -> Optional[pd.DataFrame]:
    """
    Extrai série semanal NDVI/EVI (buffer + ponto) para cada site.

    sites_df: colunas gee_site_key, cidade_norm, lat_foco, lon_foco (linhas únicas).
    Retorna DataFrame longo: cidade_norm, gee_site_key, composite_start, NDVI_buffer, ...
    """
    if sites_df is None or len(sites_df) == 0:
        log.warning("Nenhum site para extração GEE no ano %d.", year)
        return None

    gcfg = cfg.gee
    ee = ee_mod
    ra = gcfg.gee_retry_max_attempts
    chunk_sz = max(1, gcfg.sites_chunk_size)
    tile_scale = max(1, gcfg.tile_scale)
    pause_s = max(0.0, gcfg.pause_between_chunks_s)
    buffer_m = gcfg.buffer_radius_km * 1000.0
    bands = list(gcfg.bands)
    if not bands:
        log.warning("article_pipeline.gee.bands vazio — nada a extrair.")
        return None

    rows: List[Dict[str, Any]] = []

    sites_df = sites_df.copy()
    sites_df["lat_foco"] = pd.to_numeric(sites_df["lat_foco"], errors="coerce")
    sites_df["lon_foco"] = pd.to_numeric(sites_df["lon_foco"], errors="coerce")
    valid = sites_df["lat_foco"].notna() & sites_df["lon_foco"].notna()
    n_bad = (~valid).sum()
    if n_bad:
        log.warning("Removendo %d sites sem lat_foco/lon_foco válidos (ano %d).", int(n_bad), year)
    sites_df = sites_df.loc[valid].copy()
    if len(sites_df) == 0:
        return None

    lons = sites_df["lon_foco"].to_numpy()
    lats = sites_df["lat_foco"].to_numpy()
    pad = 0.5
    region = ee.Geometry.Rectangle(
        [
            float(lons.min() - pad),
            float(lats.min() - pad),
            float(lons.max() + pad),
            float(lats.max() + pad),
        ]
    )

    start = f"{year}-01-01"
    end = f"{year + 1}-01-01"
    col = (
        ee.ImageCollection(gcfg.image_collection)
        .filterDate(start, end)
        .filterBounds(region)
        .sort("system:time_start")
    )

    n_images = call_gee_with_retry(
        log, lambda: col.size().getInfo(), max_attempts=ra, base_delay=5.0, max_delay=120.0
    )
    if n_images is None:
        log.error("Falha ao obter tamanho da coleção GEE (ano %d).", year)
        return None
    if n_images == 0:
        log.warning("Coleção GEE vazia para o ano %d após filterDate/filterBounds.", year)
        return None

    list_obj = col.toList(int(n_images))
    site_records = sites_df.to_dict("records")
    chunks = _chunked(site_records, chunk_sz)

    log.info(
        "  GEE ano %d: %d imagens, %d sites em chunks de até %d (tileScale=%d).",
        year,
        int(n_images),
        len(site_records),
        chunk_sz,
        tile_scale,
    )

    for i in range(int(n_images)):

        def _get_img(idx: int = i) -> Any:
            return ee.Image(list_obj.get(idx))

        img_raw = _get_img()
        t0 = call_gee_with_retry(
            log, lambda: ee.Image(list_obj.get(i)).get("system:time_start").getInfo(),
            max_attempts=ra,
        )
        if t0 is None:
            log.warning("Pulando imagem %d/%d: sem system:time_start.", i + 1, n_images)
            continue
        composite_start = pd.to_datetime(t0, unit="ms")

        img = _mod13_prepare_image(img_raw, ee, bands)

        for chunk in chunks:
            buf_feats = []
            pt_feats = []
            for rec in chunk:
                lat = float(rec["lat_foco"])
                lon = float(rec["lon_foco"])
                sk = str(rec["gee_site_key"])
                cid = str(rec["cidade_norm"])
                pt_geom = ee.Geometry.Point([lon, lat])
                buf_geom = pt_geom.buffer(buffer_m)
                props = {"gee_site_key": sk, "cidade_norm": cid}
                buf_feats.append(ee.Feature(buf_geom, props))
                pt_feats.append(ee.Feature(pt_geom, props))

            buf_fc = ee.FeatureCollection(buf_feats)
            pt_fc = ee.FeatureCollection(pt_feats)

            def _reduce_buf():
                return img.reduceRegions(
                    collection=buf_fc,
                    reducer=ee.Reducer.mean(),
                    scale=gcfg.scale_m,
                    tileScale=tile_scale,
                ).getInfo()

            def _reduce_pt():
                return img.reduceRegions(
                    collection=pt_fc,
                    reducer=ee.Reducer.mean(),
                    scale=gcfg.scale_m,
                    tileScale=tile_scale,
                ).getInfo()

            buf_info = call_gee_with_retry(log, _reduce_buf, max_attempts=ra)
            if pause_s > 0:
                time.sleep(pause_s)
            pt_info = call_gee_with_retry(log, _reduce_pt, max_attempts=ra)
            if pause_s > 0:
                time.sleep(pause_s)

            if buf_info is None or pt_info is None:
                log.warning(
                    "reduceRegions falhou (imagem %d/%d, chunk com %d sites).",
                    i + 1,
                    n_images,
                    len(chunk),
                )
                continue

            bm = _features_to_map(buf_info, bands + ["cidade_norm"])
            pm = _features_to_map(pt_info, bands)

            for rec in chunk:
                sk = str(rec["gee_site_key"])
                bprops = bm.get(sk, {})
                pprops = pm.get(sk, {})
                row: Dict[str, Any] = {
                    "cidade_norm": rec["cidade_norm"],
                    "gee_site_key": sk,
                    "composite_start": composite_start,
                }
                for b in bands:
                    row[f"{b}_buffer"] = bprops.get(b)
                    row[f"{b}_point"] = pprops.get(b)
                rows.append(row)

        if (i + 1) % 5 == 0 or (i + 1) == n_images:
            log.info("  Processadas %d/%d imagens MOD13 (%d).", i + 1, n_images, year)

    if not rows:
        return None
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Alinhamento semanal → horário via merge_asof + ffill
# ---------------------------------------------------------------------------
def align_weekly_to_hourly(
    df_hourly: pd.DataFrame,
    df_weekly: pd.DataFrame,
    biomass_cols: List[str],
    station_col: str = "cidade_norm",
    site_key_col: str = SITE_KEY_COL,
    ts_col: str = "ts_hour",
) -> pd.DataFrame:
    """
    Alinha valores semanais de biomassa às linhas horárias.

    merge_asof backward por (station_col, site_key_col). Com `by=`, o pandas exige
    que a coluna `on` seja monotonicamente crescente no left inteiro; com vários
    sites isso falha. Aplicamos merge_asof por grupo e restauramos a ordem original.
    """
    df = df_hourly.copy()
    for c in biomass_cols:
        if c in df.columns:
            df = df.drop(columns=[c])

    df["_ts"] = pd.to_datetime(df[ts_col])
    df["_align_order"] = np.arange(len(df), dtype=np.int64)

    weekly = df_weekly.copy()
    weekly["_ts"] = pd.to_datetime(weekly["composite_start"])

    for col in (station_col, site_key_col):
        df[col] = df[col].astype("string")
        weekly[col] = weekly[col].astype("string")

    merge_cols = [station_col, site_key_col, "_ts"] + biomass_cols
    weekly_sub = weekly[[c for c in merge_cols if c in weekly.columns]]

    parts: List[pd.DataFrame] = []
    for (_cid, _sk), g_left in df.groupby([station_col, site_key_col], sort=False):
        g_left = g_left.sort_values("_ts")
        mask = (weekly_sub[station_col] == _cid) & (weekly_sub[site_key_col] == _sk)
        g_right = weekly_sub.loc[mask]
        if g_right.empty:
            g_out = g_left.copy()
            for col in biomass_cols:
                g_out[col] = np.nan
            parts.append(g_out)
            continue
        g_right = g_right.sort_values("_ts")
        right_take = ["_ts"] + [c for c in biomass_cols if c in g_right.columns]
        g_right_m = g_right[right_take].drop_duplicates(subset=["_ts"], keep="last")
        m = pd.merge_asof(g_left, g_right_m, on="_ts", direction="backward")
        parts.append(m)

    merged = pd.concat(parts, axis=0).sort_values("_align_order")
    merged = merged.drop(columns=["_align_order"])

    for col in biomass_cols:
        merged[col] = merged.groupby([station_col, site_key_col], sort=False)[col].ffill()

    merged.drop(columns=["_ts"], inplace=True)
    return merged


# ---------------------------------------------------------------------------
# Propagação canônica → outras bases
# ---------------------------------------------------------------------------
def propagate_biomass(
    canonical_df: pd.DataFrame,
    target_parquet: Path,
    biomass_cols: List[str],
    issues: IssueLogger,
    log: logging.Logger,
    year: int,
    station_col: str = "cidade_norm",
    site_key_col: str = SITE_KEY_COL,
    ts_col: str = "ts_hour",
) -> pd.DataFrame:
    """
    Propaga colunas de biomassa da base canônica para outra base.

    Merge na chave (cidade_norm, gee_site_key, ts_hour).
    """
    target_df = pd.read_parquet(target_parquet)
    n_target = len(target_df)

    target_df = ensure_gee_site_key(target_df)

    key_cols = [station_col, site_key_col, ts_col]
    canon_subset = canonical_df[key_cols + biomass_cols].drop_duplicates(subset=key_cols)

    result = target_df.merge(canon_subset, on=key_cols, how="left")

    if len(result) != n_target:
        issues.log(
            STAGE,
            "ROWCOUNT_MISMATCH",
            f"Propagação alterou contagem de linhas: {n_target} → {len(result)} "
            f"(ano {year}, arquivo {target_parquet.name}).",
            "LOGGED_ONLY",
            year=year,
        )
        log.warning("ROWCOUNT_MISMATCH no merge de biomassa: %s", target_parquet.name)

    n_missing = result[biomass_cols[0]].isna().sum() if biomass_cols else 0
    if n_missing > 0:
        issues.log(
            STAGE,
            "BIOMASS_KEY_MISSING",
            f"{n_missing} linhas sem match de biomassa no merge "
            f"(ano {year}, arquivo {target_parquet.name}).",
            "LOGGED_ONLY",
            year=year,
        )

    return result


# ---------------------------------------------------------------------------
# Orquestração por ano
# ---------------------------------------------------------------------------
def _discover_years(directory: Path) -> List[int]:
    years = []
    for p in sorted(directory.glob("inmet_bdq_*_cerrado.parquet")):
        m = re.search(r"inmet_bdq_(\d{4})_cerrado", p.stem)
        if m:
            years.append(int(m.group(1)))
    return years


def run_gee_pipeline(
    years: Optional[List[int]] = None,
    skip_years: Optional[List[int]] = None,
) -> None:
    """
    Orquestra: extração GEE semanal na canônica → ffill → propagação para D/E/F.

    skip_years: anos a ignorar (útil após já ter processado um ano pesado).
    """
    acfg = load_article_config()
    log = get_logger("article.gee_biomass", kind="article", per_run_file=True)
    issues = IssueLogger(ensure_dir(acfg.output_root / "logs") / "processing_issues.csv")

    log.info("=" * 72)
    log.info("ARTICLE PIPELINE — Etapa 1: GEE Biomassa (semanal → horária)")
    log.info("=" * 72)

    ee_mod = initialize_earth_engine(
        log,
        service_account_key_path=acfg.gee.service_account_key_path,
        project_id=acfg.gee.project_id,
        log_resource_name=f"Coleção biomassa: {acfg.gee.image_collection}",
    )
    if ee_mod is None:
        log.error(
            "Earth Engine não inicializado — defina credenciais (article_pipeline.gee ou "
            "inmet_gee_pipeline.gee, ou GEE_SERVICE_ACCOUNT_JSON / GEE_PROJECT). "
            "Biomassa não será extraída."
        )

    canonical = acfg.gee.canonical_scenario
    canonical_dir = acfg.output_root / "0_datasets_with_coords" / canonical
    if not canonical_dir.exists():
        log.error("Diretório canônico não existe: %s. Execute enrich_coords primeiro.", canonical_dir)
        return

    available = _discover_years(canonical_dir)
    if years:
        available = [y for y in available if y in years]
    if skip_years:
        sk = set(skip_years)
        available = [y for y in available if y not in sk]
        log.info("  Ignorando anos (skip_years): %s", sorted(sk))

    if not available:
        log.warning("Nenhum ano a processar após --years / --skip-years.")
        issues.flush()
        return

    bands = acfg.gee.bands
    biomass_cols = [f"{b}_buffer" for b in bands] + [f"{b}_point" for b in bands]

    other_scenarios = {k: v for k, v in acfg.scenarios.items() if v != canonical}

    for year in sorted(available):
        log.info("=== Ano %d ===", year)

        canon_path = canonical_dir / PARQUET_TEMPLATE.format(year=year)
        df_canon = pd.read_parquet(canon_path)
        df_canon = ensure_gee_site_key(df_canon)

        sites = df_canon.drop_duplicates(subset=["cidade_norm", SITE_KEY_COL])[
            [SITE_KEY_COL, "cidade_norm", "lat_foco", "lon_foco"]
        ]

        if ee_mod is None:
            weekly = None
        else:
            weekly = extract_weekly_biomass_gee(sites, year, acfg, log, ee_mod)

        if weekly is not None and len(weekly) > 0:
            df_canon = align_weekly_to_hourly(
                df_canon,
                weekly,
                biomass_cols,
                station_col="cidade_norm",
                site_key_col=SITE_KEY_COL,
            )
            df_canon.to_parquet(canon_path, index=False, engine="pyarrow")
            log.info("  Canônica atualizada com biomassa: %s", canon_path.name)

            for key, folder in other_scenarios.items():
                target_dir = acfg.output_root / "0_datasets_with_coords" / folder
                target_path = target_dir / PARQUET_TEMPLATE.format(year=year)
                if not target_path.exists():
                    log.warning("  Target não encontrado para cenário %s: %s", key, target_path)
                    continue

                result = propagate_biomass(
                    df_canon, target_path, biomass_cols, issues, log, year,
                )
                result.to_parquet(target_path, index=False, engine="pyarrow")
                log.info("  Propagado para %s: %s (%d linhas)", key, target_path.name, len(result))
                del result
                gc.collect()
        else:
            log.info(
                "  GEE sem dados de biomassa para ano %d — colunas não atualizadas.",
                year,
            )

        del df_canon
        gc.collect()

    issues.flush()
    log.info("Pipeline GEE finalizado.")


if __name__ == "__main__":
    run_gee_pipeline()
