# src/article/config.py
# =============================================================================
# Configuração do pipeline do artigo — carrega bloco `article_pipeline` do
# config.yaml e expõe dataclasses tipados + paths resolvidos.
# =============================================================================
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils


@dataclass
class GeeArticleConfig:
    canonical_scenario: str
    image_collection: str
    scale_m: int
    buffer_radius_km: float
    composite_period_days: int
    bands: List[str]
    # Caminho absoluto do JSON da conta de serviço (vazio = tentar OAuth/ADC).
    service_account_key_path: str
    project_id: str
    sites_chunk_size: int
    tile_scale: int
    pause_between_chunks_s: float
    gee_retry_max_attempts: int


@dataclass
class EdaConfig:
    benchmark_cities: List[str]


@dataclass
class ArticlePipelineConfig:
    root_dir: Path
    output_root: Path
    modeling_dir: Path

    scenarios: Dict[str, str]
    years: List[int]
    station_id_columns: List[str]

    gee: GeeArticleConfig
    eda: EdaConfig

    station_locations_csv: Path
    bdq_processed_dir: Path

    modeling_scenarios: Dict[str, str]


def load_article_config() -> ArticlePipelineConfig:
    cfg = utils.loadConfig()
    raw = cfg.get("article_pipeline", {})
    if not raw:
        raise ValueError(
            "Bloco 'article_pipeline' ausente no config.yaml. "
            "Adicione as chaves conforme o template do pipeline do artigo."
        )

    root_dir = Path(cfg["paths"]["root"])
    modeling_dir = Path(cfg["paths"]["data"]["modeling"])
    output_root = Path(cfg["paths"]["data"]["article"])

    gee_raw = raw.get("gee", {})
    inmet_gee = cfg.get("inmet_gee_pipeline", {}).get("gee", {})

    project_id = (os.getenv("GEE_PROJECT") or gee_raw.get("project_id") or "").strip()
    if not project_id:
        project_id = (inmet_gee.get("project_id") or "").strip()

    sa_key_raw = (
        (os.getenv("GEE_SERVICE_ACCOUNT_JSON") or "").strip()
        or (gee_raw.get("service_account_key_path") or "").strip()
        or (inmet_gee.get("service_account_key_path") or "").strip()
    )
    sa_resolved = ""
    if sa_key_raw:
        p = Path(sa_key_raw)
        if not p.is_absolute():
            p = (root_dir / p).resolve()
        else:
            p = p.resolve()
        if p.is_file():
            sa_resolved = str(p)

    gee_cfg = GeeArticleConfig(
        canonical_scenario=gee_raw.get("canonical_scenario", "base_E_with_rad_knn_calculated"),
        image_collection=gee_raw.get("image_collection", "MODIS/061/MOD13Q1"),
        scale_m=int(gee_raw.get("scale_m", 250)),
        buffer_radius_km=float(gee_raw.get("buffer_radius_km", 50)),
        composite_period_days=int(gee_raw.get("composite_period_days", 16)),
        bands=gee_raw.get("bands", ["NDVI", "EVI"]),
        service_account_key_path=sa_resolved,
        project_id=project_id,
        sites_chunk_size=int(gee_raw.get("sites_chunk_size", 400)),
        tile_scale=int(gee_raw.get("tile_scale", 2)),
        pause_between_chunks_s=float(gee_raw.get("pause_between_chunks_s", 0.5)),
        gee_retry_max_attempts=int(gee_raw.get("gee_retry_max_attempts", 3)),
    )

    eda_raw = raw.get("eda", {})
    benchmark = eda_raw.get("benchmark_cities", [])
    if not benchmark:
        ts_raw = cfg.get("inmet_gee_pipeline", {}).get("timeseries", {})
        benchmark = ts_raw.get("sample_cities", [])
    eda_cfg = EdaConfig(benchmark_cities=benchmark)

    inmet_gee_dir = Path(cfg["paths"]["data"]["integrations_inmet_gee"])
    station_locations_csv = inmet_gee_dir / "outputs" / "csv" / "station_year_locations.csv"
    bdq_processed_dir = Path(cfg["paths"]["data"]["processed"]) / "ID_BDQUEIMADAS"

    return ArticlePipelineConfig(
        root_dir=root_dir,
        output_root=output_root,
        modeling_dir=modeling_dir,
        scenarios=raw.get("scenarios", {}),
        years=[int(y) for y in raw.get("years", [])],
        station_id_columns=raw.get("station_id_columns", ["cidade_norm"]),
        gee=gee_cfg,
        eda=eda_cfg,
        station_locations_csv=station_locations_csv,
        bdq_processed_dir=bdq_processed_dir,
        modeling_scenarios=cfg.get("modeling_scenarios", {}),
    )


def biomass_modeling_columns_for_schema(
    cfg: Dict[str, Any], schema_names: Iterable[str]
) -> List[str]:
    """Colunas NDVI/EVI a usar no train_runner --article, conforme modeling_biomass_mode.

    Apenas nomes presentes em ``schema_names`` entram na lista (ordem estável).
    Modos: ``buffers`` | ``points`` | ``all_four`` (default: buffers).
    """
    from src.article.temporal_fusion_article import (
        COL_EVI_BUFFER,
        COL_EVI_POINT,
        COL_NDVI_BUFFER,
        COL_NDVI_POINT,
    )

    ap = cfg.get("article_pipeline") or {}
    mode = str(ap.get("modeling_biomass_mode", "buffers")).strip().lower()
    names = set(schema_names)

    if mode == "points":
        candidates = [COL_NDVI_POINT, COL_EVI_POINT]
    elif mode == "all_four":
        candidates = [COL_NDVI_BUFFER, COL_EVI_BUFFER, COL_NDVI_POINT, COL_EVI_POINT]
    elif mode == "buffers":
        candidates = [COL_NDVI_BUFFER, COL_EVI_BUFFER]
    else:
        candidates = [COL_NDVI_BUFFER, COL_EVI_BUFFER]

    out: List[str] = [c for c in candidates if c in names]
    return out
