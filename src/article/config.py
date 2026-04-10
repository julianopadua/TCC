# src/article/config.py
# =============================================================================
# Configuração do pipeline do artigo — carrega bloco `article_pipeline` do
# config.yaml e expõe dataclasses tipados + paths resolvidos.
# =============================================================================
from __future__ import annotations

import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List

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
    gee_cfg = GeeArticleConfig(
        canonical_scenario=gee_raw.get("canonical_scenario", "base_E_with_rad_knn_calculated"),
        image_collection=gee_raw.get("image_collection", "MODIS/061/MOD13Q1"),
        scale_m=int(gee_raw.get("scale_m", 250)),
        buffer_radius_km=float(gee_raw.get("buffer_radius_km", 50)),
        composite_period_days=int(gee_raw.get("composite_period_days", 16)),
        bands=gee_raw.get("bands", ["NDVI", "EVI"]),
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
