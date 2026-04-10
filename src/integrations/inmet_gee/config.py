# src/integrations/inmet_gee/config.py
# =============================================================================
# CONFIGURAÇÃO — Pipeline INMET-GEE
# Carrega e valida o bloco `inmet_gee_pipeline` do config.yaml do projeto.
# =============================================================================
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional

import sys
current_file = Path(__file__).resolve()
project_root = current_file.parents[3]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import src.utils as utils


@dataclass
class GeeConfig:
    project_id: str
    reference_image_collection: str
    scale_m: int
    roi_mode: str
    buffer_radius_km: float
    calls_per_minute_cap: int
    validation_strategy: str


@dataclass
class TimeseriesConfig:
    enabled: bool
    scenarios: Dict[str, str]
    variables: List[str]
    sample_cities: List[str]
    auto_sample_n: int
    reference_year_for_city_pick: str
    cumsum_precip: bool
    cumsum_multiyear: bool
    downsample_for_global_plot: str
    export_format: str
    export_sample_csv: bool


@dataclass
class SpatialInterpolationConfig:
    roi_size_km: float
    resolution_m: int


@dataclass
class PipelineConfig:
    # Caminhos resolvidos
    root_dir: Path
    output_dir: Path
    modeling_dir: Path

    # Entrada
    station_source_scenario: str
    station_id_columns: List[str]
    years: List[int]

    # Deriva espacial
    coordinate_jitter_max_m: float
    drift_alert_min_m: float

    # Retry
    retry_max_attempts: int
    retry_base_delay_s: float
    retry_max_delay_s: float

    # GEE
    gee: GeeConfig

    # Séries temporais
    timeseries: TimeseriesConfig

    # Cores de gráfico
    plot_color_F: str
    plot_color_E: str

    # Phase-2 hooks
    station_influence_radius_km: float
    spatial_interpolation: SpatialInterpolationConfig

    # Cenários mapeados (nome chave -> pasta física)
    modeling_scenarios: Dict[str, str]


def load_pipeline_config() -> PipelineConfig:
    """
    Carrega o config.yaml do projeto e retorna um PipelineConfig tipado para
    o pipeline INMET-GEE. Levanta ValueError com mensagem descritiva se
    parâmetros obrigatórios estiverem ausentes ou inválidos.
    """
    cfg = utils.loadConfig()
    raw = cfg.get("inmet_gee_pipeline", {})
    if not raw:
        raise ValueError(
            "CRITICAL: bloco 'inmet_gee_pipeline' ausente no config.yaml. "
            "Adicione as chaves de configuração conforme o template do pipeline."
        )

    modeling_dir = Path(cfg["paths"]["data"]["modeling"])
    output_dir = Path(cfg["paths"]["data"]["integrations_inmet_gee"])
    root_dir = Path(cfg["paths"]["root"])
    modeling_scenarios: Dict[str, str] = cfg.get("modeling_scenarios", {})

    gee_raw = raw.get("gee", {})
    # project_id pode vir da variável de ambiente GEE_PROJECT
    project_id = os.getenv("GEE_PROJECT", gee_raw.get("project_id", ""))
    gee_cfg = GeeConfig(
        project_id=project_id,
        reference_image_collection=gee_raw.get(
            "reference_image_collection", "COPERNICUS/S2_SR_HARMONIZED"
        ),
        scale_m=int(gee_raw.get("scale_m", 10)),
        roi_mode=gee_raw.get("roi_mode", "point"),
        buffer_radius_km=float(gee_raw.get("buffer_radius_km", 5.0)),
        calls_per_minute_cap=int(gee_raw.get("calls_per_minute_cap", 20)),
        validation_strategy=gee_raw.get("validation_strategy", "per_geo_version"),
    )

    ts_raw = raw.get("timeseries", {})
    ts_cfg = TimeseriesConfig(
        enabled=bool(ts_raw.get("enabled", True)),
        scenarios=ts_raw.get("scenarios", {"E": "base_E_calculated", "F": "base_F_calculated"}),
        variables=ts_raw.get("variables", []),
        sample_cities=ts_raw.get("sample_cities", []),
        auto_sample_n=int(ts_raw.get("auto_sample_n", 5)),
        reference_year_for_city_pick=str(ts_raw.get("reference_year_for_city_pick", "last")),
        cumsum_precip=bool(ts_raw.get("cumsum_precip", True)),
        cumsum_multiyear=bool(ts_raw.get("cumsum_multiyear", False)),
        downsample_for_global_plot=ts_raw.get("downsample_for_global_plot", "daily_mean"),
        export_format=ts_raw.get("export_format", "parquet"),
        export_sample_csv=bool(ts_raw.get("export_sample_csv", True)),
    )

    si_raw = raw.get("spatial_interpolation", {})
    si_cfg = SpatialInterpolationConfig(
        roi_size_km=float(si_raw.get("roi_size_km", 10.0)),
        resolution_m=int(si_raw.get("resolution_m", 1000)),
    )

    return PipelineConfig(
        root_dir=root_dir,
        output_dir=output_dir,
        modeling_dir=modeling_dir,
        station_source_scenario=raw.get("station_source_scenario", "base_E_calculated"),
        station_id_columns=raw.get("station_id_columns", ["cidade_norm"]),
        years=[int(y) for y in raw.get("years", [])],
        coordinate_jitter_max_m=float(raw.get("coordinate_jitter_max_m", 50)),
        drift_alert_min_m=float(raw.get("drift_alert_min_m", 500)),
        retry_max_attempts=int(raw.get("retry_max_attempts", 3)),
        retry_base_delay_s=float(raw.get("retry_base_delay_s", 5)),
        retry_max_delay_s=float(raw.get("retry_max_delay_s", 120)),
        gee=gee_cfg,
        timeseries=ts_cfg,
        plot_color_F=raw.get("plot_color_F", "#1f77b4"),
        plot_color_E=raw.get("plot_color_E", "#d62728"),
        station_influence_radius_km=float(raw.get("station_influence_radius_km", 50.0)),
        spatial_interpolation=si_cfg,
        modeling_scenarios=modeling_scenarios,
    )
