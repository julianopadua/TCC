# src/article/viz/config_paths.py
"""Resolve caminhos dos Parquets do artigo a partir de ArticlePipelineConfig."""
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List

from config import PARQUET_TEMPLATE

from src.article.config import ArticlePipelineConfig, load_article_config
_YEAR_RE = re.compile(r"inmet_bdq_(\d{4})_cerrado\.parquet$")


def get_config() -> ArticlePipelineConfig:
    return load_article_config()


def scenario_dirs(cfg: ArticlePipelineConfig) -> Dict[str, str]:
    """Chave de cenário (D/E/F) → nome da pasta em 0_datasets_with_coords."""
    return dict(cfg.scenarios)


def datasets_root(cfg: ArticlePipelineConfig) -> Path:
    return cfg.output_root / "0_datasets_with_coords"


def parquet_path(cfg: ArticlePipelineConfig, scenario_key: str, year: int) -> Path:
    folder = cfg.scenarios[scenario_key]
    return datasets_root(cfg) / folder / PARQUET_TEMPLATE.format(year=year)


def discover_years_for_scenario(cfg: ArticlePipelineConfig, scenario_key: str) -> List[int]:
    folder = cfg.scenarios[scenario_key]
    d = datasets_root(cfg) / folder
    if not d.is_dir():
        return []
    years: List[int] = []
    for p in sorted(d.glob("inmet_bdq_*_cerrado.parquet")):
        m = _YEAR_RE.search(p.name)
        if m:
            years.append(int(m.group(1)))
    return years


def list_scenario_keys(cfg: ArticlePipelineConfig) -> List[str]:
    return sorted(cfg.scenarios.keys())
