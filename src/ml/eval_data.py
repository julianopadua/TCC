from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

import pandas as pd

from src.train_runner import (
    TrainingOrchestrator,
    _article_temporal_test_size_years,
)


@dataclass
class ScenarioEvalData:
    scenario_key: str
    scenario_folder: str
    parquet_source: str
    X_test: pd.DataFrame
    y_test: pd.Series
    X_train: pd.DataFrame
    y_train: pd.Series
    valid_features: list[str]
    data_audit: Dict[str, Any]


def load_scenario_eval_data(
    scenario_key: str,
    *,
    use_article_data: bool,
    batch_rows: Optional[int] = None,
    max_train_rows: Optional[int] = None,
    max_test_rows: Optional[int] = None,
) -> ScenarioEvalData:
    """Reproduz o mesmo split temporal do treino para reavaliar modelos salvos."""
    orch = TrainingOrchestrator(scenario_key, use_article_data=use_article_data)
    split = orch.prepare_eval_split_data(
        test_size_years=_article_temporal_test_size_years(orch.cfg),
        gap_years=0,
        max_train_rows=max_train_rows,
        max_test_rows=max_test_rows,
        neg_pos_ratio=200,
        min_neg_keep_per_chunk=50_000,
        batch_rows=batch_rows,
    )
    return ScenarioEvalData(
        scenario_key=orch.scenario_key,
        scenario_folder=orch.scenario_folder,
        parquet_source=orch._parquet_source,
        X_test=split.X_test,
        y_test=split.y_test,
        X_train=split.X_train,
        y_train=split.y_train,
        valid_features=split.valid_features,
        data_audit=split.data_audit,
    )
