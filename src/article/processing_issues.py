# src/article/processing_issues.py
# =============================================================================
# Rastreador de anomalias — processing_issues.csv
# Append atômico seguindo o padrão de csv_writers.py do pipeline INMET-GEE.
# =============================================================================
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

import pandas as pd

ISSUES_COLS = [
    "timestamp",
    "module_stage",
    "station_uid",
    "foco_id",
    "year",
    "issue_type",
    "description",
    "action_taken",
]


def _atomic_csv_append(
    path: Path,
    new_rows: pd.DataFrame,
    expected_cols: List[str],
) -> None:
    """Append incremental com escrita atômica (.tmp → .bak → replace)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        existing = pd.read_csv(path, encoding="utf-8", dtype=str)
    else:
        existing = pd.DataFrame(columns=expected_cols)

    combined = pd.concat([existing, new_rows.astype(str)], ignore_index=True)
    extra = [c for c in combined.columns if c not in expected_cols]
    ordered = [c for c in expected_cols if c in combined.columns] + extra
    combined = combined[ordered]

    tmp = path.with_suffix(".tmp.csv")
    combined.to_csv(tmp, index=False, encoding="utf-8")
    if path.exists():
        bak = path.with_suffix(".bak.csv")
        path.replace(bak)
    os.replace(tmp, path)


class IssueLogger:
    """Acumula issues em memória e faz flush atômico no CSV."""

    def __init__(self, csv_path: Path) -> None:
        self._path = csv_path
        self._buffer: List[dict] = []

    def log(
        self,
        module_stage: str,
        issue_type: str,
        description: str,
        action_taken: str,
        *,
        station_uid: str = "",
        foco_id: str = "",
        year: Optional[int] = None,
    ) -> None:
        self._buffer.append({
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "module_stage": module_stage,
            "station_uid": station_uid,
            "foco_id": foco_id,
            "year": str(year) if year is not None else "",
            "issue_type": issue_type,
            "description": description,
            "action_taken": action_taken,
        })

    def flush(self) -> int:
        """Escreve buffer pendente no CSV e retorna a quantidade de issues gravadas."""
        if not self._buffer:
            return 0
        df = pd.DataFrame(self._buffer)
        _atomic_csv_append(self._path, df, ISSUES_COLS)
        n = len(self._buffer)
        self._buffer.clear()
        return n

    @property
    def pending(self) -> int:
        return len(self._buffer)
