# src/article/pipeline_state.py
# =============================================================================
# Manifestos JSON para retomar pipeline sem repetir coords / GEE já concluídos.
# =============================================================================
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

GEE_MANIFEST_NAME = "gee_biomass_completed.json"
COORDS_MANIFEST_NAME = "enrich_coords_completed.json"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


# ---------------------------------------------------------------------------
# GEE
# ---------------------------------------------------------------------------
def load_gee_manifest(path: Path) -> Set[int]:
    if not path.exists():
        return set()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        years = raw.get("years_completed", [])
        return {int(y) for y in years}
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        return set()


def save_gee_manifest(path: Path, years_completed: Set[int]) -> None:
    data = {
        "version": 1,
        "years_completed": sorted(years_completed),
        "last_updated": _utc_now_iso(),
    }
    _write_json(path, data)


def gee_should_process_year(
    year: int,
    manifest: Set[int],
    overwrite: bool,
    years_arg: Optional[List[int]],
) -> bool:
    """Ano novo (fora do manifest) sempre processa; completos só com --overwrite."""
    if year not in manifest:
        return True
    if not overwrite:
        return False
    if years_arg is not None:
        return year in years_arg
    return True


def gee_mark_year_complete(path: Path, year: int) -> None:
    m = load_gee_manifest(path)
    m.add(year)
    save_gee_manifest(path, m)


# ---------------------------------------------------------------------------
# Coords (por chave de cenário: D, E, F)
# ---------------------------------------------------------------------------
def load_coords_manifest(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"version": 1, "completed": {}, "last_updated": None}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        completed = raw.get("completed") or {}
        # normalizar para listas de int
        out: Dict[str, List[int]] = {}
        for k, v in completed.items():
            if isinstance(v, list):
                out[str(k)] = sorted({int(x) for x in v})
        return {
            "version": raw.get("version", 1),
            "completed": out,
            "last_updated": raw.get("last_updated"),
        }
    except (json.JSONDecodeError, OSError, TypeError, ValueError):
        return {"version": 1, "completed": {}, "last_updated": None}


def save_coords_manifest(path: Path, state: Dict[str, Any]) -> None:
    state = dict(state)
    state["last_updated"] = _utc_now_iso()
    completed = state.get("completed") or {}
    state["completed"] = {k: sorted(set(int(x) for x in v)) for k, v in completed.items()}
    _write_json(path, state)


def coords_should_process_year(
    year: int,
    scenario_key: str,
    state: Dict[str, Any],
    overwrite: bool,
) -> bool:
    done = set(state.get("completed", {}).get(scenario_key, []) or [])
    if year not in done:
        return True
    return overwrite


def coords_mark_year_complete(
    state: Dict[str, Any],
    scenario_key: str,
    year: int,
    path: Path,
) -> None:
    comp = state.setdefault("completed", {})
    lst = list(comp.get(scenario_key, []))
    if year not in lst:
        lst.append(year)
    comp[scenario_key] = sorted(lst)
    save_coords_manifest(path, state)
