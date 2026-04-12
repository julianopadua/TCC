# src/integrations/inmet_gee/checkpoint.py
# =============================================================================
# CHECKPOINTS — Load/save atômico de estado JSON
# Schema versão: 1
# =============================================================================
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

SCHEMA_VERSION = 1
_BACKUP_SUFFIX = ".bak"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _atomic_write(path: Path, data: dict) -> None:
    """Escreve JSON em arquivo temporário e renomeia atomicamente."""
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    # Salva backup antes de substituir
    if path.exists():
        path.replace(path.with_suffix(_BACKUP_SUFFIX))
    os.replace(tmp, path)


def load_state(path: Path) -> dict:
    """
    Carrega estado do checkpoint. Retorna dict vazio se o arquivo não existir.
    Tenta o .bak em caso de arquivo corrompido.
    """
    for candidate in [path, path.with_suffix(_BACKUP_SUFFIX)]:
        if candidate.exists():
            try:
                data = json.loads(candidate.read_text(encoding="utf-8"))
                return data
            except (json.JSONDecodeError, OSError):
                continue
    return {}


def init_state(
    input_scenario: str,
    input_root: str,
) -> dict:
    """Cria um estado inicial de pipeline."""
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": str(uuid.uuid4()),
        "started_at": _utc_now(),
        "updated_at": _utc_now(),
        "input_scenario": input_scenario,
        "input_root": input_root,
        "completed_years": [],
        "current_year": None,
        "failed_years": {},
        "gee": {"completed_keys": []},
        "file_fingerprints": {},
        "drift_state": {},
        "drift_events": [],
        "earthengine_api_version": _get_ee_version(),
    }


def _get_ee_version() -> str:
    try:
        import ee
        return getattr(ee, "__version__", "unknown")
    except ImportError:
        return "not_installed"


def mark_year_started(state: dict, year: int, file_path: Optional[Path] = None) -> dict:
    state = dict(state)
    state["current_year"] = year
    state["updated_at"] = _utc_now()
    if file_path and file_path.exists():
        stat = file_path.stat()
        state.setdefault("file_fingerprints", {})[str(year)] = {
            "size": stat.st_size,
            "mtime": stat.st_mtime,
        }
    return state


def mark_year_done(state: dict, year: int) -> dict:
    state = dict(state)
    completed = sorted(set(state.get("completed_years", []) + [year]))
    state["completed_years"] = completed
    state["current_year"] = None
    state["updated_at"] = _utc_now()
    # Remove da lista de falhas se foi reprocessado com sucesso
    state.get("failed_years", {}).pop(str(year), None)
    return state


def mark_year_failed(state: dict, year: int, error_class: str, message: str) -> dict:
    state = dict(state)
    state["current_year"] = None
    state["updated_at"] = _utc_now()
    state.setdefault("failed_years", {})[str(year)] = {
        "error_class": error_class,
        "message": str(message)[:500],
        "at": _utc_now(),
    }
    return state


def mark_gee_key_done(state: dict, key: str) -> dict:
    state = dict(state)
    gee = dict(state.get("gee", {}))
    keys = list(set(gee.get("completed_keys", []) + [key]))
    gee["completed_keys"] = keys
    state["gee"] = gee
    state["updated_at"] = _utc_now()
    return state


def save_state(state: dict, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    state = dict(state)
    state["updated_at"] = _utc_now()
    _atomic_write(path, state)


# ---------------------------------------------------------------------------
# Estado de séries temporais (arquivo paralelo)
# ---------------------------------------------------------------------------

def load_ts_state(path: Path) -> dict:
    return load_state(path)


def init_ts_state() -> dict:
    return {
        "schema_version": SCHEMA_VERSION,
        "started_at": _utc_now(),
        "updated_at": _utc_now(),
        "completed_years_ts": [],
        "last_plot_year": None,
        "failed_years_ts": {},
        "sample_cities": [],
        "file_fingerprints": {},
        "cumsum_carry": {},   # estado de acumulado multi-anual por cidade
    }


def mark_ts_year_done(state: dict, year: int) -> dict:
    state = dict(state)
    done = sorted(set(state.get("completed_years_ts", []) + [year]))
    state["completed_years_ts"] = done
    state["last_plot_year"] = year
    state["updated_at"] = _utc_now()
    state.get("failed_years_ts", {}).pop(str(year), None)
    return state


def mark_ts_year_failed(state: dict, year: int, error_class: str, message: str) -> dict:
    state = dict(state)
    state["updated_at"] = _utc_now()
    state.setdefault("failed_years_ts", {})[str(year)] = {
        "error_class": error_class,
        "message": str(message)[:500],
        "at": _utc_now(),
    }
    return state


def save_ts_state(state: dict, path: Path) -> None:
    save_state(state, path)
