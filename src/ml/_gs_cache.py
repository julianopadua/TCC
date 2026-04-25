"""Cache persistente em disco para GridSearchCV.

Motivacao: variacoes 3 (smote+grid) e 4 (weight+grid) do mesmo (model,
scenario) repetiam o mesmo GridSearch — operacao mais cara do pipeline
(dezenas de fits). O cache em memoria existente em RandomForestTrainer e
perdido entre processos. Aqui guardamos best_params em JSON sob
data/_article/_caches/gridsearch/, com chave que inclui assinatura do grid
para invalidar cache automaticamente quando o param_grid muda.

Estrutura em disco:
    data/_article/_caches/gridsearch/
        {model}__{scenario}__{grid_mode}__{grid_hash}.json

JSON contem:
    {
      "model": "...",
      "scenario": "...",
      "grid_mode": "...",
      "grid_hash": "...",
      "param_grid": {...},
      "best_params": {...},
      "best_score": float,
      "scoring": "...",
      "saved_at": "ISO-8601",
      "source_run": "...",   # opcional, para debug
    }
"""
from __future__ import annotations

import hashlib
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))


def _grid_signature(grid: Dict[str, Any]) -> str:
    """Hash deterministico do param_grid (orderable repr)."""
    payload = json.dumps(grid, sort_keys=True, default=str)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]


def _safe_token(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(s or "default"))


def _cache_root() -> Path:
    """Resolve raiz do cache. Preferencia para config.yaml; fallback fixo."""
    try:
        from src.utils import loadConfig
        cfg = loadConfig()
        article_root = Path(cfg["paths"]["data"].get("article")
                            or (_project_root / "data" / "_article"))
    except Exception:
        article_root = _project_root / "data" / "_article"
    p = article_root / "_caches" / "gridsearch"
    p.mkdir(parents=True, exist_ok=True)
    return p


def cache_path(model: str, scenario: str, grid_mode: str, grid: Dict[str, Any]) -> Path:
    h = _grid_signature(grid)
    fname = f"{_safe_token(model)}__{_safe_token(scenario)}__{_safe_token(grid_mode)}__{h}.json"
    return _cache_root() / fname


def load_best_params(
    model: str,
    scenario: str,
    grid_mode: str,
    grid: Dict[str, Any],
    log=None,
) -> Optional[Dict[str, Any]]:
    """Le best_params do disco; retorna None se cache nao existir/quebrado."""
    p = cache_path(model, scenario, grid_mode, grid)
    if not p.is_file():
        return None
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
        bp = data.get("best_params")
        if not isinstance(bp, dict):
            return None
        if log is not None:
            log.info(
                f"[GS-CACHE] HIT disco: {p.name} "
                f"| best_score={data.get('best_score')} | best_params={bp}"
            )
        return bp
    except Exception as e:
        if log is not None:
            log.warning(f"[GS-CACHE] cache corrompido em {p.name}: {e}; ignorando")
        return None


def save_best_params(
    model: str,
    scenario: str,
    grid_mode: str,
    grid: Dict[str, Any],
    best_params: Dict[str, Any],
    *,
    best_score: Optional[float] = None,
    scoring: Optional[str] = None,
    source_run: Optional[str] = None,
    log=None,
) -> Path:
    p = cache_path(model, scenario, grid_mode, grid)
    payload = {
        "model": model,
        "scenario": scenario,
        "grid_mode": grid_mode,
        "grid_hash": _grid_signature(grid),
        "param_grid": grid,
        "best_params": best_params,
        "best_score": best_score,
        "scoring": scoring,
        "saved_at": datetime.now().isoformat(timespec="seconds"),
        "source_run": source_run,
    }
    p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    if log is not None:
        log.info(f"[GS-CACHE] SAVE disco: {p.name}")
    return p
