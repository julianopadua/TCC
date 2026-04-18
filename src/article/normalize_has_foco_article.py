# src/article/normalize_has_foco_article.py
# =============================================================================
# Normaliza a coluna HAS_FOCO para int8 {0,1} em todos os parquets sob
# data/_article/0_datasets_with_coords/ e 1_datasets_with_fusion/ (recursivo),
# para consistência entre bases antes do treino / EDA.
#
#   python -m src.article.normalize_has_foco_article
#   python -m src.article.normalize_has_foco_article --dry-run
#   python -m src.article.normalize_has_foco_article --scenario base_E_with_rad_knn_calculated
# =============================================================================
from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils  # noqa: E402

LABEL = "HAS_FOCO"

_ARTICLE_SUBDIRS = (
    "0_datasets_with_coords",
    "1_datasets_with_fusion",
)


def normalize_has_foco_series(s: pd.Series) -> pd.Series:
    """Converte HAS_FOCO para int8 com valores apenas 0 ou 1."""
    if pd.api.types.is_bool_dtype(s):
        return s.astype("int8")
    if str(s.dtype) == "boolean":
        return s.fillna(False).astype("int8")
    if pd.api.types.is_float_dtype(s):
        return (s.fillna(0.0) >= 0.5).astype("int8")
    if pd.api.types.is_integer_dtype(s):
        return s.clip(0, 1).astype("int8")
    num = pd.to_numeric(s, errors="coerce").fillna(0.0)
    return (num >= 0.5).astype("int8")


def _needs_write(before: pd.Series, after: pd.Series) -> bool:
    if before.dtype != after.dtype:
        return True
    try:
        return not np.array_equal(before.to_numpy(), after.to_numpy())
    except Exception:
        return True


def _iter_parquet_paths(article_root: Path, scenario: Optional[str]) -> List[Path]:
    out: List[Path] = []
    for sub in _ARTICLE_SUBDIRS:
        root = article_root / sub
        if not root.is_dir():
            continue
        for p in sorted(root.rglob("*.parquet")):
            if _results_or_noise(p):
                continue
            if scenario is not None and scenario not in p.parts:
                continue
            out.append(p)
    return out


def _results_or_noise(p: Path) -> bool:
    parts = set(p.parts)
    return "_results" in parts or "logs" in parts


def _has_column(path: Path, col: str) -> bool:
    try:
        import pyarrow.parquet as pq

        return col in pq.read_schema(str(path)).names
    except Exception:
        return False


def run(
    *,
    dry_run: bool = False,
    scenario: Optional[str] = None,
    log=None,
) -> Tuple[int, int, int, int]:
    """
    Returns (n_changed, n_noop, n_skip_no_col, n_err).
    """
    if log is None:
        log = utils.get_logger("article.has_foco", kind="article", per_run_file=True)

    cfg = utils.loadConfig()
    article_root = Path(cfg["paths"]["data"]["article"]).resolve()
    paths = _iter_parquet_paths(article_root, scenario)

    log.info(
        f"[normalize_has_foco] article_root={article_root} | "
        f"dry_run={dry_run} | scenario={scenario!r} | parquets={len(paths)}"
    )

    n_changed = 0
    n_noop = 0
    n_skip = 0
    n_err = 0

    for path in paths:
        try:
            if not _has_column(path, LABEL):
                n_skip += 1
                continue
            before = pd.read_parquet(path, columns=[LABEL])[LABEL]
            after = normalize_has_foco_series(before)
            if not _needs_write(before, after):
                n_noop += 1
                continue
            if dry_run:
                log.info(
                    f"[DRY-RUN] {path}: dtype {before.dtype} -> int8 | "
                    f"n={len(after)} positivos={int(after.sum())}"
                )
                n_changed += 1
                continue

            df = pd.read_parquet(path)
            df[LABEL] = normalize_has_foco_series(df[LABEL])
            tmp = path.with_suffix(".parquet.tmp")
            try:
                df.to_parquet(tmp, index=False)
                tmp.replace(path)
            except Exception:
                if tmp.exists():
                    tmp.unlink(missing_ok=True)
                raise
            rel = path
            try:
                rel = path.relative_to(article_root)
            except ValueError:
                pass
            log.info(
                f"[OK] {rel}: HAS_FOCO int8 | n={len(df)} positivos={int(df[LABEL].sum())}"
            )
            n_changed += 1
            del df
            gc.collect()
        except Exception as exc:
            n_err += 1
            log.error(f"[ERRO] {path}: {exc}", exc_info=True)

    log.info(
        f"[normalize_has_foco] resumo: gravados={n_changed} ja_int8={n_noop} "
        f"sem_{LABEL}={n_skip} erros={n_err} | dry_run={dry_run}"
    )
    if dry_run:
        log.info(
            f"[normalize_has_foco] SUCESSO (dry-run): {n_changed} ficheiro(s) precisam atualizacao."
        )
    elif n_err == 0:
        log.info(
            "[normalize_has_foco] SUCESSO: HAS_FOCO normalizado para int8 {{0,1}} onde necessario."
        )
    return n_changed, n_noop, n_skip, n_err


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Normaliza HAS_FOCO para int8 {0,1} nos parquets de _article."
    )
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="So reporta ficheiros que seriam alterados, sem gravar.",
    )
    ap.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="Restringe a caminhos que contem este segmento (ex.: base_E_with_rad_knn_calculated).",
    )
    args = ap.parse_args(argv)

    log = utils.get_logger("article.has_foco", kind="article", per_run_file=True)
    _, _, _, n_err = run(dry_run=args.dry_run, scenario=args.scenario, log=log)
    return 1 if n_err else 0


if __name__ == "__main__":
    raise SystemExit(main())
