"""Remove duplicatas nos parquets de modeling/base_* e de coords.

Estagios:
    modeling  (default)
        data/modeling/base_* — **mesma regra que coords**:
        drop_duplicates(subset=['cidade_norm','ts_hour'], keep='first').
        Opcional --full-row para o antigo modo (linhas bit-identicas em todas as colunas).

    coords
        data/_article/0_datasets_with_coords/* — chave (cidade_norm, ts_hour).

Uso:
    python -m src.dedupe_base_datasets                       # dry-run modeling (por chave)
    python -m src.dedupe_base_datasets --apply
    python -m src.dedupe_base_datasets --apply --full-row       # modo legado: somente linhas identicas

    python -m src.dedupe_base_datasets --stage coords --apply
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd

_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.utils import get_logger, loadConfig

# Bases pre-calculated (fonte) — as _calculated sao regeneradas depois
DEFAULT_MODELING_BASES = (
    "base_A_no_rad",
    "base_B_no_rad_knn",
    "base_C_no_rad_drop_rows",
    "base_D_with_rad_drop_rows",
    "base_E_with_rad_knn",
    "base_F_full_original",
)

# Bases presentes em 0_datasets_with_coords
DEFAULT_COORDS_BASES = (
    "base_D_with_rad_drop_rows_calculated",
    "base_E_with_rad_knn_calculated",
    "base_F_full_original_calculated",
)

KEY_COLS = ["cidade_norm", "ts_hour"]


def _dedupe_parquet_full(path: Path, log, apply: bool) -> dict:
    """Drop bit-identical rows (modeling stage)."""
    df = pd.read_parquet(path)
    before = len(df)
    df2 = df.drop_duplicates()
    after = len(df2)
    removed = before - after
    ratio = before / after if after else 0.0
    log.info(f"  {path.name:40s} {before:>9d} -> {after:>9d} ({ratio:.4f}x, -{removed})")
    if apply and removed > 0:
        tmp = path.with_suffix(path.suffix + ".dedup.tmp")
        df2.to_parquet(tmp, index=False)
        shutil.move(str(tmp), str(path))
    return {"file": str(path), "before": before, "after": after, "removed": removed}


def _dedupe_parquet_keys(path: Path, log, apply: bool) -> dict:
    """Drop duplicate (cidade_norm, ts_hour) keys, keep='first'."""
    df = pd.read_parquet(path)
    before = len(df)
    miss = [c for c in KEY_COLS if c not in df.columns]
    if miss:
        log.error(f"  {path.name}: colunas ausentes {miss} — skip")
        return {"file": str(path), "before": before, "after": before, "removed": 0}
    df2 = df.drop_duplicates(subset=KEY_COLS, keep="first")
    after = len(df2)
    removed = before - after
    ratio = before / after if after else 0.0
    log.info(f"  {path.name:40s} {before:>9d} -> {after:>9d} ({ratio:.4f}x, -{removed})")
    if apply and removed > 0:
        tmp = path.with_suffix(path.suffix + ".dedup.tmp")
        df2.to_parquet(tmp, index=False)
        shutil.move(str(tmp), str(path))
    return {"file": str(path), "before": before, "after": after, "removed": removed}


def _run_stage(
    stage_name: str,
    root: Path,
    bases: List[str],
    apply: bool,
    year: Optional[int],
    dedupe_fn,
    log,
) -> None:
    log.info(f"root = {root}")
    log.info(f"mode = {'APPLY (sobrescreve)' if apply else 'DRY-RUN (apenas reporta)'}")
    log.info(f"bases = {bases}")

    totals = {"files": 0, "before": 0, "after": 0, "removed": 0}
    for base in bases:
        bdir = root / base
        if not bdir.is_dir():
            log.warning(f"[skip] {base} nao encontrada em {bdir}")
            continue
        pattern = f"inmet_bdq_{year}_cerrado.parquet" if year else "inmet_bdq_*_cerrado.parquet"
        files = sorted(bdir.glob(pattern))
        if not files:
            log.warning(f"[skip] {base}: nenhum parquet casa com {pattern}")
            continue
        log.info(f"[{base}] {len(files)} parquet(s)")
        for f in files:
            r = dedupe_fn(f, log, apply)
            totals["files"] += 1
            totals["before"] += r["before"]
            totals["after"] += r["after"]
            totals["removed"] += r["removed"]

    log.info("=" * 60)
    log.info(f"TOTAL [{stage_name}]: {totals['files']} arquivos")
    log.info(f"  linhas antes:         {totals['before']:>12d}")
    log.info(f"  linhas depois:        {totals['after']:>12d}")
    log.info(f"  duplicatas removidas: {totals['removed']:>12d}")
    if not apply:
        log.info("Dry-run. Rode novamente com --apply para escrever.")


def run_modeling(
    bases: List[str],
    apply: bool,
    year: Optional[int] = None,
    use_full_row: bool = False,
) -> None:
    cfg = loadConfig()
    log = get_logger("dedupe.modeling")
    root = Path(cfg["paths"]["data"]["modeling"])
    fn = _dedupe_parquet_full if use_full_row else _dedupe_parquet_keys
    tag = "full_row" if use_full_row else "key_subset"
    log.info(f"modeling dedupe mode = {tag} {KEY_COLS}")
    _run_stage("modeling", root, bases, apply, year, fn, log)


def run_coords(bases: List[str], apply: bool, year: Optional[int] = None) -> None:
    cfg = loadConfig()
    log = get_logger("dedupe.coords")
    article_root = Path(cfg["paths"]["data"].get("article") or
                        (_project_root / "data" / "_article"))
    root = article_root / "0_datasets_with_coords"
    _run_stage("coords", root, bases, apply, year, _dedupe_parquet_keys, log)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stage", default="modeling", choices=["modeling", "coords"],
                   help="Qual estagio deduplicar (default: modeling).")
    p.add_argument("--apply", action="store_true", help="Sobrescreve parquets (default: dry-run).")
    p.add_argument("--bases", nargs="+", default=None,
                   help="Bases a processar (default: todas do estagio).")
    p.add_argument("--year", type=int, default=None, help="Ano especifico (default: todos).")
    p.add_argument(
        "--full-row",
        action="store_true",
        help="Somente modeling: remove apenas linhas identicas em todas as colunas (legado). "
        "Sem esta flag, usa subset cidade_norm+ts_hour como coords.",
    )
    args = p.parse_args()

    if args.stage == "modeling":
        bases = args.bases or list(DEFAULT_MODELING_BASES)
        run_modeling(bases, args.apply, args.year, use_full_row=args.full_row)
    else:
        bases = args.bases or list(DEFAULT_COORDS_BASES)
        run_coords(bases, args.apply, args.year)


if __name__ == "__main__":
    main()
