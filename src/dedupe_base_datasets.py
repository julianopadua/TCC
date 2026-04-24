"""Remove duplicatas exatas de linhas nos parquets de data/modeling/base_*.

Contexto:
    A etapa upstream de build dos datasets gerou 2x linhas bit-identicas por
    par (cidade_norm, ts_hour). Todas as bases pre-calculated (A, B, C, D, E, F)
    carregam o bug. Isso se propaga para:
      - base_*_calculated (feature_engineering_physics.py ja distorce features
        sequenciais como precip_ewma, dias_sem_chuva)
      - data/_article/0_datasets_with_coords/
      - data/_article/1_datasets_with_fusion/ (ewma_lags, minirocket, champion)
    Corrigir aqui e regenerar tudo a jusante.

Uso:
    python -m src.dedupe_base_datasets               # dry-run (so reporta)
    python -m src.dedupe_base_datasets --apply       # aplica e sobrescreve
    python -m src.dedupe_base_datasets --apply --bases base_E_with_rad_knn
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
DEFAULT_BASES = (
    "base_A_no_rad",
    "base_B_no_rad_knn",
    "base_C_no_rad_drop_rows",
    "base_D_with_rad_drop_rows",
    "base_E_with_rad_knn",
    "base_F_full_original",
)


def _dedupe_parquet(path: Path, log, apply: bool) -> dict:
    df = pd.read_parquet(path)
    before = len(df)
    df2 = df.drop_duplicates()
    after = len(df2)
    removed = before - after
    ratio = before / after if after else 0.0
    log.info(f"  {path.name:40s} {before:>9d} -> {after:>9d} ({ratio:.2f}x, -{removed})")

    if apply and removed > 0:
        tmp = path.with_suffix(path.suffix + ".dedup.tmp")
        df2.to_parquet(tmp, index=False)
        shutil.move(str(tmp), str(path))
    return {"file": str(path), "before": before, "after": after, "removed": removed}


def run(bases: List[str], apply: bool, year: Optional[int] = None) -> None:
    cfg = loadConfig()
    log = get_logger("dedupe.base")
    modeling_root = Path(cfg["paths"]["data"]["modeling"])

    log.info(f"modeling_root = {modeling_root}")
    log.info(f"mode = {'APPLY (sobrescreve)' if apply else 'DRY-RUN (apenas reporta)'}")
    log.info(f"bases = {bases}")

    totals = {"files": 0, "before": 0, "after": 0, "removed": 0}
    for base in bases:
        bdir = modeling_root / base
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
            r = _dedupe_parquet(f, log, apply)
            totals["files"] += 1
            totals["before"] += r["before"]
            totals["after"] += r["after"]
            totals["removed"] += r["removed"]

    log.info("=" * 60)
    log.info(f"TOTAL: {totals['files']} arquivos")
    log.info(f"  linhas antes:    {totals['before']:>12d}")
    log.info(f"  linhas depois:   {totals['after']:>12d}")
    log.info(f"  duplicatas removidas: {totals['removed']:>12d}")
    if not apply:
        log.info("Dry-run. Rode novamente com --apply para escrever.")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--apply", action="store_true", help="Sobrescreve parquets (default: dry-run).")
    p.add_argument("--bases", nargs="+", default=list(DEFAULT_BASES), help="Bases a processar.")
    p.add_argument("--year", type=int, default=None, help="Ano especifico (default: todos).")
    args = p.parse_args()
    run(args.bases, args.apply, args.year)


if __name__ == "__main__":
    main()
