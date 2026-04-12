# src/article/run_pipeline.py
# =============================================================================
# Ponto de entrada do pipeline do artigo.
#
# Etapas:
#   0. enrich_coords — coordenadas BDQ + estação + fallback
#   1. gee_biomass   — extração semanal + ffill + propagação (stub até GEE real)
#   2. eda           — séries temporais + correlação (benchmark cities)
# =============================================================================
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.utils import get_logger


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline do artigo: coordenadas, GEE e EDA.",
    )
    parser.add_argument(
        "--years", type=int, nargs="*", default=None,
        help="Anos a processar (vazio = todos).",
    )
    parser.add_argument(
        "--skip-years", type=int, nargs="+", default=None,
        metavar="YEAR",
        help="Anos a ignorar em todas as etapas (ex.: já processados com GEE).",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help=(
            "Coords e GEE: ignorar manifesto (refazer). Com --years, só esses anos "
            "são refeitos no GEE; coords refaz qualquer ano da execução."
        ),
    )
    parser.add_argument(
        "--skip-coords", action="store_true",
        help="Pular etapa 0 (enriquecimento de coordenadas).",
    )
    parser.add_argument(
        "--skip-gee", action="store_true",
        help="Pular etapa 1 (extração GEE + propagação).",
    )
    parser.add_argument(
        "--skip-eda", action="store_true",
        help="Pular etapa 2 (EDA: plots + correlações).",
    )
    parser.add_argument(
        "--only-coords", action="store_true",
        help="Rodar apenas etapa 0.",
    )
    parser.add_argument(
        "--only-eda", action="store_true",
        help="Rodar apenas etapa 2.",
    )

    args = parser.parse_args()
    log = get_logger("article.pipeline", kind="article", per_run_file=True)

    run_coords = not args.skip_coords
    run_gee = not args.skip_gee
    run_eda = not args.skip_eda

    if args.only_coords:
        run_gee = run_eda = False
    if args.only_eda:
        run_coords = run_gee = False

    log.info("=" * 72)
    log.info("ARTICLE PIPELINE — Início")
    log.info("  Etapas: coords=%s, gee=%s, eda=%s", run_coords, run_gee, run_eda)
    log.info("  Anos: %s", args.years or "todos")
    if args.skip_years:
        log.info("  Pular anos: %s", args.skip_years)
    if args.overwrite:
        log.info("  --overwrite: coords e GEE podem refazer trabalho já registrado.")
    log.info("=" * 72)

    if run_coords:
        from src.article.enrich_coords import run_all as run_coords_fn
        run_coords_fn(
            years=args.years, skip_years=args.skip_years, overwrite=args.overwrite,
        )

    if run_gee:
        from src.article.gee_biomass import run_gee_pipeline
        run_gee_pipeline(
            years=args.years, skip_years=args.skip_years, overwrite=args.overwrite,
        )

    if run_eda:
        from src.article.eda import run_eda as run_eda_fn
        run_eda_fn(years=args.years, skip_years=args.skip_years)

    log.info("ARTICLE PIPELINE — Concluído.")


if __name__ == "__main__":
    main()
