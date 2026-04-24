# src/article/article_orchestrator.py
# =============================================================================
# ORQUESTRADOR CLI DO PIPELINE DO ARTIGO
#
# Orquestra as etapas do pipeline de fusao temporal do artigo:
#   Etapa 1 — Fusao Temporal    (temporal_fusion_article.run_article_fusion)
#   Etapa 2 — Camada A          (feature_selection_article.run_feature_selection)
#   Etapa 3 — Champion builder  (consolidar TOP K tsf_* + originais em
#                                1_datasets_with_fusion/{cenario}/champion/)
#
# Ver doc/planos/plano_fusao_article_v1.md para o fluxograma completo.
#
# Exemplos de uso:
#   # tudo, cenario E
#   python src/article/article_orchestrator.py
#
#   # apenas EWMA+Lags no cenario D, anos 2020-2022, sem treinar
#   python src/article/article_orchestrator.py \
#       --scenario D --methods ewma_lags --years 2020 2021 2022 --skip-train
#
#   # re-rodar tudo do zero
#   python src/article/article_orchestrator.py --overwrite
#
#   # so feature selection (supondo parquets de fusion ja gerados)
#   python src/article/article_orchestrator.py --skip-fusion --skip-train
# =============================================================================
from __future__ import annotations

import argparse
import gc
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import numpy as np
import pandas as pd

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils  # noqa: E402
from src.article.temporal_fusion_article import (  # noqa: E402
    ALLOWED_METHODS,
    load_fusion_config,
    resolve_scenario,
    run_article_fusion,
)
from src.article.feature_selection_article import (  # noqa: E402
    load_selected_features,
    run_feature_selection,
)


# ---------------------------------------------------------------------------
# Logging dedicado por execucao
# ---------------------------------------------------------------------------
def _setup_run_logger(cfg: dict) -> logging.Logger:
    """Cria logger com arquivo dedicado em logs/article_run_{timestamp}.log."""
    logs_dir = Path(cfg["paths"]["logs"])
    utils.ensure_dir(logs_dir)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_log_path = logs_dir / f"article_run_{ts}.log"

    logger = logging.getLogger(f"article.orchestrator.{ts}")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    fh = logging.FileHandler(run_log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"Log desta execucao: {run_log_path}")
    return logger


# ---------------------------------------------------------------------------
# Etapa 3 — Champion: consolida originais + TOP K tsf_* num parquet por ano
# ---------------------------------------------------------------------------
def _build_champion(
    scenario_folder: str,
    methods: List[str],
    overwrite: bool,
    years: Optional[List[int]],
    log: logging.Logger,
) -> None:
    """Gera parquets em 1_datasets_with_fusion/{cenario}/champion/.

    Cada parquet contem: todas as colunas originais (ou seja, as do cenario
    em 0_datasets_with_coords/) mais as TOP K features tsf_* selecionadas
    pela Camada A, tiradas de 1_datasets_with_fusion/{cenario}/{metodo}/.
    """
    fcfg = load_fusion_config()
    coord_dir = fcfg["input_dir"] / scenario_folder
    fusion_root = fcfg["output_dir"] / scenario_folder
    champion_dir = fusion_root / "champion"

    try:
        selected = load_selected_features()
    except FileNotFoundError as exc:
        log.error(f"[champion] {exc}")
        return

    if not selected:
        log.warning("[champion] lista de features selecionadas vazia; nada a montar.")
        return

    log.info(f"[champion] {len(selected)} features selecionadas serao integradas.")

    # Indexa qual metodo fornece cada feature (a feature pode estar em mais
    # de um parquet; escolhemos a pasta pelo prefixo do nome).
    def _infer_method_folder(feat: str) -> Optional[str]:
        head = feat.split("_", 2)
        if len(head) < 2:
            return None
        token = head[1]
        if token in ("ewma", "lag"):
            return "ewma_lags" if "ewma_lags" in methods else None
        if token == "minirocket":
            return "minirocket" if "minirocket" in methods else None
        if token == "sarimax":
            return "sarimax_exog" if "sarimax_exog" in methods else None
        return None

    # Agrupa por metodo -> lista de features.
    by_method: dict = {}
    skipped: List[str] = []
    for feat in selected:
        m = _infer_method_folder(feat)
        if m is None:
            skipped.append(feat)
            continue
        by_method.setdefault(m, []).append(feat)
    if skipped:
        log.warning(
            f"[champion] {len(skipped)} features sem metodo resolvido; ignoradas. "
            f"(ex.: {skipped[:3]})"
        )

    # Descobre anos a processar a partir do diretorio de coords.
    all_files = sorted(coord_dir.glob("inmet_bdq_*_cerrado.parquet"))
    files_by_year: dict = {}
    for f in all_files:
        try:
            y = int(f.stem.split("_")[2])
            if years and y not in years:
                continue
            files_by_year[y] = f
        except Exception:
            continue

    if not files_by_year:
        log.warning(f"[champion] nenhum parquet encontrado em {coord_dir}")
        return

    utils.ensure_dir(champion_dir)

    for year in sorted(files_by_year.keys()):
        src_path = files_by_year[year]
        out_path = champion_dir / src_path.name
        if out_path.exists() and not overwrite:
            log.info(f"[champion] SKIP {out_path.name} (ja existe)")
            continue

        t0 = time.time()
        base = pd.read_parquet(src_path)
        log.info(
            f"[champion] {year}: base={len(base)} rows, {len(base.columns)} cols"
        )

        merged = base
        total_added = 0
        for method, feats in by_method.items():
            feat_parquet = fusion_root / method / src_path.name
            if not feat_parquet.exists():
                log.warning(
                    f"[champion] {year}/{method}: parquet ausente "
                    f"({feat_parquet}); features do metodo puladas."
                )
                continue
            wanted = ["cidade_norm", "ts_hour"] + [f for f in feats]

            # Projeta apenas colunas relevantes para economizar RAM.
            try:
                import pyarrow.parquet as pq
                avail = set(pq.ParquetFile(str(feat_parquet)).schema_arrow.names)
            except Exception:
                avail = set(pd.read_parquet(feat_parquet).columns)

            wanted = [c for c in wanted if c in avail]
            missing = [f for f in feats if f not in avail]
            if missing:
                log.warning(
                    f"[champion] {year}/{method}: {len(missing)} features nao "
                    f"encontradas no parquet (ex.: {missing[:3]})"
                )

            feat_df = pd.read_parquet(feat_parquet, columns=wanted)
            # Deduplica chave se ja presente em merged.
            new_feat_cols = [c for c in feat_df.columns if c not in ("cidade_norm", "ts_hour")]
            if not new_feat_cols:
                continue
            # Parquets de fusao podem repetir (cidade_norm, ts_hour) N vezes (ex.: artefatos
            # de join). Merge com duplicatas no DIREITO faz produto cartesiano (NxM por chave)
            # e explode linhas + RAM — ex.: 2.36M -> 37.8M linhas e bloco float64 ~2.8GiB.
            key_cols = ["cidade_norm", "ts_hour"]
            n_feat_before = len(feat_df)
            feat_df = feat_df.drop_duplicates(subset=key_cols, keep="last")
            if len(feat_df) < n_feat_before:
                log.warning(
                    f"[champion] {year}/{method}: dedup {key_cols}: "
                    f"{n_feat_before} -> {len(feat_df)} linhas (evita explosao no merge)"
                )
            for c in new_feat_cols:
                if pd.api.types.is_float_dtype(feat_df[c].dtype):
                    feat_df[c] = feat_df[c].astype(np.float32)
            merged = merged.merge(
                feat_df,
                on=key_cols,
                how="left",
                suffixes=("", "_dup"),
                validate="many_to_one",
            )
            # Descarta possiveis colunas duplicadas.
            dup_cols = [c for c in merged.columns if c.endswith("_dup")]
            if dup_cols:
                merged.drop(columns=dup_cols, inplace=True)
            total_added += len(new_feat_cols)
            del feat_df
            gc.collect()

        expected_rows = len(base)
        actual_rows = len(merged)
        if actual_rows > expected_rows * 1.05:
            log.error(
                f"[champion] ABORTED {out_path.name}: row count exploded "
                f"({actual_rows} vs expected ~{expected_rows}). "
                "Likely duplicate (cidade_norm, ts_hour) keys in a fusion parquet. "
                "Regenerate fusion parquets with --overwrite and retry."
            )
            del merged, base
            gc.collect()
            continue

        merged.to_parquet(out_path, index=False)
        log.info(
            f"[champion] SAVED {out_path.name} | {len(merged)} rows | "
            f"+{total_added} features tsf_* | {time.time() - t0:.1f}s"
        )
        del merged, base
        gc.collect()

    log.info("[champion] concluido.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="article_orchestrator",
        description=(
            "Orquestrador CLI do pipeline de fusao temporal do artigo. "
            "Ver doc/planos/plano_fusao_article_v1.md."
        ),
    )
    parser.add_argument(
        "--scenario", default="E",
        help="Cenario (D, E, F ou folder name completo). Default: E.",
    )
    parser.add_argument(
        "--methods", nargs="+",
        choices=sorted(ALLOWED_METHODS),
        default=None,
        help="Metodos de fusao (default: todos do config).",
    )
    parser.add_argument(
        "--top-k", type=int, default=None,
        help="TOP K features tsf_* a reter na Camada A (default: config).",
    )
    parser.add_argument(
        "--years", type=int, nargs="+", default=None,
        help="Subset de anos a processar (default: todos descobertos).",
    )
    parser.add_argument(
        "--test-years", type=int, default=None,
        help="Ultimos N anos reservados como teste (default: config).",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Regravar parquets existentes.",
    )
    parser.add_argument(
        "--sarimax-workers", type=int, default=None,
        help=(
            "Override de workers para sarimax_exog "
            "(default: config.yaml article_pipeline.temporal_fusion.sarimax_exog.workers)."
        ),
    )
    parser.add_argument(
        "--skip-fusion", action="store_true",
        help="Pular Etapa 1 (fusao temporal).",
    )
    parser.add_argument(
        "--skip-selection", action="store_true",
        help="Pular Etapa 2 (Camada A).",
    )
    parser.add_argument(
        "--skip-train", action="store_true",
        help=(
            "Pular Etapa 3 (champion builder). Se presente, a orquestracao nao "
            "gera o parquet final em {cenario}/champion/."
        ),
    )
    return parser


def main(argv: Optional[List[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    cfg = utils.loadConfig()
    log = _setup_run_logger(cfg)

    t_total = time.time()
    log.info("=" * 72)
    log.info("ARTICLE PIPELINE ORCHESTRATOR")
    log.info(f"args: {vars(args)}")
    log.info("=" * 72)

    # Resolve cenario.
    fcfg = load_fusion_config()
    try:
        scenario_folder = resolve_scenario(args.scenario, fcfg["scenarios"])
    except ValueError as exc:
        log.error(str(exc))
        return 2
    log.info(f"Cenario resolvido: {args.scenario} -> {scenario_folder}")

    # Verifica diretorio de entrada (0_datasets_with_coords).
    input_dir = fcfg["input_dir"] / scenario_folder
    if not input_dir.exists():
        log.error(f"Diretorio de entrada ausente: {input_dir}")
        log.error("Rode antes: python src/article/run_pipeline.py")
        return 3

    methods = list(args.methods) if args.methods else list(fcfg["methods"])
    log.info(f"Metodos ativos: {methods}")

    summary: dict = {
        "scenario": scenario_folder,
        "methods": methods,
        "steps_ran": [],
        "start": datetime.now().isoformat(),
    }

    # -----------------------------------------------------------------------
    # Etapa 1 — Fusao temporal
    # -----------------------------------------------------------------------
    if args.skip_fusion:
        log.info("[etapa 1/3] SKIPPED (--skip-fusion)")
    else:
        log.info("[etapa 1/3] Fusao Temporal - inicio")
        t1 = time.time()
        try:
            run_article_fusion(
                scenario_folder=scenario_folder,
                methods=methods,
                overwrite=args.overwrite,
                years=args.years,
                test_size_years=args.test_years,
                sarimax_workers=args.sarimax_workers,
                log=log,
            )
            summary["steps_ran"].append("fusion")
            log.info(
                f"[etapa 1/3] Fusao concluida em {time.time() - t1:.0f}s"
            )
        except Exception as exc:
            log.error(f"[etapa 1/3] Falha fatal: {exc}", exc_info=True)
            return 10

    # -----------------------------------------------------------------------
    # Etapa 2 — Camada A (Feature Selection)
    # -----------------------------------------------------------------------
    if args.skip_selection:
        log.info("[etapa 2/3] SKIPPED (--skip-selection)")
    else:
        log.info("[etapa 2/3] Camada A (Spearman + MI) - inicio")
        t2 = time.time()
        try:
            info = run_feature_selection(
                scenario_folder=scenario_folder,
                methods=methods,
                top_k=args.top_k,
                log=log,
            )
            summary["selection_result"] = info
            summary["steps_ran"].append("selection")
            log.info(
                f"[etapa 2/3] Camada A concluida em {time.time() - t2:.0f}s "
                f"| total={info['n_features_total']} selecionadas="
                f"{info['n_features_selected']}"
            )
        except Exception as exc:
            log.error(f"[etapa 2/3] Falha: {exc}", exc_info=True)
            if not args.skip_train:
                log.warning(
                    "[etapa 2/3] Pulando Etapa 3 porque a Camada A falhou."
                )
                args.skip_train = True

    # -----------------------------------------------------------------------
    # Etapa 3 — Champion builder (base consolidada para XGBoost/RF)
    # -----------------------------------------------------------------------
    if args.skip_train:
        log.info("[etapa 3/3] SKIPPED (--skip-train)")
    else:
        log.info("[etapa 3/3] Champion builder - inicio")
        t3 = time.time()
        try:
            _build_champion(
                scenario_folder=scenario_folder,
                methods=methods,
                overwrite=args.overwrite,
                years=args.years,
                log=log,
            )
            summary["steps_ran"].append("champion")
            log.info(
                f"[etapa 3/3] Champion concluido em {time.time() - t3:.0f}s"
            )
            log.info(
                "[etapa 3/3] Treino: use train_runner com --article e cenario "
                f"tf_*_champion (ex.: python src/train_runner.py run --article "
                f"-s tf_E_champion -m xgboost -v 1). Parquets: 1_datasets_with_fusion/"
                f"{scenario_folder}/champion/"
            )
        except Exception as exc:
            log.error(f"[etapa 3/3] Falha: {exc}", exc_info=True)

    # -----------------------------------------------------------------------
    # Summary
    # -----------------------------------------------------------------------
    summary["elapsed_s"] = round(time.time() - t_total, 1)
    summary["end"] = datetime.now().isoformat()
    log.info("=" * 72)
    log.info(f"SUMMARY: {json.dumps(summary, ensure_ascii=False, indent=2)}")
    log.info(f"Pipeline concluido em {summary['elapsed_s']}s")
    log.info("=" * 72)
    return 0


if __name__ == "__main__":
    sys.exit(main())
