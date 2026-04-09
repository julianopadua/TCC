# src/build_champion_temporal_bases.py
# =============================================================================
# BUILD CHAMPION TEMPORAL BASES
#
# Reads the method_ranking_train.csv produced by feature_engineering_temporal.py
# (Layer A metrics on training years only) and merges the tsf_* columns from
# the top-k ranked methods into two "champion" parquet sets — one for base D
# and one for base F — stored in:
#
#   data/temporal_fusion/base_D_with_rad_drop_rows_calculated_champion_tsfusion/
#   data/temporal_fusion/base_F_full_original_calculated_champion_tsfusion/
#
# These folders are registered in config.yaml as tf_D_champion / tf_F_champion
# and are automatically recognized by train_runner.py via _is_temporal_fusion_scenario.
#
# Usage:
#   python src/build_champion_temporal_bases.py
#   python src/build_champion_temporal_bases.py --top-k 3
#   python src/build_champion_temporal_bases.py --methods arima prophet
#   python src/build_champion_temporal_bases.py --overwrite
# =============================================================================

import sys
import gc
import time
import argparse
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import numpy as np

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

try:
    import src.utils as utils
except ImportError:
    print("[ERRO] Falha ao importar src.utils")
    sys.exit(1)

# Memory monitoring (psutil optional)
try:
    import psutil as _psutil
    _PSUTIL = True
except ImportError:
    _PSUTIL = False

ALL_METHODS = {"ewma_lags", "arima", "sarima", "prophet", "minirocket", "tskmeans"}

# Mapping from base-source key to the long folder name under temporal_fusion/
BASE_SOURCES: Dict[str, str] = {
    "D": "base_D_with_rad_drop_rows_calculated",
    "F": "base_F_full_original_calculated",
}

CHAMPION_SUFFIX = "_champion_tsfusion"


def _log_memory(log, ctx: str) -> None:
    if not _PSUTIL:
        return
    try:
        import os
        p = _psutil.Process(os.getpid())
        vm = _psutil.virtual_memory()
        rss_gb  = p.memory_info().rss / (1024 ** 3)
        avail_gb = vm.available / (1024 ** 3)
        log.info(
            f"[MEMORIA] {ctx} | rss={rss_gb:.2f}GB "
            f"avail={avail_gb:.2f}GB used={vm.percent}%"
        )
    except Exception:
        pass


def _select_methods(
    ranking_path: Path,
    top_k: Optional[int],
    explicit_methods: Optional[List[str]],
    log,
) -> List[str]:
    """Return the list of methods to merge into the champion base."""
    if explicit_methods:
        chosen = [m for m in explicit_methods if m in ALL_METHODS]
        invalid = [m for m in explicit_methods if m not in ALL_METHODS]
        if invalid:
            log.warning(
                f"[SELECT] Métodos inválidos ignorados: {invalid}. "
                f"Válidos: {sorted(ALL_METHODS)}"
            )
        if not chosen:
            raise ValueError(
                f"Nenhum método válido em --methods {explicit_methods}. "
                f"Válidos: {sorted(ALL_METHODS)}"
            )
        log.info(f"[SELECT] Métodos explícitos: {chosen}")
        return chosen

    if not ranking_path.exists():
        raise FileNotFoundError(
            f"Arquivo de ranking não encontrado: {ranking_path}\n"
            "Execute feature_engineering_temporal.py antes de rodar este script, "
            "ou passe --methods explicitamente."
        )

    rank_df = pd.read_csv(ranking_path)
    if "method" not in rank_df.columns or "mae_mean" not in rank_df.columns:
        raise ValueError(
            f"Colunas esperadas ('method', 'mae_mean') não encontradas em "
            f"{ranking_path}. Colunas presentes: {list(rank_df.columns)}"
        )

    rank_df = rank_df.sort_values("mae_mean").reset_index(drop=True)
    log.info(
        f"[SELECT] Ranking de métodos (treino, MAE crescente):\n"
        + rank_df[["method", "mae_mean", "r2_mean"]].to_string(index=False)
    )

    k = top_k if top_k and top_k > 0 else len(rank_df)
    chosen = rank_df["method"].iloc[:k].tolist()
    log.info(f"[SELECT] Top-{k} métodos selecionados: {chosen}")
    return chosen


def _build_champion_for_base(
    base_label: str,
    base_folder: str,
    methods: List[str],
    tf_dir: Path,
    out_dir: Path,
    overwrite: bool,
    log,
) -> None:
    """Merge tsf_* columns from each method into champion parquets for one base."""
    log.info("=" * 60)
    log.info(f"[CHAMPION] Base {base_label} | métodos: {methods}")
    log.info(f"[CHAMPION] Saída: {out_dir}")
    log.info("=" * 60)

    # Discover years from the first available method folder
    year_files: Dict[int, Path] = {}
    for method in methods:
        method_dir = tf_dir / base_folder / method
        if not method_dir.exists():
            log.warning(
                f"[CHAMPION] Pasta não encontrada: {method_dir} — "
                "rode feature_engineering_temporal.py primeiro."
            )
            continue
        for f in sorted(method_dir.glob("inmet_bdq_*_cerrado.parquet")):
            try:
                year = int(f.stem.split("_")[2])
                if year not in year_files:
                    year_files[year] = f
            except Exception:
                pass

    if not year_files:
        log.error(
            f"[CHAMPION] Nenhum parquet encontrado para base {base_label} "
            f"em {tf_dir / base_folder}/. Abortando."
        )
        return

    utils.ensure_dir(out_dir)
    years_sorted = sorted(year_files.keys())
    log.info(f"[CHAMPION] Anos descobertos: {years_sorted}")

    for year in years_sorted:
        filename = year_files[year].name
        out_path  = out_dir / filename

        if out_path.exists() and not overwrite:
            log.info(f"[SKIP] {filename} já existe")
            continue

        t0 = time.time()
        log.info(f"[CHAMPION] Processando {filename}...")
        _log_memory(log, f"pre-load {year}")

        # Start with the first method's full parquet (contains all original cols + tsf_*)
        base_df: Optional[pd.DataFrame] = None
        existing_tsf: set = set()

        for method in methods:
            method_path = tf_dir / base_folder / method / filename
            if not method_path.exists():
                log.warning(
                    f"[CHAMPION] {method}/{filename} não encontrado, pulando este método."
                )
                continue

            try:
                m_df = pd.read_parquet(method_path)
            except Exception as exc:
                log.error(
                    f"[CHAMPION] Falha ao ler {method_path}: {exc}. "
                    "Verifique se o arquivo não está corrompido."
                )
                continue

            tsf_cols = [c for c in m_df.columns if c.startswith("tsf_")]

            if base_df is None:
                # First method: use the full dataframe (all original cols + tsf_*)
                base_df = m_df.copy()
                existing_tsf.update(tsf_cols)
                log.info(
                    f"[CHAMPION {year}] {method}: base carregada "
                    f"({len(base_df)} linhas, {len(tsf_cols)} colunas tsf_*)"
                )
            else:
                # Subsequent methods: merge only NEW tsf_* columns
                new_cols = [c for c in tsf_cols if c not in existing_tsf]
                collisions = [c for c in tsf_cols if c in existing_tsf]
                if collisions:
                    log.warning(
                        f"[CHAMPION {year}] {method}: {len(collisions)} coluna(s) "
                        f"já existem e serão ignoradas: {collisions[:5]}"
                        + (" ..." if len(collisions) > 5 else "")
                    )
                if not new_cols:
                    log.info(
                        f"[CHAMPION {year}] {method}: sem novas colunas tsf_* para mesclar."
                    )
                    del m_df
                    continue

                # Merge on (cidade_norm, ts_hour) — same join key used in feature_engineering_temporal
                merge_keys = ["cidade_norm", "ts_hour"]
                available_keys = [k for k in merge_keys if k in m_df.columns and k in base_df.columns]
                if not available_keys:
                    log.error(
                        f"[CHAMPION {year}] {method}: chaves de merge "
                        f"{merge_keys} não encontradas. Pulando método."
                    )
                    del m_df
                    continue

                base_df = base_df.merge(
                    m_df[available_keys + new_cols],
                    on=available_keys,
                    how="left",
                )
                existing_tsf.update(new_cols)
                log.info(
                    f"[CHAMPION {year}] {method}: {len(new_cols)} novas colunas "
                    f"tsf_* mescladas"
                )
                del m_df
                gc.collect()

        if base_df is None:
            log.error(
                f"[CHAMPION {year}] Nenhum método forneceu dados. "
                f"Arquivo {filename} não gerado."
            )
            continue

        base_df.to_parquet(out_path, index=False)
        elapsed = time.time() - t0
        total_tsf = sum(1 for c in base_df.columns if c.startswith("tsf_"))
        log.info(
            f"[SAVED] {filename} | {len(base_df)} linhas "
            f"| {total_tsf} colunas tsf_* | {elapsed:.1f}s"
        )
        _log_memory(log, f"pos-save {year}")

        del base_df
        gc.collect()

    log.info(f"[CHAMPION] Base {base_label} concluída -> {out_dir}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Constrói bases 'campeãs' combinando as colunas tsf_* dos "
            "melhores métodos de fusão temporal."
        )
    )
    parser.add_argument(
        "--top-k", type=int, default=None,
        help=(
            "Selecionar os top-k métodos por MAE (treino) do ranking. "
            "Padrão: todos os métodos no ranking."
        ),
    )
    parser.add_argument(
        "--methods", nargs="+", default=None,
        choices=sorted(ALL_METHODS),
        help=(
            "Lista explícita de métodos a mesclar (ignora ranking). "
            "Ex.: --methods arima prophet"
        ),
    )
    parser.add_argument(
        "--bases", nargs="+", default=["D", "F"],
        choices=["D", "F"],
        help="Bases a processar (padrão: D F)",
    )
    parser.add_argument(
        "--overwrite", action="store_true",
        help="Sobrescrever parquets campeãs existentes.",
    )
    args = parser.parse_args()

    log = utils.get_logger(
        "champion.build", kind="champion", per_run_file=True
    )
    log.info("=" * 70)
    log.info("BUILD CHAMPION TEMPORAL BASES")
    log.info("=" * 70)

    cfg    = utils.loadConfig()
    tf_dir = Path(cfg["paths"]["data"]["temporal_fusion"])

    # Locate the ranking CSV
    eda_dir     = Path(cfg["paths"]["data"].get("dataset", "data/dataset"))
    ranking_path = eda_dir.parent / "eda" / "temporal_fusion" / "method_ranking_train.csv"
    log.info(f"[RANKING] Procurando em: {ranking_path}")

    try:
        methods = _select_methods(
            ranking_path=ranking_path,
            top_k=args.top_k,
            explicit_methods=args.methods,
            log=log,
        )
    except (FileNotFoundError, ValueError) as exc:
        log.error(f"[ERRO] {exc}")
        sys.exit(1)

    for base_label in args.bases:
        base_folder = BASE_SOURCES[base_label]
        champion_folder = base_folder + CHAMPION_SUFFIX
        out_dir = tf_dir / champion_folder

        _build_champion_for_base(
            base_label=base_label,
            base_folder=base_folder,
            methods=methods,
            tf_dir=tf_dir,
            out_dir=out_dir,
            overwrite=args.overwrite,
            log=log,
        )

    log.info("BUILD CHAMPION COMPLETO")


if __name__ == "__main__":
    main()
