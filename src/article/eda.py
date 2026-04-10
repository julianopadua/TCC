# src/article/eda.py
# =============================================================================
# EDA espaço-temporal — séries de biomassa + ignições + correlações.
#
# Roda APENAS para cidades benchmark (lista explícita ou herdada da config).
# Consome Parquets de data/_article/0_datasets_with_coords/ já enriquecidos.
# =============================================================================
from __future__ import annotations

import json
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from scipy import stats

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.article.config import ArticlePipelineConfig, load_article_config
from src.article.processing_issues import IssueLogger
from src.utils import ensure_dir, get_logger

STAGE = "eda"
PARQUET_TEMPLATE = "inmet_bdq_{year}_cerrado.parquet"
LABEL_COL = "HAS_FOCO"


# ---------------------------------------------------------------------------
# Data loading (benchmark filter)
# ---------------------------------------------------------------------------
def _discover_years(directory: Path) -> List[int]:
    years = []
    for p in sorted(directory.glob("inmet_bdq_*_cerrado.parquet")):
        m = re.search(r"inmet_bdq_(\d{4})_cerrado", p.stem)
        if m:
            years.append(int(m.group(1)))
    return years


def load_benchmark_data(
    scenario_dir: Path,
    benchmark_cities: List[str],
    years: Optional[List[int]] = None,
    log: Optional[logging.Logger] = None,
) -> pd.DataFrame:
    """Carrega e concatena dados filtrados pelas cidades benchmark."""
    available = _discover_years(scenario_dir)
    if years:
        available = [y for y in available if y in years]

    frames: List[pd.DataFrame] = []
    for year in sorted(available):
        pq = scenario_dir / PARQUET_TEMPLATE.format(year=year)
        if not pq.exists():
            continue
        df = pd.read_parquet(pq)
        df = df[df["cidade_norm"].isin(benchmark_cities)]
        if df.empty:
            if log:
                log.warning("Ano %d: nenhuma linha para cidades benchmark em %s", year, scenario_dir.name)
            continue
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Timeseries plots: biomassa + ignições
# ---------------------------------------------------------------------------
def _has_biomass_cols(df: pd.DataFrame) -> List[str]:
    """Retorna colunas de biomassa presentes no DataFrame."""
    candidates = [c for c in df.columns if c.startswith(("NDVI_", "EVI_"))]
    return [c for c in candidates if df[c].notna().any()]


def plot_biomass_timeseries(
    df: pd.DataFrame,
    city: str,
    out_dir: Path,
    log: logging.Logger,
) -> Optional[Path]:
    """
    Plota séries de biomassa (buffer + ponto) com marcadores de ignição.

    Retorna o path do PNG gerado, ou None se não houver dados de biomassa.
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.dates as mdates
    except ImportError:
        log.warning("matplotlib não disponível — pulando plot para %s.", city)
        return None

    city_df = df[df["cidade_norm"] == city].copy()
    if city_df.empty:
        return None

    bio_cols = _has_biomass_cols(city_df)
    if not bio_cols:
        log.info("  %s: sem colunas de biomassa; plot em modo dry-run (só ignições).", city)

    city_df["_ts"] = pd.to_datetime(city_df["ts_hour"])
    city_df = city_df.sort_values("_ts")

    fig, ax = plt.subplots(figsize=(16, 5))

    # Curvas de biomassa
    for col in bio_cols:
        label = col.replace("_", " ")
        ax.plot(city_df["_ts"], city_df[col], linewidth=0.8, label=label, alpha=0.85)

    # Marcadores de ignição
    if LABEL_COL in city_df.columns:
        fires = city_df[city_df[LABEL_COL] == 1]
        if not fires.empty:
            y_pos = ax.get_ylim()[0] if bio_cols else 0
            ax.scatter(
                fires["_ts"],
                [y_pos] * len(fires),
                marker="|",
                color="red",
                alpha=0.5,
                s=30,
                label="Ignição (HAS_FOCO=1)",
                zorder=5,
            )

    ax.set_title(f"Biomassa e Ignições — {city}", fontsize=12)
    ax.set_xlabel("Data")
    ax.set_ylabel("Índice de Vegetação" if bio_cols else "")
    ax.xaxis.set_major_locator(mdates.YearLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.legend(loc="upper right", fontsize=8)
    fig.tight_layout()

    city_dir = ensure_dir(out_dir / city)
    out_path = city_dir / f"biomass_timeseries_{city}.png"
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    log.info("  Plot salvo: %s", out_path)
    return out_path


# ---------------------------------------------------------------------------
# Correlação biomassa × frequência de focos
# ---------------------------------------------------------------------------
def compute_correlations(
    df: pd.DataFrame,
    biomass_cols: List[str],
    scenario: str,
    log: logging.Logger,
    aggregation: str = "week",
) -> pd.DataFrame:
    """
    Calcula correlação (Pearson e Spearman) entre biomassa e frequência de focos.

    Agrega por (cidade_norm, semana ou dia) antes de computar, para
    reduzir pseudo-replicação horária.
    """
    if not biomass_cols or LABEL_COL not in df.columns:
        return pd.DataFrame()

    df = df.copy()
    df["_ts"] = pd.to_datetime(df["ts_hour"])

    if aggregation == "day":
        df["_period"] = df["_ts"].dt.date
    elif aggregation == "month":
        df["_period"] = df["_ts"].dt.to_period("M").astype(str)
    else:
        df["_period"] = df["_ts"].dt.isocalendar().week.astype(str) + "_" + df["_ts"].dt.year.astype(str)

    grouped = df.groupby(["cidade_norm", "_period"]).agg(
        freq_focos=(LABEL_COL, "sum"),
        **{col: (col, "mean") for col in biomass_cols},
    ).reset_index()

    rows = []
    for city in grouped["cidade_norm"].unique():
        cdf = grouped[grouped["cidade_norm"] == city].dropna()
        for col in biomass_cols:
            valid = cdf[[col, "freq_focos"]].dropna()
            n = len(valid)
            if n < 5:
                continue

            r_pearson, p_pearson = stats.pearsonr(valid[col], valid["freq_focos"])
            r_spearman, p_spearman = stats.spearmanr(valid[col], valid["freq_focos"])

            for metric, r, p in [("pearson", r_pearson, p_pearson), ("spearman", r_spearman, p_spearman)]:
                rows.append({
                    "cidade_norm": city,
                    "aggregation": aggregation,
                    "variable_bio": col,
                    "metric": metric,
                    "r": round(r, 6),
                    "p_value": round(p, 8),
                    "n": n,
                    "scenario": scenario,
                    "year_span": f"{df['_ts'].dt.year.min()}-{df['_ts'].dt.year.max()}",
                })

    result = pd.DataFrame(rows)
    if not result.empty:
        log.info("  Correlações computadas: %d pares (cenário %s, agg=%s)", len(result), scenario, aggregation)
    return result


# ---------------------------------------------------------------------------
# Orquestração
# ---------------------------------------------------------------------------
def run_eda(
    scenario_key: Optional[str] = None,
    years: Optional[List[int]] = None,
) -> None:
    """Executa EDA para cidades benchmark: plots + correlações."""
    acfg = load_article_config()
    log = get_logger("article.eda", kind="article", per_run_file=True)
    issues = IssueLogger(ensure_dir(acfg.output_root / "logs") / "processing_issues.csv")

    log.info("=" * 72)
    log.info("ARTICLE PIPELINE — EDA: Biomassa e Ignições")
    log.info("=" * 72)

    benchmark = acfg.eda.benchmark_cities
    if not benchmark:
        log.warning(
            "Nenhuma cidade benchmark configurada. Defina "
            "article_pipeline.eda.benchmark_cities no config.yaml."
        )
        issues.log(
            STAGE, "NO_BENCHMARK_CITIES",
            "Lista de benchmark_cities vazia; EDA não executado.",
            "SKIPPED",
        )
        issues.flush()
        return

    log.info("Cidades benchmark: %s", benchmark)

    scenarios = acfg.scenarios
    if scenario_key:
        scenarios = {k: v for k, v in scenarios.items() if k == scenario_key}

    plots_dir = ensure_dir(acfg.output_root / "eda" / "plots" / "timeseries")
    stats_dir = ensure_dir(acfg.output_root / "eda" / "stats")
    all_correlations: List[pd.DataFrame] = []
    index_entries: List[dict] = []

    for key, folder in scenarios.items():
        log.info("--- EDA cenário %s (%s) ---", key, folder)
        scenario_dir = acfg.output_root / "0_datasets_with_coords" / folder
        if not scenario_dir.exists():
            log.warning("Diretório não existe: %s", scenario_dir)
            continue

        df = load_benchmark_data(scenario_dir, benchmark, years, log)
        if df.empty:
            issues.log(
                STAGE, "CITY_NOT_IN_DATA",
                f"Nenhum dado para cidades benchmark no cenário {key}.",
                "SKIPPED",
            )
            continue

        log.info("  %d linhas carregadas para %d cidades", len(df), df["cidade_norm"].nunique())

        # Plots
        for city in benchmark:
            if city not in df["cidade_norm"].values:
                issues.log(
                    STAGE, "CITY_NOT_IN_DATA",
                    f"Cidade '{city}' não encontrada no cenário {key}.",
                    "SKIPPED",
                )
                continue
            out_path = plot_biomass_timeseries(df, city, plots_dir, log)
            if out_path:
                index_entries.append({
                    "city": city,
                    "scenario": key,
                    "path": str(out_path.relative_to(acfg.output_root)),
                })

        # Correlações
        bio_cols = _has_biomass_cols(df)
        if bio_cols:
            for agg in ["week", "day"]:
                corr = compute_correlations(df, bio_cols, key, log, aggregation=agg)
                if not corr.empty:
                    all_correlations.append(corr)
        else:
            log.info("  Sem colunas de biomassa — correlações puladas (dry-run).")
            issues.log(
                STAGE, "MISSING_BIOMASS_COL",
                f"Nenhuma coluna NDVI_*/EVI_* encontrada no cenário {key}.",
                "SKIPPED",
            )

        del df

    # Salvar correlações
    if all_correlations:
        corr_df = pd.concat(all_correlations, ignore_index=True)
        corr_path = stats_dir / "correlation_biomass_fires.csv"
        corr_df.to_csv(corr_path, index=False, encoding="utf-8")
        log.info("Correlações salvas: %s (%d linhas)", corr_path, len(corr_df))

    # Salvar index.json
    if index_entries:
        idx_path = plots_dir / "index.json"
        with open(idx_path, "w", encoding="utf-8") as f:
            json.dump(index_entries, f, indent=2, ensure_ascii=False)
        log.info("Índice de plots: %s", idx_path)

    issues.flush()
    log.info("EDA finalizado.")


if __name__ == "__main__":
    run_eda()
