# src/integrations/inmet_gee/plots.py
# =============================================================================
# GRÁFICOS — Comparação E vs F por variável e cidade, visão global com downsample
# Backend Agg (headless). Fallback: só export tabular se matplotlib falhar.
# =============================================================================
from __future__ import annotations

import gc
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

os.environ.setdefault("MPLBACKEND", "Agg")

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    _MPL_OK = True
except ImportError:
    _MPL_OK = False

from .config import PipelineConfig, TimeseriesConfig

# Etiquetas curtas para variáveis longas (usadas em nomes de arquivo e títulos)
_VAR_SHORT: Dict[str, str] = {
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "precip_mm",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "pressao_mb",
    "RADIACAO GLOBAL (KJ/m²)": "radiacao_kj",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "temp_bulbo_c",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "temp_orvalho_c",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "umidade_pct",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "vento_direcao_gr",
    "VENTO, RAJADA MAXIMA (m/s)": "vento_rajada_ms",
    "VENTO, VELOCIDADE HORARIA (m/s)": "vento_vel_ms",
    "precip_ewma": "precip_ewma",
    "dias_sem_chuva": "dias_sem_chuva",
    "precip_cumsum_E": "precip_cumsum",
    "precip_cumsum_F": "precip_cumsum",
}

_DOWNSAMPLE_FREQS = {
    "daily_mean": "D",
    "weekly_mean": "W",
    "none": None,
}

# Variáveis cuja cumsum de precipitação faz sentido plotar separadamente
_CUMSUM_VARS = ("precip_cumsum_E", "precip_cumsum_F")


def _var_slug(var: str) -> str:
    return _VAR_SHORT.get(var, var[:30].replace(" ", "_").replace(",", "").replace("/", "-"))


def _downsample_df(df: pd.DataFrame, freq: str, variables: List[str]) -> pd.DataFrame:
    """
    Agrega df (com coluna ts_hour e cidade_norm) por frequência temporal.
    Apenas colunas numéricas (variáveis E e F).
    """
    df = df.copy()
    df["ts_hour"] = pd.to_datetime(df["ts_hour"], errors="coerce")
    df = df.dropna(subset=["ts_hour"])
    df = df.set_index("ts_hour")

    num_cols = [c for c in df.columns if c.endswith("__E") or c.endswith("__F")
                or c in _CUMSUM_VARS]
    if "cidade_norm" in df.columns:
        result = (
            df.groupby("cidade_norm")[num_cols]
            .resample(freq)
            .mean()
            .reset_index()
        )
    else:
        result = df[num_cols].resample(freq).mean().reset_index()
        result = result.rename(columns={"ts_hour": "ts_hour"})

    return result


def _plot_variable_city(
    ax: "plt.Axes",
    df_year: pd.DataFrame,
    variable: str,
    city: str,
    color_e: str,
    color_f: str,
    year_span: str,
) -> None:
    """Plota a série de uma variável para uma cidade (E e F) em um eixo."""
    col_e = f"{variable}__E"
    col_f = f"{variable}__F"
    sub = df_year[df_year["cidade_norm"] == city].copy()
    sub["ts_hour"] = pd.to_datetime(sub["ts_hour"], errors="coerce")
    sub = sub.dropna(subset=["ts_hour"]).sort_values("ts_hour")

    if col_e in sub.columns:
        ax.plot(sub["ts_hour"], sub[col_e], color=color_e, lw=0.6, alpha=0.85, label="Base E")
    if col_f in sub.columns:
        ax.plot(sub["ts_hour"], sub[col_f], color=color_f, lw=0.6, alpha=0.60, label="Base F", linestyle="--")

    ax.set_title(f"{city.upper()} | {variable[:55]}", fontsize=7)
    ax.tick_params(axis="both", labelsize=6)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%b"))
    ax.legend(fontsize=5, loc="upper right")


def generate_city_plots(
    yearly_dir: Path,
    plots_dir: Path,
    sample_cities: List[str],
    variables: List[str],
    color_e: str,
    color_f: str,
    log: logging.Logger,
    downsample: Optional[str] = "daily_mean",
) -> List[dict]:
    """
    Para cada variável e cidade amostra, gera figura multi-painel (um subplot por ano).
    Retorna lista de metadados de figura para o index.json.
    """
    if not _MPL_OK:
        log.warning("matplotlib não disponível. Gráficos ignorados.")
        return []

    plots_dir.mkdir(parents=True, exist_ok=True)
    freq = _DOWNSAMPLE_FREQS.get(downsample or "none")
    index_entries = []

    parquet_files = sorted(yearly_dir.glob("ts_compare_*.parquet"))
    if not parquet_files:
        log.warning("Nenhum Parquet de séries encontrado em '%s'.", yearly_dir)
        return []

    # Para cada variável, gera uma figura com subplots (cidades × anos)
    for var in variables:
        col_e = f"{var}__E"
        col_f = f"{var}__F"
        slug = _var_slug(var)

        for city in sample_cities:
            frames = []
            years_found = []
            for pq_path in parquet_files:
                try:
                    year_str = pq_path.stem.replace("ts_compare_", "")
                    year = int(year_str)
                    cols_need = ["ts_hour", "cidade_norm"] + [
                        c for c in [col_e, col_f, "precip_cumsum_E", "precip_cumsum_F"]
                        if True  # always try, missing handled below
                    ]
                    df = pd.read_parquet(pq_path, columns=[
                        c for c in pd.read_parquet(pq_path, columns=None).columns
                        if c in cols_need
                    ])
                    df_city = df[df["cidade_norm"] == city]
                    if df_city.empty:
                        continue
                    if freq:
                        df_city = _downsample_df(df_city, freq, variables)
                    df_city["_year"] = year
                    frames.append(df_city)
                    years_found.append(year)
                except Exception as exc:
                    log.warning("Erro ao ler '%s' para plot: %s", pq_path.name, exc)
                    continue

            if not frames:
                log.info("Variável '%s' | Cidade '%s': sem dados — gráfico ignorado.", var, city)
                continue

            df_all = pd.concat(frames, ignore_index=True)
            df_all["ts_hour"] = pd.to_datetime(df_all["ts_hour"], errors="coerce")
            df_all = df_all.dropna(subset=["ts_hour"]).sort_values("ts_hour")

            # Uma figura única com toda a série temporal (multi-ano)
            fig, ax = plt.subplots(figsize=(14, 4))

            if col_e in df_all.columns:
                ax.plot(
                    df_all["ts_hour"], df_all[col_e],
                    color=color_e, lw=0.7, alpha=0.9, label="Base E (KNN imputada)",
                )
            if col_f in df_all.columns:
                ax.plot(
                    df_all["ts_hour"], df_all[col_f],
                    color=color_f, lw=0.7, alpha=0.65, label="Base F (original)", linestyle="--",
                )

            year_span = f"{min(years_found)}–{max(years_found)}"
            ax.set_title(
                f"{city.upper()} | {var} | {year_span}"
                + (f" [{downsample}]" if downsample and downsample != "none" else ""),
                fontsize=9,
            )
            ax.set_xlabel("Período", fontsize=8)
            ax.set_ylabel(var[:40], fontsize=7)
            ax.legend(fontsize=7)
            ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
            plt.xticks(rotation=30, fontsize=7)
            plt.tight_layout()

            fname = f"compare_{slug}_{city}_{year_span}.png"
            out_path = plots_dir / fname
            fig.savefig(out_path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            gc.collect()

            entry = {
                "file": fname,
                "variable": var,
                "city": city,
                "years": years_found,
                "downsample": downsample,
                "color_E": color_e,
                "color_F": color_f,
                "generated_at": datetime.now(timezone.utc).isoformat(),
            }
            index_entries.append(entry)
            log.info("Gráfico gerado: '%s'.", out_path)

            # Acumulado de precipitação (figura separada)
            if "precip_cumsum_E" in df_all.columns or "precip_cumsum_F" in df_all.columns:
                fig2, ax2 = plt.subplots(figsize=(14, 4))
                if "precip_cumsum_E" in df_all.columns:
                    ax2.plot(
                        df_all["ts_hour"], df_all["precip_cumsum_E"],
                        color=color_e, lw=0.8, alpha=0.9, label="Acumulado E",
                    )
                if "precip_cumsum_F" in df_all.columns:
                    ax2.plot(
                        df_all["ts_hour"], df_all["precip_cumsum_F"],
                        color=color_f, lw=0.8, alpha=0.65, label="Acumulado F", linestyle="--",
                    )
                ax2.set_title(
                    f"{city.upper()} | Precipitação Acumulada | {year_span}",
                    fontsize=9,
                )
                ax2.set_xlabel("Período", fontsize=8)
                ax2.set_ylabel("Precip. Acumulada (mm)", fontsize=7)
                ax2.legend(fontsize=7)
                ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
                plt.xticks(rotation=30, fontsize=7)
                plt.tight_layout()

                fname2 = f"compare_precip_cumsum_{city}_{year_span}.png"
                out_path2 = plots_dir / fname2
                fig2.savefig(out_path2, dpi=150, bbox_inches="tight")
                plt.close(fig2)
                gc.collect()

                index_entries.append({
                    "file": fname2,
                    "variable": "precip_cumsum",
                    "city": city,
                    "years": years_found,
                    "downsample": downsample,
                    "color_E": color_e,
                    "color_F": color_f,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                })
                log.info("Gráfico acumulado: '%s'.", out_path2)

    return index_entries


def generate_global_overview(
    yearly_dir: Path,
    plots_dir: Path,
    sample_cities: List[str],
    variable: str,
    color_e: str,
    color_f: str,
    downsample: str,
    log: logging.Logger,
) -> Optional[str]:
    """
    Gera visão agregada 'todo o dataset': média das cidades amostra após downsample.
    Retorna caminho do arquivo ou None em caso de falha.
    """
    if not _MPL_OK:
        return None

    freq = _DOWNSAMPLE_FREQS.get(downsample, "D") or "D"
    col_e = f"{variable}__E"
    col_f = f"{variable}__F"
    slug = _var_slug(variable)

    frames = []
    for pq_path in sorted(yearly_dir.glob("ts_compare_*.parquet")):
        try:
            avail = pd.read_parquet(pq_path, columns=None).columns.tolist()
            cols = [c for c in ["ts_hour", "cidade_norm", col_e, col_f] if c in avail]
            df = pd.read_parquet(pq_path, columns=cols)
            if sample_cities:
                df = df[df["cidade_norm"].isin(sample_cities)]
            df["ts_hour"] = pd.to_datetime(df["ts_hour"], errors="coerce")
            df = df.dropna(subset=["ts_hour"])
            frames.append(df)
        except Exception as exc:
            log.warning("Visão global: erro ao ler '%s': %s", pq_path.name, exc)

    if not frames:
        log.warning("Visão global: sem dados para '%s'.", variable)
        return None

    df_all = pd.concat(frames, ignore_index=True)
    df_all = df_all.set_index("ts_hour")

    num_cols = [c for c in [col_e, col_f] if c in df_all.columns]
    df_ds = df_all[num_cols].resample(freq).mean()

    log.info(
        "Visão global '%s': downsample=%s aplicado. "
        "Método de agregação: média das cidades amostra + resample temporal por '%s'. "
        "Linhas após downsample: %d.",
        variable, downsample, freq, len(df_ds),
    )

    fig, ax = plt.subplots(figsize=(16, 4))
    if col_e in df_ds.columns:
        ax.plot(df_ds.index, df_ds[col_e], color=color_e, lw=0.8, label="Base E (regional)")
    if col_f in df_ds.columns:
        ax.plot(df_ds.index, df_ds[col_f], color=color_f, lw=0.8, label="Base F (regional)", linestyle="--")

    year_range = f"{df_ds.index.min().year}–{df_ds.index.max().year}" if len(df_ds) else "?"
    ax.set_title(f"Visão Regional (Cidades Amostra) | {variable} | {year_range}", fontsize=9)
    ax.set_xlabel("Período", fontsize=8)
    ax.set_ylabel(variable[:40], fontsize=7)
    ax.legend(fontsize=7)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    plt.xticks(rotation=30, fontsize=7)
    plt.tight_layout()

    fname = f"global_{slug}_{year_range}_{downsample}.png"
    out_path = plots_dir / fname
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    gc.collect()
    log.info("Gráfico global gerado: '%s'.", out_path)
    return str(out_path)


def write_plot_index(plots_dir: Path, entries: List[dict], log: logging.Logger) -> None:
    """Grava index.json com metadados de todos os gráficos gerados."""
    idx_path = plots_dir / "index.json"
    tmp = idx_path.with_suffix(".tmp.json")
    tmp.write_text(
        json.dumps({"generated_at": datetime.now(timezone.utc).isoformat(), "plots": entries},
                   ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    os.replace(tmp, idx_path)
    log.info("index.json de gráficos gravado em '%s' (%d entradas).", idx_path, len(entries))


class PlotRunner:
    """
    Orquestra toda a geração de gráficos após as séries temporais estarem exportadas.
    """

    def __init__(self, cfg: PipelineConfig, log: logging.Logger):
        self.cfg = cfg
        self.ts_cfg = cfg.timeseries
        self.log = log
        self.yearly_dir = cfg.output_dir / "outputs" / "timeseries" / "yearly"
        self.plots_dir = cfg.output_dir / "outputs" / "plots"
        self.ts_cp_path = cfg.output_dir / "checkpoints" / "timeseries_state.json"

    def run(self) -> None:
        from .checkpoint import load_ts_state

        if not _MPL_OK:
            self.log.warning(
                "matplotlib indisponível — geração de gráficos ignorada. "
                "Export tabular permanece intacto."
            )
            return

        ts_state = load_ts_state(self.ts_cp_path)
        sample_cities = ts_state.get("sample_cities", list(self.ts_cfg.sample_cities))

        if not sample_cities:
            self.log.warning(
                "Nenhuma cidade amostra disponível para plotar. "
                "Execute o módulo de séries temporais primeiro ou configure "
                "'timeseries.sample_cities' no config.yaml."
            )
            return

        self.log.info(
            "Iniciando geração de gráficos para %d variáveis × %d cidades.",
            len(self.ts_cfg.variables), len(sample_cities),
        )

        index_entries = generate_city_plots(
            yearly_dir=self.yearly_dir,
            plots_dir=self.plots_dir,
            sample_cities=sample_cities,
            variables=self.ts_cfg.variables,
            color_e=self.cfg.plot_color_E,
            color_f=self.cfg.plot_color_F,
            log=self.log,
            downsample=self.ts_cfg.downsample_for_global_plot,
        )

        # Visão global para as principais variáveis (precipitação e temperatura)
        global_vars = [
            "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
            "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
        ]
        for gvar in global_vars:
            if gvar in self.ts_cfg.variables:
                out = generate_global_overview(
                    yearly_dir=self.yearly_dir,
                    plots_dir=self.plots_dir,
                    sample_cities=sample_cities,
                    variable=gvar,
                    color_e=self.cfg.plot_color_E,
                    color_f=self.cfg.plot_color_F,
                    downsample=self.ts_cfg.downsample_for_global_plot,
                    log=self.log,
                )
                if out:
                    index_entries.append({
                        "file": Path(out).name,
                        "variable": gvar,
                        "city": "regional",
                        "type": "global_overview",
                    })

        write_plot_index(self.plots_dir, index_entries, self.log)
        self.log.info(
            "Geração de gráficos concluída. %d figuras produzidas.", len(index_entries)
        )
