# src/article/viz/plots.py
"""Figuras Plotly para séries temporais + HAS_FOCO."""
from __future__ import annotations

from typing import Dict, List, Literal, Optional, Sequence

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from src.article.viz.variables import CITY_COL, LABEL_COL, METEO_REGISTRY, TS_COL

HasFocoMode = Literal["none", "markers", "secondary"]


def _ts_series(df: pd.DataFrame) -> pd.Series:
    return pd.to_datetime(df[TS_COL])


def build_timeseries_figure(
    df: pd.DataFrame,
    scenario_key: str,
    year_label: str,
    continuous_cols: List[str],
    col_labels: Dict[str, str],
    has_foco_mode: HasFocoMode = "none",
) -> go.Figure:
    """
    continuous_cols: nomes reais de colunas no DataFrame.
    col_labels: col_name -> texto da legenda.
    """
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="Sem dados para os filtros selecionados.", height=400)
        return fig

    if not continuous_cols:
        if has_foco_mode != "none" and LABEL_COL in df.columns:
            df = df.copy()
            df["_ts"] = _ts_series(df)
            df = df.sort_values("_ts")
            fig = go.Figure(
                go.Scatter(
                    x=df["_ts"],
                    y=df[LABEL_COL],
                    mode="lines",
                    name="HAS_FOCO",
                    line=dict(width=1),
                )
            )
            fig.update_layout(
                title=f"{scenario_key} · {year_label} — HAS_FOCO",
                height=480,
                yaxis_range=[-0.05, 1.05],
            )
            return fig
        fig = go.Figure()
        fig.update_layout(title="Selecione pelo menos uma variável contínua ou HAS_FOCO.", height=400)
        return fig

    df = df.copy()
    df["_ts"] = _ts_series(df)
    df = df.sort_values("_ts")

    cities = df[CITY_COL].dropna().unique().tolist()
    n_cities = len(cities)

    if n_cities == 0:
        fig = go.Figure()
        fig.update_layout(title="Sem cidades nos dados.", height=400)
        return fig

    show_legend = True

    if n_cities == 1:
        city = cities[0]
        cdf = df[df[CITY_COL] == city]
        fig = go.Figure()
        for col in continuous_cols:
            if col not in cdf.columns:
                continue
            label = col_labels.get(col, col)
            fig.add_trace(
                go.Scatter(
                    x=cdf["_ts"],
                    y=cdf[col],
                    mode="lines",
                    name=f"{city} — {label}",
                    connectgaps=False,
                    opacity=0.85,
                )
            )
        if has_foco_mode != "none" and LABEL_COL in cdf.columns:
            fires = cdf[cdf[LABEL_COL] == 1]
            if has_foco_mode == "markers" and not fires.empty:
                y0 = float(cdf[continuous_cols[0]].min()) if continuous_cols else 0.0
                fig.add_trace(
                    go.Scatter(
                        x=fires["_ts"],
                        y=[y0] * len(fires),
                        mode="markers",
                        name="HAS_FOCO=1",
                        marker=dict(symbol="line-ns-open", size=12, color="red"),
                        legendgroup="has_foco",
                    )
                )
            elif has_foco_mode == "secondary":
                fig.add_trace(
                    go.Scatter(
                        x=cdf["_ts"],
                        y=cdf[LABEL_COL],
                        mode="lines",
                        name="HAS_FOCO",
                        line=dict(width=1, dash="dot", color="firebrick"),
                        yaxis="y2",
                    )
                )
                fig.update_layout(
                    yaxis2=dict(
                        title="HAS_FOCO",
                        overlaying="y",
                        side="right",
                        range=[-0.05, 1.05],
                        showgrid=False,
                    )
                )
        fig.update_layout(
            title=f"{scenario_key} · {year_label} · {city}",
            xaxis_title="Tempo",
            yaxis_title="Valor",
            height=520,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            hovermode="x unified",
        )
        return fig

    # Múltiplas cidades: um subplot por cidade
    fig = make_subplots(
        rows=n_cities,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.04,
        subplot_titles=[str(c) for c in cities],
    )
    color_cycle = (
        "#1f77b4",
        "#ff7f0e",
        "#2ca02c",
        "#d62728",
        "#9467bd",
        "#8c564b",
        "#e377c2",
        "#7f7f7f",
    )
    for ri, city in enumerate(cities, start=1):
        cdf = df[df[CITY_COL] == city]
        for ci, col in enumerate(continuous_cols):
            if col not in cdf.columns:
                continue
            label = col_labels.get(col, col)
            color = color_cycle[ci % len(color_cycle)]
            fig.add_trace(
                go.Scatter(
                    x=cdf["_ts"],
                    y=cdf[col],
                    mode="lines",
                    name=f"{label}" if ri == 1 else None,
                    legendgroup=col,
                    showlegend=(ri == 1),
                    line=dict(width=1.0, color=color),
                    connectgaps=False,
                ),
                row=ri,
                col=1,
            )
        if has_foco_mode in ("markers", "secondary") and LABEL_COL in cdf.columns:
            # Várias cidades: sempre marcadores (eixo secundário por painel não suportado aqui).
            fires = cdf[cdf[LABEL_COL] == 1]
            if not fires.empty and continuous_cols:
                ref = float(cdf[continuous_cols[0]].min())
                fig.add_trace(
                    go.Scatter(
                        x=fires["_ts"],
                        y=[ref] * len(fires),
                        mode="markers",
                        name="HAS_FOCO=1" if ri == 1 else None,
                        legendgroup="has_foco",
                        showlegend=(ri == 1),
                        marker=dict(symbol="line-ns-open", size=10, color="red"),
                    ),
                    row=ri,
                    col=1,
                )

    fig.update_layout(
        title=f"{scenario_key} · {year_label}",
        height=min(300 * n_cities, 2400),
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    fig.update_xaxes(title_text="Tempo", row=n_cities, col=1)
    return fig


def labels_for_selected(
    selected_meteo_slugs: Sequence[str],
    selected_biomass: Sequence[str],
) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for slug in selected_meteo_slugs:
        if slug in METEO_REGISTRY:
            short, col = METEO_REGISTRY[slug]
            out[col] = short
    for c in selected_biomass:
        out[c] = c.replace("_", " ")
    return out
