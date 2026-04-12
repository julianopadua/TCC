# src/article/viz/sections/multi_year.py
"""Secção: vários anos (análise mais pesada; limite de anos)."""
from __future__ import annotations

import logging
from typing import List

import pandas as pd
import streamlit as st

from config import MAX_MULTI_YEARS, SYNTH_PRECIP_CUM_COL

from src.article.config import ArticlePipelineConfig
from src.article.eda import compute_correlations
from src.article.viz.config_paths import discover_years_for_scenario, list_scenario_keys, parquet_path
from src.article.viz.data_loader import (
    concat_years,
    estimate_rows_warning,
    filter_cities,
    list_cities_in_parquet,
    load_parquet_columns,
    parquet_column_names,
)
from src.article.viz.foco_details import build_foco_events_table, count_focos
from src.article.viz.plots import build_timeseries_figure, labels_for_selected
from src.article.viz.variables import (
    CITY_COL,
    LABEL_COL,
    METEO_REGISTRY,
    biomass_columns_in_df,
    meteo_slugs,
    resolve_columns_to_load,
    apply_precip_cumulative,
)

_LOG = logging.getLogger("article.viz")


def render_multi_year_page(cfg: ArticlePipelineConfig) -> None:
    st.header("Vários anos")
    st.caption(
        f"Até **{MAX_MULTI_YEARS}** anos por execução. Concatena séries no tempo; pode ser pesado — "
        "reduza cidades se necessário."
    )

    keys = list_scenario_keys(cfg)
    if not keys:
        st.error("Nenhum cenário em article_pipeline.scenarios.")
        return

    scenario_key = st.selectbox(
        "Base (cenário)",
        options=keys,
        format_func=lambda k: f"{k} — {cfg.scenarios[k]}",
        key="multi_scenario",
    )

    years_avail = discover_years_for_scenario(cfg, scenario_key)
    if not years_avail:
        st.warning("Nenhum Parquet encontrado para este cenário.")
        return

    year_default = years_avail[: min(3, len(years_avail))]
    sel_years = st.multiselect(
        "Anos (máx. %d)" % MAX_MULTI_YEARS,
        options=years_avail,
        default=year_default,
        key="multi_years",
    )
    sel_years = sorted(sel_years)
    if len(sel_years) > MAX_MULTI_YEARS:
        st.error(f"Selecione no máximo {MAX_MULTI_YEARS} anos.")
        return
    if not sel_years:
        st.info("Escolha pelo menos um ano.")
        return

    pq0 = parquet_path(cfg, scenario_key, sel_years[0])
    pq0_str = str(pq0.resolve())
    if not pq0.is_file():
        st.error(f"Ficheiro em falta: {pq0}")
        return

    all_cols = list(parquet_column_names(pq0_str))
    biomass_opts = biomass_columns_in_df(all_cols)

    c1, c2 = st.columns(2)
    with c1:
        meteo_sel = st.multiselect(
            "Variáveis meteorológicas",
            options=meteo_slugs(),
            format_func=lambda s: METEO_REGISTRY[s][0],
            default=[s for s in ("precip", "rad") if METEO_REGISTRY[s][1] in all_cols],
            key="multi_meteo",
        )
    with c2:
        bio_sel = st.multiselect(
            "Biomassa (NDVI/EVI)",
            options=biomass_opts,
            default=[],
            key="multi_bio",
        )

    incl_foco = st.checkbox("Incluir HAS_FOCO no gráfico", value=True, key="multi_foco_cb")
    foco_mode = st.radio(
        "Modo HAS_FOCO",
        options=["markers", "secondary", "none"],
        format_func=lambda x: {
            "markers": "Marcadores",
            "secondary": "Eixo secundário (1 cidade)",
            "none": "Não mostrar",
        }[x],
        horizontal=True,
        key="multi_foco_mode",
    )
    if foco_mode != "none" and LABEL_COL not in all_cols:
        st.warning("Coluna HAS_FOCO ausente.")
        incl_foco = False
        foco_mode = "none"

    show_foco_meta = False
    if LABEL_COL in all_cols:
        show_foco_meta = st.checkbox("Metadados de focos (total + tabela)", value=True, key="multi_foco_meta")

    use_all_cities = st.checkbox("Todas as cidades", value=False, key="multi_allc")
    city_list = list_cities_in_parquet(pq0_str)
    if not city_list:
        st.error("Não foi possível listar cidades.")
        return

    if use_all_cities:
        selected_cities = None
        if len(city_list) > 25:
            st.warning("Muitas cidades × vários anos pode exceder memória. Considere filtrar.")
    else:
        selected_cities = st.multiselect("Cidades", options=city_list, default=city_list[:1], key="multi_cities")
        if not selected_cities:
            st.info("Escolha cidades ou «Todas».")
            return

    need_foco_chart = incl_foco and foco_mode != "none"
    include_has_foco = need_foco_chart or show_foco_meta

    col_tuple = tuple(
        sorted(
            resolve_columns_to_load(
                meteo_sel,
                bio_sel,
                include_has_foco,
                all_cols,
            )
        )
    )
    if not col_tuple:
        st.error("Nenhuma coluna válida.")
        return

    frames: List[pd.DataFrame] = []
    for y in sel_years:
        pq = parquet_path(cfg, scenario_key, y)
        if not pq.is_file():
            st.warning(f"Falta ficheiro para {y}, ignorado.")
            continue
        part = load_parquet_columns(str(pq.resolve()), col_tuple)
        if part.empty:
            continue
        part = part.copy()
        part["_viz_year"] = y
        frames.append(part)

    df = concat_years(frames)
    if df.empty:
        st.error("Nada carregado.")
        return

    df = filter_cities(df, CITY_COL, selected_cities, use_all_cities)
    warn = estimate_rows_warning(len(df))
    if warn:
        st.warning(warn)

    df = apply_precip_cumulative(df, meteo_sel, multi_year=True)

    cont_cols: List[str] = []
    for slug in meteo_sel:
        if slug == "precip_cum":
            if SYNTH_PRECIP_CUM_COL in df.columns:
                cont_cols.append(SYNTH_PRECIP_CUM_COL)
            continue
        if slug in METEO_REGISTRY:
            c = METEO_REGISTRY[slug][1]
            if c in df.columns:
                cont_cols.append(c)
    for c in bio_sel:
        if c in df.columns:
            cont_cols.append(c)

    labels = labels_for_selected(meteo_sel, bio_sel)
    span = f"{sel_years[0]}–{sel_years[-1]}" if len(sel_years) > 1 else str(sel_years[0])

    fig = build_timeseries_figure(
        df,
        scenario_key=scenario_key,
        year_label=span,
        continuous_cols=cont_cols,
        col_labels=labels,
        has_foco_mode=("none" if not need_foco_chart else foco_mode),  # type: ignore[arg-type]
    )
    st.plotly_chart(fig, use_container_width=True)

    if show_foco_meta and LABEL_COL in df.columns:
        st.subheader("Focos de incêndio (seleção atual)")
        n = count_focos(df)
        st.metric("Total de registos com foco (HAS_FOCO=1)", n)
        tbl = build_foco_events_table(df, max_rows=2000)
        if tbl.empty:
            st.caption("Nenhum evento com foco nos dados filtrados.")
        else:
            if use_all_cities:
                st.caption("«Todas as cidades»: tabela limitada às primeiras 2000 linhas; filtre cidades para o detalhe completo.")
            st.dataframe(tbl, use_container_width=True, height=320)

    st.subheader("Correlações (estilo EDA)")
    show_corr = st.checkbox(
        "Calcular correlação biomassa × soma de focos (agregado por semana ou dia)",
        value=bool(bio_sel),
    )
    if show_corr and bio_sel:
        agg = st.selectbox("Agregação", options=["week", "day", "month"], index=0)
        corr_df = compute_correlations(df, bio_sel, scenario_key, _LOG, aggregation=agg)
        if corr_df.empty:
            st.info("Sem correlações (dados insuficientes ou sem HAS_FOCO/biomassa).")
        else:
            st.dataframe(corr_df, use_container_width=True)
    elif show_corr and not bio_sel:
        st.info("Selecione colunas de biomassa para correlações.")

    with st.expander("Metadados"):
        st.write({"linhas": len(df), "anos": sel_years, "cenário": scenario_key})
