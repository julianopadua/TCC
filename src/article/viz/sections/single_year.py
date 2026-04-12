# src/article/viz/sections/single_year.py
"""Secção: exatamente um ano (exploração leve)."""
from __future__ import annotations

from typing import List

import streamlit as st

from config import SYNTH_PRECIP_CUM_COL

from src.article.config import ArticlePipelineConfig
from src.article.viz.config_paths import discover_years_for_scenario, list_scenario_keys, parquet_path
from src.article.viz.data_loader import (
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


def render_single_year_page(cfg: ArticlePipelineConfig) -> None:
    st.header("Um ano")
    st.caption(
        "Selecione uma base (cenário), um ano, cidades e variáveis. "
        "Com várias cidades, o gráfico usa um painel por cidade."
    )

    keys = list_scenario_keys(cfg)
    if not keys:
        st.error("Nenhum cenário em article_pipeline.scenarios.")
        return

    scenario_key = st.selectbox("Base (cenário)", options=keys, format_func=lambda k: f"{k} — {cfg.scenarios[k]}")

    years = discover_years_for_scenario(cfg, scenario_key)
    if not years:
        st.warning("Nenhum Parquet encontrado para este cenário.")
        return

    year = st.selectbox("Ano", options=years, index=min(len(years) - 1, 0))

    pq_path = parquet_path(cfg, scenario_key, year)
    pq_str = str(pq_path.resolve())

    if not pq_path.is_file():
        st.error(f"Ficheiro em falta: {pq_path}")
        return

    all_cols = list(parquet_column_names(pq_str))
    biomass_opts = biomass_columns_in_df(all_cols)

    c1, c2 = st.columns(2)
    with c1:
        meteo_sel = st.multiselect(
            "Variáveis meteorológicas",
            options=meteo_slugs(),
            format_func=lambda s: METEO_REGISTRY[s][0],
            default=[s for s in ("precip", "rad") if METEO_REGISTRY[s][1] in all_cols],
        )
    with c2:
        bio_sel = st.multiselect(
            "Biomassa (NDVI/EVI)",
            options=biomass_opts,
            default=[],
        )

    incl_foco = st.checkbox("Incluir HAS_FOCO no gráfico", value=True)
    foco_mode = st.radio(
        "Modo HAS_FOCO",
        options=["markers", "secondary", "none"],
        format_func=lambda x: {
            "markers": "Marcadores (recomendado com várias cidades)",
            "secondary": "Eixo Y secundário (melhor com 1 cidade)",
            "none": "Não mostrar",
        }[x],
        horizontal=True,
    )
    if foco_mode != "none" and LABEL_COL not in all_cols:
        st.warning("Coluna HAS_FOCO ausente neste Parquet.")
        incl_foco = False
        foco_mode = "none"

    show_foco_meta = False
    if LABEL_COL in all_cols:
        show_foco_meta = st.checkbox("Metadados de focos (total + tabela)", value=True)

    use_all_cities = st.checkbox("Todas as cidades", value=False)
    city_list = list_cities_in_parquet(pq_str)
    if not city_list:
        st.error("Não foi possível listar cidades.")
        return

    if use_all_cities:
        selected_cities: List[str] | None = None
        st.caption(f"**{len(city_list)}** cidades — gráficos em painéis empilhados.")
        if len(city_list) > 40:
            st.warning("Muitas cidades: o carregamento pode ser lento. Considere filtrar.")
    else:
        selected_cities = st.multiselect("Cidades", options=city_list, default=city_list[:1])
        if not selected_cities:
            st.info("Escolha pelo menos uma cidade ou marque «Todas as cidades».")
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
        st.error("Nenhuma coluna válida para carregar.")
        return

    df = load_parquet_columns(pq_str, col_tuple)
    if df.empty:
        st.error("DataFrame vazio após leitura.")
        return

    df = filter_cities(df, CITY_COL, selected_cities, use_all_cities)
    warn = estimate_rows_warning(len(df))
    if warn:
        st.warning(warn)

    df = apply_precip_cumulative(df, meteo_sel, multi_year=False)

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

    fig = build_timeseries_figure(
        df,
        scenario_key=scenario_key,
        year_label=str(year),
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

    with st.expander("Metadados"):
        st.write({"linhas": len(df), "ficheiro": pq_str, "colunas_carregadas": list(col_tuple)})
