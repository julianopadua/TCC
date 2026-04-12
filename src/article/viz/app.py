# src/article/viz/app.py
"""
Visualização interativa das bases do artigo (Parquets em data/_article).

Executar a partir da raiz do repositório:

    streamlit run src/article/viz/app.py

Requer: config.yaml com bloco article_pipeline e dados em
``data/_article/0_datasets_with_coords/<cenário>/``.

A navegação «Um ano» / «Vários anos» está no corpo principal (tabs).
A pasta de secções chama-se ``sections/`` para não ativar o multipage automático do Streamlit (``pages/``).
"""
from __future__ import annotations

import sys
from pathlib import Path

# Raiz do projeto (pai de ``src``)
_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import streamlit as st

from src.article.viz.config_paths import get_config
from src.article.viz.sections.multi_year import render_multi_year_page
from src.article.viz.sections.single_year import render_single_year_page


def main() -> None:
    st.set_page_config(
        page_title="Artigo — Visualização",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    st.sidebar.title("Artigo — dados")
    st.sidebar.markdown("Exploração dos Parquets com coordenadas e biomassa (GEE).")

    try:
        cfg = get_config()
    except Exception as e:
        st.error(f"Erro ao carregar config: {e}")
        st.stop()

    tab_one, tab_multi = st.tabs(["Um ano", "Vários anos"])
    with tab_one:
        render_single_year_page(cfg)
    with tab_multi:
        render_multi_year_page(cfg)


if __name__ == "__main__":
    main()
