# src/webapp/app.py
# =============================================================================
# WebApp — Análise Exploratória das Bases (INMET & BDQueimadas)
# =============================================================================
from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

# utilitários centrais do projeto
from utils import loadConfig, get_path

# dicionário de metadados das colunas (descrições)
from webapp.columns_meta import INMET_COLS_DESC, BDQ_COLS_DESC

st.set_page_config(
    page_title="TCC — WebApp de Análise (INMET / BDQueimadas)",
    layout="wide",
)

# -----------------------------------------------------------------------------
# Helpers de descoberta de datasets
# -----------------------------------------------------------------------------
def _list_inmet_processed_files() -> Dict[str, Path]:
    base = get_path("paths", "providers", "inmet", "processed")
    base = Path(base)
    files = sorted(base.glob("inmet_*.csv"))
    out: Dict[str, Path] = {}
    for fp in files:
        m = re.search(r"inmet_(\d{4})\.csv$", fp.name, re.I)
        if m:
            label = f"INMET {m.group(1)}"
        else:
            label = fp.stem
        out[label] = fp
    return out


def _list_bdq_processed_files() -> Dict[str, Path]:
    """
    Tenta listar arquivos já processados primeiro; se vazio, lista do raw com padrão de nome do projeto.
    """
    proc = Path(get_path("paths", "providers", "bdqueimadas", "processed"))
    raw = Path(get_path("paths", "providers", "bdqueimadas", "raw"))
    candidates = list(proc.glob("*.csv"))
    if not candidates:
        candidates = list(raw.glob("exportador_*_ref_*.csv"))
    out: Dict[str, Path] = {}
    for fp in sorted(candidates):
        # extrai ano de referência, se houver
        m = re.search(r"_ref_(\d{4})\.csv$", fp.name)
        label = f"BDQueimadas {m.group(1)}" if m else f"BDQueimadas — {fp.stem}"
        out[label] = fp
    return out


# -----------------------------------------------------------------------------
# Carregamento com cache
# -----------------------------------------------------------------------------
@st.cache_data(show_spinner=False)
def load_inmet_df(path: Path) -> pd.DataFrame:
    """
    Lê um CSV consolidado do INMET (inmet_{year}.csv) e aplica limpeza mínima:
      - gera coluna DATETIME a partir de DATA (YYYY-MM-DD) + HORA (UTC);
      - converte colunas numéricas de string (inclusive com decimal ',') para float;
      - substitui sentinela -9999 por NaN em colunas numéricas.
    """
    df = pd.read_csv(path, encoding="utf-8")
    # normaliza nomes (sem mudar os originais; guardamos cópias)
    col_date = "DATA (YYYY-MM-DD)"
    col_time = "HORA (UTC)"

    # cria DATETIME
    if col_date in df.columns and col_time in df.columns:
        # pandas aceita datetime para duas colunas concatenadas
        dt = pd.to_datetime(df[col_date].astype(str) + " " + df[col_time].astype(str), errors="coerce", utc=True)
        df["DATETIME"] = dt
    elif col_date in df.columns:
        df["DATETIME"] = pd.to_datetime(df[col_date], errors="coerce", utc=True)
    else:
        df["DATETIME"] = pd.NaT

    # tenta converter toda coluna numérica representada como string
    def _to_float_series(s: pd.Series) -> pd.Series:
        if s.dtype.kind in "biufc":
            return s.astype(float)
        # remove aspas e troca vírgula decimal por ponto
        if s.dtype == object:
            return pd.to_numeric(s.astype(str).str.replace('"', "", regex=False).str.replace(",", ".", regex=False), errors="coerce")
        return pd.to_numeric(s, errors="coerce")

    numeric_candidates = [c for c in df.columns if c not in {col_date, col_time, "CIDADE"}]
    for c in numeric_candidates:
        df[c] = _to_float_series(df[c])

    # sentinela
    df = df.replace(-9999, np.nan)

    return df


@st.cache_data(show_spinner=False)
def load_bdq_df(path: Path) -> pd.DataFrame:
    """
    Lê um CSV do BDQueimadas e aplica limpeza mínima:
      - parse de DataHora;
      - cast numérico em colunas adequadas;
      - normalização de sentinelas (-999, -9999).
    """
    # tentativa robusta de encoding
    try:
        df = pd.read_csv(path, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, encoding="latin1")

    # DataHora
    if "DataHora" in df.columns:
        # formatos comuns do TerraBrasilis: "YYYY/MM/DD HH:MM:SS"
        df["DataHora"] = pd.to_datetime(df["DataHora"], errors="coerce")
    elif "datahora" in df.columns:
        df["DataHora"] = pd.to_datetime(df["datahora"], errors="coerce")
    else:
        df["DataHora"] = pd.NaT

    # cast de numéricos
    num_cols = [c for c in df.columns if c.lower() in {"riscofogo", "frp", "precipitacao", "diasemchuva", "latitude", "longitude"}]
    for c in num_cols:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # sentinelas
    df = df.replace([-999, -9999], np.nan)
    return df


# -----------------------------------------------------------------------------
# Métricas básicas e outliers
# -----------------------------------------------------------------------------
def basic_stats(df: pd.DataFrame, datetime_col: str) -> Dict[str, str]:
    n = len(df)
    # faixa temporal
    if datetime_col in df.columns:
        dtmin = pd.to_datetime(df[datetime_col], errors="coerce").min()
        dtmax = pd.to_datetime(df[datetime_col], errors="coerce").max()
    else:
        dtmin = dtmax = pd.NaT
    return {
        "linhas": f"{n:,}".replace(",", "."),
        "início": str(dtmin) if pd.notna(dtmin) else "-",
        "fim": str(dtmax) if pd.notna(dtmax) else "-",
    }


def iqr_outliers(s: pd.Series) -> Tuple[pd.Series, float, float]:
    """Retorna máscara de outliers pelo critério IQR, e os limites inferior/superior."""
    x = pd.to_numeric(s, errors="coerce").dropna()
    if x.empty:
        return pd.Series([False] * len(s), index=s.index), np.nan, np.nan
    q1, q3 = np.percentile(x, [25, 75])
    iqr = q3 - q1
    lo = q1 - 1.5 * iqr
    hi = q3 + 1.5 * iqr
    mask = (pd.to_numeric(s, errors="coerce") < lo) | (pd.to_numeric(s, errors="coerce") > hi)
    return mask.fillna(False), lo, hi


# -----------------------------------------------------------------------------
# UI
# -----------------------------------------------------------------------------
st.title("WebApp — Análise de Bases (INMET / BDQueimadas)")

with st.sidebar:
    st.header("Seleção da base")
    fonte = st.selectbox("Origem", ["INMET", "BDQueimadas"], index=0)

    if fonte == "INMET":
        datasets = _list_inmet_processed_files()
    else:
        datasets = _list_bdq_processed_files()

    if not datasets:
        st.error("Nenhum dataset encontrado. Verifique as pastas configuradas no config.yaml.")
        st.stop()

    label = st.selectbox("Dataset", list(datasets.keys()))
    arquivo = datasets[label]

    st.caption(f"Arquivo selecionado: {arquivo}")

# Carrega dataframe
if fonte == "INMET":
    df = load_inmet_df(arquivo)
    datetime_col = "DATETIME"
    default_metric_col = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
    cols_desc = INMET_COLS_DESC
else:
    df = load_bdq_df(arquivo)
    datetime_col = "DataHora"
    default_metric_col = "Precipitacao" if "Precipitacao" in df.columns else "Precipitação" if "Precipitação" in df.columns else "FRP"
    cols_desc = BDQ_COLS_DESC

# Preview
st.subheader("Prévia dos dados")
st.dataframe(df.head(50), use_container_width=True, height=300)

# Métricas básicas
st.subheader("Informações gerais")
stats = basic_stats(df, datetime_col)
c1, c2, c3 = st.columns(3)
c1.metric("Linhas", stats["linhas"])
c2.metric("Início", stats["início"])
c3.metric("Fim", stats["fim"])

# Descrições de colunas
with st.expander("Dicionário de colunas"):
    left, right = st.columns(2)
    with left:
        st.write("Colunas disponíveis:")
        st.write(", ".join(df.columns))
    with right:
        st.write("Descrições conhecidas:")
        desc_table = pd.DataFrame(
            [{"coluna": k, "descrição": v} for k, v in cols_desc.items() if k in df.columns]
        )
        if desc_table.empty:
            st.info("Sem descrições mapeadas para estas colunas.")
        else:
            st.dataframe(desc_table, use_container_width=True, hide_index=True)

# Análise de uma coluna
st.subheader("Estatísticas de coluna")
num_cols = [c for c in df.columns if pd.api.types.is_numeric_dtype(df[c]) or c == default_metric_col]
if not num_cols:
    st.info("Nenhuma coluna numérica detectada para análise.")
else:
    col_sel = st.selectbox("Selecione a coluna para analisar:", options=sorted(set(num_cols)),
                           index=sorted(set(num_cols)).index(default_metric_col) if default_metric_col in num_cols else 0)

    series = pd.to_numeric(df[col_sel], errors="coerce")
    valid = series.dropna()

    # Métricas simples
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("n válidos", f"{valid.size:,}".replace(",", "."))
    c2.metric("média", f"{valid.mean():.3f}" if valid.size else "-")
    c3.metric("mediana", f"{valid.median():.3f}" if valid.size else "-")
    c4.metric("desvio padrão", f"{valid.std():.3f}" if valid.size else "-")

    # Outliers
    mask, lo, hi = iqr_outliers(series)
    n_out = int(mask.sum())
    st.markdown(f"**Outliers (IQR):** {n_out} | limites: [{lo:.3f}, {hi:.3f}]") if np.isfinite(lo) and np.isfinite(hi) else st.markdown("**Outliers (IQR):** não aplicável")

    # Gráficos
    tab1, tab2 = st.tabs(["Histograma", "Boxplot"])
    with tab1:
        fig = px.histogram(valid, x=valid, nbins=50, title=f"Histograma — {col_sel}")
        st.plotly_chart(fig, use_container_width=True)
    with tab2:
        fig2 = px.box(valid, y=valid, points="outliers", title=f"Boxplot — {col_sel}")
        st.plotly_chart(fig2, use_container_width=True)

    # Tabela de outliers
    if n_out > 0:
        st.markdown("**Registros outliers (primeiros 200):**")
        out_df = df.loc[mask, [datetime_col, col_sel]].copy()
        out_df = out_df.sort_values(col_sel, key=lambda s: s.abs(), ascending=False).head(200)
        st.dataframe(out_df, use_container_width=True, height=300)

st.caption("Versão inicial — métricas básicas. Próximas iterações: perfis por estação/UF/bioma, mapas, correlações cruzadas e validações de qualidade dos dados.")
