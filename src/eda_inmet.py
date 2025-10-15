# =============================================================================
# EDA/LIMPEZA INICIAL - INMET (consolidated/inmet_all_years.csv)
# Le incrementalmente (chunks) e gera:
#  - Estatisticas descritivas agregadas (numericas e categoricas)
#  - Amostra (reservoir sampling) para visualizacoes
#  - Visualizacoes: hist, dispersoes, correlacao, missingness
#  - Relatorios CSV/JSON
# Saidas em:
#   processed/EDA/INMET/
#   images/eda/inmet/
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Tuple
import json
import math
import random
import itertools
import argparse
import re
import csv
import sys

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
)

# aumenta o limite do tamanho de campo do parser csv (relevante para engine="python")
try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2**31 - 1)

# ------------------------------ Configs padrao -------------------------------

DEFAULT_CHUNKSIZE = 250_000           # ajuste conforme RAM
RESERVOIR_TARGET_ROWS = 300_000       # amostra para graficos
TOPK_CATEGORICAL = 15
MAX_HISTS = 12
MAX_SCATTER_PAIRS = 8
RANDOM_SEED = 42
BLOCK_ROWS = 2_500_000                # 2.5M linhas por bloco de estudo

# --------------------------- Pastas e arquivos alvo --------------------------

def get_consolidated_csv_path() -> Path:
    # data.external = ./data/consolidated
    consolidated_dir = get_path("paths", "data", "external")
    return Path(consolidated_dir) / "INMET" / "inmet_all_years.csv"

def get_reports_dir() -> Path:
    base = get_path("paths", "data", "processed")
    return ensure_dir(Path(base) / "EDA" / "INMET")

def get_images_dir() -> Path:
    base = get_path("paths", "images")
    return ensure_dir(Path(base) / "eda" / "inmet")

# ---------------------------- Helpers de sanitizacao -------------------------

_PROHIBITED = re.compile(r'[\\/:*?"<>|\n\r\t]+')

def sanitize_filename(name: str) -> str:
    """
    Remove caractere proibido e troca barra/contrabarra etc. por underscore.
    Tambem remove explicitamente o caractere em dash, se houver.
    """
    # remove em dash proibido
    name = name.replace("—", "")
    # troca barras e afins por underscore
    name = _PROHIBITED.sub("_", name)
    # compacta underscores repetidos
    name = re.sub(r"_+", "_", name).strip(" _")
    return name

# ---------------------------- Schema canonico --------------------------------
# Nomes exatamente como no seu exemplo de cabecalho
COL_DATA = "DATA (YYYY-MM-DD)"
COL_HORA = "HORA (UTC)"

NUM_COLS = [
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
    "RADIACAO GLOBAL (KJ/m²)",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))",
    "VENTO, RAJADA MAXIMA (m/s)",
    "VENTO, VELOCIDADE HORARIA (m/s)",
    "ANO",
    "LATITUDE",
    "LONGITUDE",
]

CAT_COLS = [
    "CIDADE",
]

ALL_COLS = [COL_DATA, COL_HORA] + NUM_COLS + CAT_COLS

# Sentinela de ausente
NA_SENTINELS = ["-9999", -9999]

# Dtypes sugeridos para reduzir DtypeWarning (engine='c' + decimal=',')
DTYPE_MAP: Dict[str, str] = {
    COL_DATA: "string",
    COL_HORA: "string",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "float64",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "float64",
    "RADIACAO GLOBAL (KJ/m²)": "float64",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "float64",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "float64",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "float64",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "float64",
    "VENTO, RAJADA MAXIMA (m/s)": "float64",
    "VENTO, VELOCIDADE HORARIA (m/s)": "float64",
    "ANO": "float64",        # le como float e depois converte suave
    "LATITUDE": "float64",
    "LONGITUDE": "float64",
    "CIDADE": "string",
}

# ---------------------------- Agregadores on-line ----------------------------

class RunningStats:
    """Estatisticas on-line (Welford) por coluna numerica."""
    __slots__ = ("n", "mean", "M2", "minv", "maxv", "n_missing")

    def __init__(self) -> None:
        self.n = 0
        self.mean = 0.0
        self.M2 = 0.0
        self.minv = math.inf
        self.maxv = -math.inf
        self.n_missing = 0

    def update_series(self, s: pd.Series) -> None:
        m = s.isna().sum()
        self.n_missing += int(m)
        s = s.dropna()
        if s.empty:
            return
        vals = s.astype(float).values
        self.minv = min(self.minv, float(np.nanmin(vals)))
        self.maxv = max(self.maxv, float(np.nanmax(vals)))
        for x in vals:
            self.n += 1
            delta = x - self.mean
            self.mean += delta / self.n
            self.M2 += delta * (x - self.mean)

    def to_dict(self) -> Dict[str, Any]:
        var = self.M2 / (self.n - 1) if self.n > 1 else float("nan")
        std = math.sqrt(var) if var == var else float("nan")
        total = self.n + self.n_missing
        miss_pct = (self.n_missing / total * 100.0) if total else 0.0
        return {
            "count": self.n,
            "missing": self.n_missing,
            "missing_pct": miss_pct,
            "mean": self.mean if self.n > 0 else float("nan"),
            "std": std,
            "min": self.minv if self.minv != math.inf else float("nan"),
            "max": self.maxv if self.maxv != -math.inf else float("nan"),
        }

class TopKFreq:
    """Frequencia top-k para categoricas (strings/codigos), com cap de memoria."""
    __slots__ = ("freq", "k", "n_total", "n_missing")

    def __init__(self, k: int = TOPK_CATEGORICAL) -> None:
        self.freq: Dict[str, int] = {}
        self.k = k
        self.n_total = 0
        self.n_missing = 0

    def update_series(self, s: pd.Series) -> None:
        self.n_total += len(s)
        self.n_missing += int(s.isna().sum())
        for v in s.dropna().astype(str).values:
            self.freq[v] = self.freq.get(v, 0) + 1
            if len(self.freq) > (self.k * 40):
                self.freq = dict(sorted(self.freq.items(), key=lambda kv: kv[1], reverse=True)[: self.k * 20])

    def topk(self) -> List[Tuple[str, int]]:
        return sorted(self.freq.items(), key=lambda kv: kv[1], reverse=True)[: self.k]

    def to_dict(self) -> Dict[str, Any]:
        non_missing = self.n_total - self.n_missing
        return {
            "count": int(non_missing),
            "missing": int(self.n_missing),
            "missing_pct": (self.n_missing / self.n_total * 100.0) if self.n_total else 0.0,
            "top_values": [{"value": v, "count": int(c)} for v, c in self.topk()],
            "unique_approx": len(self.freq),
        }

# --------------------------------- EDA ---------------------------------------

def eda_inmet(
    chunksize: int = DEFAULT_CHUNKSIZE,
    reservoir_target: int = RESERVOIR_TARGET_ROWS,
    topk_cat: int = TOPK_CATEGORICAL,
    seed: int = RANDOM_SEED,
    viz_chunks: int = 1,  # quantidade de blocos de 2.5M linhas para o estudo
) -> Dict[str, Any]:
    cfg = loadConfig()
    log = get_logger("inmet.eda", kind="eda", per_run_file=True)
    random.seed(seed)
    np.random.seed(seed)

    csv_path = get_consolidated_csv_path()
    if not csv_path.exists():
        raise FileNotFoundError(f"Arquivo consolidado nao encontrado: {csv_path}")

    reports_dir = get_reports_dir()
    images_dir = get_images_dir()
    sample_out = reports_dir / "sample.parquet"
    colsum_out = reports_dir / "column_summary.csv"
    cattop_out = reports_dir / "categorical_top_values.json"

    # Agregadores (schema fixo)
    num_stats: Dict[str, RunningStats] = {c: RunningStats() for c in NUM_COLS}
    cat_stats: Dict[str, TopKFreq] = {c: TopKFreq(topk_cat) for c in CAT_COLS}

    # Amostra
    sample_batches: List[pd.DataFrame] = []

    # Limite de linhas a considerar para esta rodada de estudo
    viz_limit_rows = max(1, int(viz_chunks)) * BLOCK_ROWS
    log.info(f"[READ] Leitura incremental em chunks={chunksize}; limite para estudo = {viz_limit_rows:,} linhas")

    # Leitor incremental com dtypes e decimal fixos
    reader = pd.read_csv(
        csv_path,
        chunksize=chunksize,
        encoding=cfg.get("io", {}).get("encoding", "utf-8"),
        engine="c",              # rapido/estavel
        sep=",",
        decimal=",",             # trata "888,2" -> 888.2
        na_values=NA_SENTINELS,  # -9999 como NaN
        usecols=ALL_COLS,        # garante set de colunas
        dtype=DTYPE_MAP,         # reduz DtypeWarning
        low_memory=False,
    )

    total_rows = 0
    for i, chunk in enumerate(reader, 1):
        try:
            # corta se passar do limite de estudo
            if total_rows >= viz_limit_rows:
                break

            # se ultrapassar no final, corta o excesso para nao passar do limite
            remaining = viz_limit_rows - total_rows
            if len(chunk) > remaining:
                chunk = chunk.iloc[:remaining, :]

            # coercoes adicionais
            if "ANO" in chunk.columns:
                # mantem como float para evitar erro; estatisticas sao numericas
                chunk["ANO"] = pd.to_numeric(chunk["ANO"], errors="coerce")

            for c in NUM_COLS:
                if c in chunk.columns:
                    chunk[c] = pd.to_numeric(chunk[c], errors="coerce")

            for c in CAT_COLS:
                if c in chunk.columns:
                    chunk[c] = chunk[c].astype("string")

            # DATAHORA_UTC: combina DATA + HORA
            if (COL_DATA in chunk.columns) and (COL_HORA in chunk.columns):
                dt_str = chunk[COL_DATA].astype("string") + " " + chunk[COL_HORA].astype("string")
                chunk["DATAHORA_UTC"] = pd.to_datetime(
                    dt_str, format="%Y-%m-%d %H:%M", errors="coerce", utc=True
                )

            # Atualiza agregadores
            for c in NUM_COLS:
                if c in chunk.columns:
                    num_stats[c].update_series(chunk[c])

            for c in CAT_COLS:
                if c in chunk.columns:
                    cat_stats[c].update_series(chunk[c])

            # Amostragem (por chunk; heuristica ~50 lotes)
            take_n = min(len(chunk), max(1, int(reservoir_target / 50)))
            if take_n > 0:
                cols_for_sample = [*NUM_COLS, *CAT_COLS, COL_DATA, COL_HORA]
                cols_for_sample = [c for c in cols_for_sample if c in chunk.columns]
                df_chunk = chunk[cols_for_sample].sample(n=take_n, random_state=(seed + i))
                sample_batches.append(df_chunk)

            # Compactacao da amostra para o target
            cur_size = sum(len(x) for x in sample_batches)
            if cur_size > reservoir_target * 1.2:
                cat_df = pd.concat(sample_batches, ignore_index=True)
                sample_batches = [cat_df.sample(n=reservoir_target, random_state=seed)]

            total_rows += len(chunk)
            if i % 10 == 0:
                log.info(f"[CHUNK {i}] linhas acumuladas: {total_rows:,}")

        except Exception as e:
            log.exception(f"[WARN] Falha ao processar chunk {i}: {e}")
            continue

    # Amostra final
    if sample_batches:
        sample_df = pd.concat(sample_batches, ignore_index=True)
        if len(sample_df) > reservoir_target:
            sample_df = sample_df.sample(n=reservoir_target, random_state=seed)
    else:
        sample_df = pd.DataFrame(columns=[*NUM_COLS, *CAT_COLS, COL_DATA, COL_HORA])

    # Relatorios: column_summary + top values categoricas
    log.info("[REPORT] Montando resumo por coluna...")
    rows_out = []

    for c in NUM_COLS:
        base = {"column": c, "inferred_type": "numeric"}
        base.update(num_stats[c].to_dict())
        rows_out.append(base)

    for c in CAT_COLS:
        st = cat_stats[c].to_dict()
        rows_out.append({
            "column": c,
            "inferred_type": "category",
            "count": st["count"],
            "missing": st["missing"],
            "missing_pct": st["missing_pct"],
            "unique_approx": st["unique_approx"],
        })

    colsum_df = pd.DataFrame(rows_out)
    ensure_dir(colsum_out.parent)
    colsum_df.to_csv(colsum_out, index=False, encoding="utf-8")

    # Top-K categoricas -> JSON
    cat_json = {c: cat_stats[c].to_dict() for c in CAT_COLS}
    with open(cattop_out, "w", encoding="utf-8") as f:
        json.dump(cat_json, f, ensure_ascii=False, indent=2)

    # Salva amostra (Parquet) para analises futuras
    sample_df.to_parquet(sample_out, index=False, engine="pyarrow", compression="zstd")

    # ----------------------- Visualizacoes (amostra) --------------------------

    images_dir = get_images_dir()

    # Seleciona numericas com mais dados para plot
    num_cols_present = [c for c in NUM_COLS if c in sample_df.columns]
    num_nonnull_order = sorted(num_cols_present, key=lambda c: sample_df[c].notna().sum(), reverse=True)
    num_plot_cols = num_nonnull_order[:MAX_HISTS]

    # 1) Histogramas
    for c in num_plot_cols:
        plt.figure(figsize=(10, 6))
        sns.histplot(sample_df[c].dropna(), bins=50, kde=False, color="#3B82F6")
        title = f"Histograma - {c} - Base INMET (amostra, sem processamento)"
        plt.title(title)
        plt.xlabel(c)
        plt.ylabel("Frequencia")
        out = images_dir / f"{sanitize_filename(title)}.png"
        plt.tight_layout()
        plt.savefig(out, dpi=150)
        plt.close()

    # 2) Dispersoes (pares entre as 6 mais completas)
    scatter_pairs = list(itertools.combinations(num_nonnull_order[:min(6, len(num_nonnull_order))], 2))[:MAX_SCATTER_PAIRS]
    for x, y in scatter_pairs:
        df_xy = sample_df[[x, y]].dropna()
        if df_xy.empty:
            continue
        plt.figure(figsize=(8, 6))
        sns.scatterplot(
            data=df_xy.sample(min(50_000, len(df_xy)), random_state=RANDOM_SEED),
            x=x, y=y, s=8, alpha=0.3, edgecolor=None, color="#10B981"
        )
        title = f"Grafico de Dispersao - {x} vs {y} - Base INMET (amostra, sem processamento)"
        plt.title(title)
        plt.xlabel(x)
        plt.ylabel(y)
        out = images_dir / f"{sanitize_filename(title)}.png"
        plt.tight_layout()
        plt.savefig(out, dpi=150)
        plt.close()

    # 3) Correlacao (Pearson)
    if len(num_plot_cols) >= 2:
        corr = sample_df[num_plot_cols].corr(numeric_only=True, method="pearson")
        plt.figure(figsize=(1.0 + 0.6*len(num_plot_cols), 1.0 + 0.6*len(num_plot_cols)))
        sns.heatmap(corr, cmap="RdBu_r", vmin=-1, vmax=1, center=0, square=True)
        title = "Matriz de Correlacao (Pearson) - Base INMET (amostra, sem processamento)"
        plt.title(title)
        out = images_dir / f"{sanitize_filename(title)}.png"
        plt.tight_layout()
        plt.savefig(out, dpi=150)
        plt.close()

    # 4) Missingness por coluna (amostra)
    miss_pct = sample_df.isna().mean().sort_values(ascending=False) * 100.0
    if not miss_pct.empty:
        plt.figure(figsize=(max(10, len(miss_pct) * 0.25), 6))
        sns.barplot(x=miss_pct.values, y=miss_pct.index, color="#9CA3AF")
        plt.xlabel("% de valores ausentes")
        plt.ylabel("Colunas")
        title = "Proporcao de Valores Ausentes por Coluna - Base INMET (amostra, sem processamento)"
        plt.title(title)
        out = images_dir / f"{sanitize_filename(title)}.png"
        plt.tight_layout()
        plt.savefig(out, dpi=150)
        plt.close()

    log.info(f"[DONE] Linhas processadas no estudo: {total_rows:,}")
    log.info(f"[OUT] Resumo colunas: {colsum_out}")
    log.info(f"[OUT] Top categoricas: {cattop_out}")
    log.info(f"[OUT] Amostra: {sample_out}")
    log.info(f"[OUT] Imagens: {images_dir}")

    return {
        "rows_processed": total_rows,
        "column_summary_csv": str(colsum_out),
        "categorical_top_json": str(cattop_out),
        "sample_parquet": str(sample_out),
        "images_dir": str(images_dir),
    }

# ---------------------------------- MAIN -------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="EDA inicial da base consolidada do INMET usando leitura incremental."
    )
    p.add_argument(
        "--viz-chunks",
        type=int,
        default=1,
        help="Quantidade de blocos de 2.500.000 linhas a considerar para o estudo (default: 1).",
    )
    p.add_argument(
        "--chunksize",
        type=int,
        default=DEFAULT_CHUNKSIZE,
        help=f"Tamanho do chunk para leitura incremental (default: {DEFAULT_CHUNKSIZE}).",
    )
    p.add_argument(
        "--reservoir",
        type=int,
        default=RESERVOIR_TARGET_ROWS,
        help=f"Tamanho alvo da amostra para graficos (default: {RESERVOIR_TARGET_ROWS}).",
    )
    return p.parse_args()

def main():
    args = parse_args()
    eda_inmet(
        chunksize=args.chunksize,
        reservoir_target=args.reservoir,
        topk_cat=TOPK_CATEGORICAL,
        seed=RANDOM_SEED,
        viz_chunks=args.viz_chunks,
    )

if __name__ == "__main__":
    main()
