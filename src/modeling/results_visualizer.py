# src/modeling/results_visualizer.py

from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from src.utils import get_path, ensure_dir, get_logger


# ============================================================
# CONFIGURAÇÕES
# ============================================================

INTELLIGENT_MODELS = {
    "Naive Bayes",
    "Random Forest",
    "Regressão Logística",
    "SVM (Linear)",
    "XGBoost",
}

DUMMY_MODEL_KEYWORDS = {
    "dummy",
    "dummy classifier",
    "dummyclassifier",
}

MAIN_METRICS_HIGHER_BETTER = [
    "pr_auc",
    "roc_auc",
    "f1",
    "precision",
    "recall",
    "specificity",
    "accuracy",
    "tp",
    "tn",
]

MAIN_METRICS_LOWER_BETTER = [
    "brier_score",
    "fp",
    "fn",
]

HEATMAP_METRICS = [
    "pr_auc",
    "roc_auc",
    "f1",
    "precision",
    "recall",
    "specificity",
    "brier_score",
]

STAGE_METRICS = [
    "pr_auc",
    "roc_auc",
    "recall",
    "precision",
    "f1",
    "specificity",
]

METRIC_LABELS = {
    "pr_auc": "PR AUC",
    "roc_auc": "ROC AUC",
    "f1": "F1-score",
    "precision": "Precisão",
    "recall": "Recall",
    "specificity": "Especificidade",
    "brier_score": "Brier Score",
    "accuracy": "Acurácia",
    "tp": "Verdadeiros Positivos",
    "tn": "Verdadeiros Negativos",
    "fp": "Falsos Positivos",
    "fn": "Falsos Negativos",
}

DIRECTION_LABELS = {
    "pr_auc": "Maior é melhor",
    "roc_auc": "Maior é melhor",
    "f1": "Maior é melhor",
    "precision": "Maior é melhor",
    "recall": "Maior é melhor",
    "specificity": "Maior é melhor",
    "accuracy": "Maior é melhor",
    "tp": "Maior é melhor",
    "tn": "Maior é melhor",
    "brier_score": "Menor é melhor",
    "fp": "Menor é melhor",
    "fn": "Menor é melhor",
}

APPROACH_ORDER = [
    "Base (sem SMOTE, sem GridSearch, sem pesos)",
    "Base (sem SMOTE, sem GridSearch, sem balanceamento por peso)",
    "GridSearch + SMOTE",
    "GridSearchCV + SMOTE",
    "GridSearch + pesos de classe",
    "GridSearchCV + balanceamento por peso",
]

STAGE_POSITION_MAP = {
    "dummy_nao_calculada": 1,
    "dummy_calculada": 1,
    "modelo_nao_calculada_smote": 2,
    "modelo_nao_calculada_peso": 2,
    "modelo_calculada_smote": 3,
    "modelo_calculada_peso": 3,
}

STAGE_POSITION_LABELS = {
    1: "Estágio 1\nDummies",
    2: "Estágio 2\nModelos em bases não calculadas",
    3: "Estágio 3\nModelos em bases calculadas",
}

STAGE_COLOR_MAP = {
    "dummy_nao_calculada": "#9aa0a6",
    "dummy_calculada": "#5f6368",
    "modelo_nao_calculada_smote": "#5e81ac",
    "modelo_nao_calculada_peso": "#2e5d9f",
    "modelo_calculada_smote": "#a3be8c",
    "modelo_calculada_peso": "#4f8a3b",
}

MODEL_MARKERS = {
    "Naive Bayes": "o",
    "Random Forest": "s",
    "Regressão Logística": "^",
    "SVM (Linear)": "D",
    "XGBoost": "P",
    "Dummy": "X",
}

STAGE_PLOT_STYLE = {
    "point_size": 580,              # tamanho dos pontos no gráfico principal
    "point_edge_width": 0.9,        # espessura da borda do ponto
    "base_letter_fontsize": 10,     # tamanho da letra A-F dentro do ponto
    "mean_fontsize": 15,            # tamanho do texto da média
    "mean_y_offset_frac": 0.035,    # deslocamento vertical da média (fração da altura do eixo)
    "legend_marker_size": 11,       # tamanho dos marcadores nas legendas exportadas
    "legend_fontsize": 10,
    "legend_title_fontsize": 11,
}


# ============================================================
# LEITURA E PREPARO
# ============================================================

def load_results_table() -> pd.DataFrame:
    """
    Lê a planilha consolidada gerada anteriormente.
    Preferência: results.xlsx / aba academic_latest
    Fallback: results.csv
    """
    modeling_dir = get_path("paths", "data", "modeling")
    results_dir = Path(modeling_dir) / "results"

    xlsx_path = results_dir / "results.xlsx"
    csv_path = results_dir / "results.csv"

    if xlsx_path.exists():
        df = pd.read_excel(xlsx_path, sheet_name="academic_latest")
    elif csv_path.exists():
        df = pd.read_csv(csv_path)
    else:
        raise FileNotFoundError(
            f"Nenhum arquivo consolidado encontrado em {results_dir}. "
            "Esperado: results.xlsx ou results.csv"
        )

    if "timestamp_dt" in df.columns:
        df["timestamp_dt"] = pd.to_datetime(df["timestamp_dt"], errors="coerce")

    return df


def normalize_variation_label(value: str) -> str:
    if pd.isna(value):
        return ""
    v = str(value).strip()

    mapping = {
        "GridSearchCV + SMOTE": "GridSearch + SMOTE",
        "GridSearchCV + balanceamento por peso": "GridSearch + pesos de classe",
        "Base (sem SMOTE, sem GridSearch, sem balanceamento por peso)": "Base (sem SMOTE, sem GridSearch, sem pesos)",
    }
    return mapping.get(v, v)


def normalize_scenario_order_key(value: str) -> Tuple[int, str]:
    """
    Ordena A, B, C, D, E, F e depois calculated.
    """
    s = str(value)

    letter_order = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6}
    found = 999
    for letter, rank in letter_order.items():
        if f"base_{letter}" in s or f"Cenário {letter}" in s:
            found = rank
            break

    calc_flag = 1 if ("calculated" in s.lower() or "variáveis derivadas" in s.lower()) else 0
    return (found * 10 + calc_flag, s)


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra modelos inteligentes e padroniza rótulos.
    """
    work = df.copy()

    work["variation_label"] = work["variation_label"].apply(normalize_variation_label)
    work = work[work["model_label"].isin(INTELLIGENT_MODELS)].copy()

    # Remove duplicatas por segurança, mantendo o mais recente
    if "timestamp_dt" in work.columns:
        work = (
            work.sort_values("timestamp_dt")
            .groupby(["scenario_label", "model_label", "variation_label"], as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )

    work["combo_label"] = work["model_label"] + " | " + work["variation_label"]

    return work


# ============================================================
# HELPERS GERAIS
# ============================================================

def save_standalone_legend(
    handles: List,
    labels: List[str],
    title: str,
    path: Path,
    overwrite: bool = False,
    ncol: int = 1,
) -> bool:
    ensure_dir(path.parent)

    if path.exists() and not overwrite:
        return False

    fig_legend, ax_legend = plt.subplots(figsize=(6.5, max(2.2, 0.55 * len(labels))))
    ax_legend.axis("off")

    legend = ax_legend.legend(
        handles,
        labels,
        title=title,
        loc="center left",
        frameon=True,
        ncol=ncol,
        fontsize=STAGE_PLOT_STYLE["legend_fontsize"],
        title_fontsize=STAGE_PLOT_STYLE["legend_title_fontsize"],
    )

    fig_legend.canvas.draw()
    bbox = legend.get_window_extent().transformed(fig_legend.dpi_scale_trans.inverted())
    fig_legend.savefig(path, dpi=260, bbox_inches=bbox.expanded(1.12, 1.18))
    plt.close(fig_legend)
    return True

def select_top_k_per_stage_for_metric(
    stage_df: pd.DataFrame,
    metric: str,
    top_k: int = 3,
) -> pd.DataFrame:
    """
    Para uma métrica, seleciona apenas os top_k melhores dentro de cada macrogrupo:
    - estágio 1: dummies
    - estágio 2: modelos em bases não calculadas
    - estágio 3: modelos em bases calculadas

    Dentro do estágio 1, mantém a separação por dummy calculada/não calculada apenas na cor,
    mas a seleção é feita no macrogrupo inteiro.
    """
    if metric not in stage_df.columns:
        return stage_df.iloc[0:0].copy()

    work = stage_df.copy()

    # garante numérico
    work[metric] = pd.to_numeric(work[metric], errors="coerce")
    work = work[np.isfinite(work[metric])].copy()

    if work.empty:
        return work

    def macro_stage(row: pd.Series) -> str:
        stage_pos = row.get("stage_position", np.nan)
        if stage_pos == 1:
            return "estagio_1_dummies"
        if stage_pos == 2:
            return "estagio_2_nao_calculadas"
        if stage_pos == 3:
            return "estagio_3_calculadas"
        return "outros"

    work["macro_stage"] = work.apply(macro_stage, axis=1)
    work = work[work["macro_stage"] != "outros"].copy()

    ascending = metric in MAIN_METRICS_LOWER_BETTER

    selected_parts = []
    for _, group in work.groupby("macro_stage", sort=True):
        group = group.sort_values(
            by=[metric, "base_letter", "model_label", "variation_label_norm"],
            ascending=[ascending, True, True, True],
        ).head(top_k)
        selected_parts.append(group)

    if not selected_parts:
        return work.iloc[0:0].copy()

    selected = pd.concat(selected_parts, ignore_index=True)
    return selected

def ask_overwrite_existing() -> bool:
    """
    Pergunta uma única vez se deve sobrescrever figuras já existentes.
    Em execução não interativa, assume False.
    """
    prompt = "Deseja sobrescrever visualizações já existentes? [s/N]: "
    try:
        answer = input(prompt).strip().lower()
    except EOFError:
        return False

    return answer in {"s", "sim", "y", "yes"}


def is_calculated_scenario(value: str) -> bool:
    s = str(value).lower()
    return ("calculated" in s) or ("variáveis derivadas" in s)


def extract_base_letter(value: str) -> str:
    s = str(value)

    for letter in ["A", "B", "C", "D", "E", "F"]:
        if f"base_{letter}" in s or f"Cenário {letter}" in s:
            return letter
    return "?"


def is_dummy_model(value: str) -> bool:
    s = str(value).strip().lower()
    return any(keyword in s for keyword in DUMMY_MODEL_KEYWORDS)


def simplify_model_for_marker(value: str) -> str:
    if is_dummy_model(value):
        return "Dummy"
    return str(value)


def get_stage_group_key(row: pd.Series) -> str:
    is_calc = bool(row["is_calculated"])
    is_dummy = bool(row["is_dummy"])
    variation = str(row.get("variation_label_norm", "")).strip()

    if is_dummy and not is_calc:
        return "dummy_nao_calculada"
    if is_dummy and is_calc:
        return "dummy_calculada"

    if variation == "GridSearch + SMOTE":
        return "modelo_calculada_smote" if is_calc else "modelo_nao_calculada_smote"

    if variation == "GridSearch + pesos de classe":
        return "modelo_calculada_peso" if is_calc else "modelo_nao_calculada_peso"

    return ""


def stage_group_legend_label(group_key: str) -> str:
    labels = {
        "dummy_nao_calculada": "Dummy | Base não calculada",
        "dummy_calculada": "Dummy | Base calculada",
        "modelo_nao_calculada_smote": "Modelos | Não calculada | SMOTE",
        "modelo_nao_calculada_peso": "Modelos | Não calculada | Peso",
        "modelo_calculada_smote": "Modelos | Calculada | SMOTE",
        "modelo_calculada_peso": "Modelos | Calculada | Peso",
    }
    return labels.get(group_key, group_key)


def save_figure(fig: plt.Figure, path: Path, overwrite: bool = False) -> bool:
    ensure_dir(path.parent)

    if path.exists() and not overwrite:
        plt.close(fig)
        return False

    fig.tight_layout()
    fig.savefig(path, dpi=260, bbox_inches="tight")
    plt.close(fig)
    return True


def metric_direction(metric: str) -> str:
    return DIRECTION_LABELS.get(metric, "")


def sort_for_metric(df: pd.DataFrame, metric: str) -> pd.DataFrame:
    ascending = metric in MAIN_METRICS_LOWER_BETTER
    return df.sort_values(metric, ascending=ascending).copy()


def scenario_slug(label: str) -> str:
    return (
        label.lower()
        .replace(" ", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
        .replace("/", "_")
        .replace("__", "_")
    )


def minmax_scale(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    s = series.astype(float)
    if s.nunique(dropna=True) <= 1:
        return pd.Series([1.0] * len(s), index=s.index)

    min_v = s.min()
    max_v = s.max()
    scaled = (s - min_v) / (max_v - min_v)

    if not higher_is_better:
        scaled = 1 - scaled

    return scaled


# ============================================================
# VISUALIZAÇÕES POR BASE
# ============================================================

def plot_metric_bars_for_scenario(df_scenario: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    """
    Um gráfico por métrica para um cenário.
    """
    metrics = MAIN_METRICS_HIGHER_BETTER + MAIN_METRICS_LOWER_BETTER

    for metric in metrics:
        if metric not in df_scenario.columns:
            continue

        data = df_scenario.dropna(subset=[metric]).copy()
        if data.empty:
            continue

        data = sort_for_metric(data, metric)

        fig, ax = plt.subplots(figsize=(12, max(5, 0.45 * len(data))))
        ax.barh(data["combo_label"], data[metric])

        ax.set_title(f"{METRIC_LABELS.get(metric, metric)} | {data['scenario_label'].iloc[0]}")
        ax.set_xlabel(f"{METRIC_LABELS.get(metric, metric)}")
        ax.set_ylabel("Modelo | Abordagem")
        ax.grid(axis="x", alpha=0.3)

        direction = metric_direction(metric)
        if direction:
            ax.text(
                0.99,
                0.02,
                direction,
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=10,
                bbox=dict(boxstyle="round,pad=0.3", alpha=0.15),
            )

        path = out_dir / f"{metric}.png"
        save_figure(fig, path, overwrite=overwrite)


def plot_heatmap_for_scenario(df_scenario: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    """
    Heatmap com métricas normalizadas por cenário.
    """
    data = df_scenario.copy()
    if data.empty:
        return

    norm_df = pd.DataFrame(index=data["combo_label"])

    for metric in HEATMAP_METRICS:
        if metric not in data.columns:
            continue
        col = data[metric]
        higher_is_better = metric not in MAIN_METRICS_LOWER_BETTER
        norm_df[METRIC_LABELS.get(metric, metric)] = minmax_scale(col, higher_is_better=higher_is_better).values

    if norm_df.empty:
        return

    fig, ax = plt.subplots(figsize=(11, max(5, 0.45 * len(norm_df))))
    im = ax.imshow(norm_df.values, aspect="auto")

    ax.set_title(f"Perfil comparativo normalizado | {data['scenario_label'].iloc[0]}")
    ax.set_xticks(np.arange(len(norm_df.columns)))
    ax.set_xticklabels(norm_df.columns, rotation=45, ha="right")
    ax.set_yticks(np.arange(len(norm_df.index)))
    ax.set_yticklabels(norm_df.index)

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Desempenho relativo no cenário (0 a 1)")

    save_figure(fig, out_dir / "heatmap_normalized.png", overwrite=overwrite)


def plot_precision_recall_tradeoff(df_scenario: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    """
    Scatter Precision vs Recall, tamanho por PR AUC.
    """
    needed = {"precision", "recall", "pr_auc"}
    if not needed.issubset(df_scenario.columns):
        return

    data = df_scenario.dropna(subset=list(needed)).copy()
    if data.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 7))

    for _, row in data.iterrows():
        size = 200 + 3000 * float(row["pr_auc"])
        ax.scatter(row["recall"], row["precision"], s=size, alpha=0.7)
        ax.annotate(
            row["combo_label"],
            (row["recall"], row["precision"]),
            fontsize=8,
            xytext=(5, 4),
            textcoords="offset points",
        )

    ax.set_title(f"Trade-off Precisão vs Recall | {data['scenario_label'].iloc[0]}")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precisão")
    ax.grid(alpha=0.3)

    ax.text(
        0.99,
        0.02,
        "Mais acima e mais à direita é melhor. Tamanho do ponto = PR AUC",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", alpha=0.15),
    )

    save_figure(fig, out_dir / "precision_vs_recall.png", overwrite=overwrite)


def plot_pr_vs_roc(df_scenario: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    needed = {"pr_auc", "roc_auc"}
    if not needed.issubset(df_scenario.columns):
        return

    data = df_scenario.dropna(subset=list(needed)).copy()
    if data.empty:
        return

    fig, ax = plt.subplots(figsize=(10, 7))

    for _, row in data.iterrows():
        ax.scatter(row["roc_auc"], row["pr_auc"], s=250, alpha=0.7)
        ax.annotate(
            row["combo_label"],
            (row["roc_auc"], row["pr_auc"]),
            fontsize=8,
            xytext=(5, 4),
            textcoords="offset points",
        )

    ax.set_title(f"PR AUC vs ROC AUC | {data['scenario_label'].iloc[0]}")
    ax.set_xlabel("ROC AUC")
    ax.set_ylabel("PR AUC")
    ax.grid(alpha=0.3)

    ax.text(
        0.99,
        0.02,
        "Mais acima e mais à direita é melhor",
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", alpha=0.15),
    )

    save_figure(fig, out_dir / "pr_auc_vs_roc_auc.png", overwrite=overwrite)


# ============================================================
# VISUALIZAÇÕES ENTRE BASES
# ============================================================

def build_cross_base_mean_by_model(df: pd.DataFrame) -> pd.DataFrame:
    """
    Média das abordagens por modelo em cada base.
    """
    numeric_metrics = [
        "pr_auc", "roc_auc", "f1", "precision", "recall",
        "specificity", "brier_score", "accuracy", "tp", "tn", "fp", "fn"
    ]
    present_metrics = [m for m in numeric_metrics if m in df.columns]

    grouped = (
        df.groupby(["scenario_label", "model_label"], as_index=False)[present_metrics]
        .mean(numeric_only=True)
    )
    return grouped


def plot_cross_base_metric_lines(df_cross: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    """
    Uma linha por modelo ao longo das bases, para cada métrica.
    """
    for metric in [
        "pr_auc", "roc_auc", "f1", "precision", "recall",
        "specificity", "brier_score", "accuracy"
    ]:
        if metric not in df_cross.columns:
            continue

        fig, ax = plt.subplots(figsize=(14, 7))

        for model in sorted(df_cross["model_label"].unique()):
            subset = df_cross[df_cross["model_label"] == model].copy()
            subset["scenario_order"] = subset["scenario_label"].apply(normalize_scenario_order_key)
            subset = subset.sort_values("scenario_order")

            x = subset["scenario_label"]
            y = subset[metric]
            ax.plot(x, y, marker="o", label=model)

        ax.set_title(f"{METRIC_LABELS.get(metric, metric)} médio por modelo ao longo dos cenários")
        ax.set_xlabel("Cenário")
        ax.set_ylabel(METRIC_LABELS.get(metric, metric))
        ax.tick_params(axis="x", rotation=45)
        ax.grid(alpha=0.3)
        ax.legend()

        direction = metric_direction(metric)
        if direction:
            ax.text(
                0.99,
                0.02,
                direction,
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", alpha=0.15),
            )

        save_figure(fig, out_dir / f"{metric}_lines.png", overwrite=overwrite)


def plot_cross_base_heatmaps(df_cross: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    """
    Heatmap modelo x base para cada métrica.
    """
    for metric in [
        "pr_auc", "roc_auc", "f1", "precision", "recall",
        "specificity", "brier_score", "accuracy"
    ]:
        if metric not in df_cross.columns:
            continue

        pivot = df_cross.pivot(index="model_label", columns="scenario_label", values=metric)
        if pivot.empty:
            continue

        ordered_cols = sorted(pivot.columns, key=normalize_scenario_order_key)
        pivot = pivot[ordered_cols]

        fig, ax = plt.subplots(figsize=(14, 6))
        im = ax.imshow(pivot.values, aspect="auto")

        ax.set_title(f"{METRIC_LABELS.get(metric, metric)} médio | Modelo x Cenário")
        ax.set_xticks(np.arange(len(pivot.columns)))
        ax.set_xticklabels(pivot.columns, rotation=45, ha="right")
        ax.set_yticks(np.arange(len(pivot.index)))
        ax.set_yticklabels(pivot.index)

        cbar = fig.colorbar(im, ax=ax)
        cbar.set_label(METRIC_LABELS.get(metric, metric))

        direction = metric_direction(metric)
        if direction:
            ax.text(
                0.99,
                0.02,
                direction,
                transform=ax.transAxes,
                ha="right",
                va="bottom",
                fontsize=9,
                bbox=dict(boxstyle="round,pad=0.3", alpha=0.15),
            )

        save_figure(fig, out_dir / f"{metric}_heatmap.png", overwrite=overwrite)


# ============================================================
# SCORE COMPOSTO
# ============================================================

def build_composite_score(df: pd.DataFrame) -> pd.DataFrame:
    """
    Score composto por cenário, para apoiar ranking geral.
    """
    work = df.copy()

    weights = {
        "pr_auc": 0.35,
        "f1": 0.25,
        "recall": 0.20,
        "precision": 0.10,
        "specificity": 0.05,
        "roc_auc": 0.05,
    }

    score_parts = []

    for scenario, group in work.groupby("scenario_label"):
        temp = group.copy()

        for metric, weight in weights.items():
            if metric not in temp.columns:
                temp[f"{metric}_norm"] = 0.0
                continue
            temp[f"{metric}_norm"] = minmax_scale(temp[metric], higher_is_better=True)

        temp["composite_score"] = sum(temp[f"{metric}_norm"] * weight for metric, weight in weights.items())
        score_parts.append(temp)

    scored = pd.concat(score_parts, ignore_index=True)
    return scored


def plot_composite_score_by_scenario(df_scored: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    for scenario, group in df_scored.groupby("scenario_label"):
        data = group.sort_values("composite_score", ascending=True).copy()

        fig, ax = plt.subplots(figsize=(12, max(5, 0.45 * len(data))))
        ax.barh(data["combo_label"], data["composite_score"])
        ax.set_title(f"Score composto por combinação | {scenario}")
        ax.set_xlabel("Score composto normalizado")
        ax.set_ylabel("Modelo | Abordagem")
        ax.grid(axis="x", alpha=0.3)

        ax.text(
            0.99,
            0.02,
            "Maior é melhor | pesos: PR AUC 35%, F1 25%, Recall 20%, Precision 10%, Specificity 5%, ROC AUC 5%",
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=8,
            bbox=dict(boxstyle="round,pad=0.3", alpha=0.15),
        )

        save_figure(fig, out_dir / f"{scenario_slug(scenario)}_composite_score.png", overwrite=overwrite)


# ============================================================
# NOVAS VISUALIZAÇÕES POR ESTÁGIO
# ============================================================

def build_stage_progression_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    """
    Constrói base para gráficos de evolução por estágio.

    Estágio 1:
        Dummies em bases não calculadas e calculadas.

    Estágio 2:
        Modelos inteligentes em bases não calculadas,
        apenas GridSearch + SMOTE e GridSearch + pesos de classe.

    Estágio 3:
        Modelos inteligentes em bases calculadas,
        apenas GridSearch + SMOTE e GridSearch + pesos de classe.
    """
    work = df_raw.copy()

    if "variation_label" not in work.columns:
        work["variation_label"] = ""

    work["variation_label_norm"] = work["variation_label"].apply(normalize_variation_label)
    work["is_calculated"] = work["scenario_label"].apply(is_calculated_scenario)
    work["base_letter"] = work["scenario_label"].apply(extract_base_letter)
    work["is_dummy"] = work["model_label"].apply(is_dummy_model)
    work["model_for_marker"] = work["model_label"].apply(simplify_model_for_marker)

    # Mantém só o registro mais recente por combinação relevante
    grouping_cols = ["scenario_label", "model_label", "variation_label_norm"]
    if "timestamp_dt" in work.columns:
        work = (
            work.sort_values("timestamp_dt")
            .groupby(grouping_cols, as_index=False)
            .tail(1)
            .reset_index(drop=True)
        )

    dummy_part = work[work["is_dummy"]].copy()

    model_part = work[
        (~work["is_dummy"])
        & (work["model_label"].isin(INTELLIGENT_MODELS))
        & (work["variation_label_norm"].isin({"GridSearch + SMOTE", "GridSearch + pesos de classe"}))
    ].copy()

    stage_df = pd.concat([dummy_part, model_part], ignore_index=True)

    stage_df["stage_group"] = stage_df.apply(get_stage_group_key, axis=1)
    stage_df = stage_df[stage_df["stage_group"] != ""].copy()

    stage_df["stage_position"] = stage_df["stage_group"].map(STAGE_POSITION_MAP)

    # chave de cor mais específica para separar SMOTE e peso
    def color_key(row: pd.Series) -> str:
        if row["stage_group"] in {"dummy_nao_calculada", "dummy_calculada"}:
            return row["stage_group"]
        variation = str(row["variation_label_norm"]).strip()
        suffix = "smote" if variation == "GridSearch + SMOTE" else "peso"
        prefix = "modelo_calculada" if row["is_calculated"] else "modelo_nao_calculada"
        return f"{prefix}_{suffix}"

    stage_df["color_key"] = stage_df.apply(color_key, axis=1)

    return stage_df.reset_index(drop=True)


def _compute_stage_jitter_positions(stage_df: pd.DataFrame) -> pd.DataFrame:
    """
    Cria jitter determinístico para não empilhar pontos.
    Garante 1 valor de jitter por linha (evita mismatch de shapes).
    """
    work = stage_df.copy()

    order_cols = [
        "stage_position",
        "base_letter",
        "model_for_marker",
        "variation_label_norm",
        "scenario_label",
    ]
    work = work.sort_values(order_cols).reset_index(drop=True)

    # inicializa jitter com 0.0 para todas as linhas
    work["jitter"] = 0.0

    # calcula jitter dentro de cada estágio, garantindo mesmo tamanho
    for stage_pos, idx in work.groupby("stage_position", sort=True).groups.items():
        n = len(idx)
        if n == 1:
            jit = np.array([0.0], dtype=float)
        else:
            jit = np.linspace(-0.22, 0.22, n, dtype=float)

        # atribui jitter exatamente para as linhas desse estágio
        work.loc[idx, "jitter"] = jit

    # força stage_position para float pra soma não dar problema de dtype
    work["x_plot"] = work["stage_position"].astype(float) + work["jitter"].astype(float)
    return work


def plot_stage_progression_for_metric(
    stage_df: pd.DataFrame,
    metric: str,
    out_dir: Path,
    overwrite: bool,
    top_k: int = 3,
) -> None:
    if metric not in stage_df.columns:
        return

    data = stage_df.copy()

    # garante tipos numéricos e remove valores inválidos
    data[metric] = pd.to_numeric(data[metric], errors="coerce")
    data["stage_position"] = pd.to_numeric(data["stage_position"], errors="coerce")

    data = data[
        np.isfinite(data[metric]) &
        np.isfinite(data["stage_position"])
    ].copy()

    if data.empty:
        return

    # seleciona apenas os melhores por macroestágio para esta métrica
    data = select_top_k_per_stage_for_metric(data, metric=metric, top_k=top_k)
    if data.empty:
        return

    data = _compute_stage_jitter_positions(data)

    # segurança extra
    data["x_plot"] = pd.to_numeric(data["x_plot"], errors="coerce")
    data = data[
        np.isfinite(data["x_plot"]) &
        np.isfinite(data[metric])
    ].copy()

    if data.empty:
        return

    fig, ax = plt.subplots(figsize=(14.5, 8.8))

    for stage_pos in [1, 2, 3]:
        ax.axvline(stage_pos, color="lightgray", linestyle="--", linewidth=1.2, zorder=0)

    # faixa do eixo y para posicionar médias abaixo
    y_min = float(data[metric].min())
    y_max = float(data[metric].max())
    y_range = max(y_max - y_min, 1e-9)
    mean_offset = y_range * STAGE_PLOT_STYLE["mean_y_offset_frac"]

    for _, row in data.iterrows():
        x = float(row["x_plot"])
        y = float(row[metric])

        marker = MODEL_MARKERS.get(row["model_for_marker"], "o")
        color = STAGE_COLOR_MAP.get(row["color_key"], "#4c566a")

        ax.scatter(
            x,
            y,
            s=STAGE_PLOT_STYLE["point_size"],
            alpha=0.9,
            marker=marker,
            c=color,
            edgecolors="black",
            linewidths=STAGE_PLOT_STYLE["point_edge_width"],
            zorder=3,
        )

        if np.isfinite(x) and np.isfinite(y):
            ax.text(
                x,
                y,
                str(row["base_letter"]),
                ha="center",
                va="center",
                fontsize=STAGE_PLOT_STYLE["base_letter_fontsize"],
                color="white" if row["color_key"] != "dummy_nao_calculada" else "black",
                fontweight="bold",
                zorder=4,
            )

    # linha de média por estágio usando só os top_k selecionados
    stage_means = (
        data.groupby("stage_position", as_index=False)[metric]
        .mean()
        .sort_values("stage_position")
    )

    if not stage_means.empty:
        ax.plot(
            stage_means["stage_position"],
            stage_means[metric],
            linestyle="-",
            linewidth=2.2,
            color="black",
            alpha=0.55,
            zorder=2,
        )

        for _, row in stage_means.iterrows():
            x = float(row["stage_position"])
            y = float(row[metric])
            if np.isfinite(x) and np.isfinite(y):
                ax.text(
                    x,
                    y - mean_offset,
                    f"média={y:.4f}",
                    fontsize=STAGE_PLOT_STYLE["mean_fontsize"],
                    ha="center",
                    va="top",
                    bbox=dict(boxstyle="round,pad=0.22", alpha=0.18),
                    zorder=5,
                )

    ax.set_xlim(0.55, 3.45)
    ax.set_xticks([1, 2, 3])
    ax.set_xticklabels([STAGE_POSITION_LABELS[x] for x in [1, 2, 3]], fontsize=11)
    ax.set_ylabel(METRIC_LABELS.get(metric, metric), fontsize=12)
    ax.set_title(
        f"Evolução por estágio metodológico | {METRIC_LABELS.get(metric, metric)} | Top {top_k} por estágio",
        fontsize=15,
    )
    ax.grid(axis="y", alpha=0.25)

    # dá um pequeno respiro extra embaixo para caber o texto da média
    ax.set_ylim(y_min - (0.10 * y_range), y_max + (0.05 * y_range))

    direction = metric_direction(metric)
    if direction:
        ax.text(
            0.99,
            0.02,
            f"{direction}. Letras dentro dos pontos indicam a base (A-F).",
            transform=ax.transAxes,
            ha="right",
            va="bottom",
            fontsize=10,
            bbox=dict(boxstyle="round,pad=0.35", alpha=0.15),
        )

    # handles da legenda de grupos
    color_legend_keys = [
        "dummy_nao_calculada",
        "dummy_calculada",
        "modelo_nao_calculada_smote",
        "modelo_nao_calculada_peso",
        "modelo_calculada_smote",
        "modelo_calculada_peso",
    ]
    color_handles = []
    color_labels = []
    for key in color_legend_keys:
        handle = plt.Line2D(
            [0], [0],
            marker="o",
            color="w",
            markerfacecolor=STAGE_COLOR_MAP[key],
            markeredgecolor="black",
            markersize=STAGE_PLOT_STYLE["legend_marker_size"],
            linestyle="None",
        )
        color_handles.append(handle)
        color_labels.append(stage_group_legend_label(key))

    # handles da legenda de marcadores/modelos
    marker_handles = []
    marker_labels = []
    for model_name in ["Dummy", "Naive Bayes", "Random Forest", "Regressão Logística", "SVM (Linear)", "XGBoost"]:
        handle = plt.Line2D(
            [0], [0],
            marker=MODEL_MARKERS.get(model_name, "o"),
            color="black",
            markerfacecolor="white",
            markersize=STAGE_PLOT_STYLE["legend_marker_size"],
            linestyle="None",
        )
        marker_handles.append(handle)
        marker_labels.append(model_name)

    # salva figura principal sem a legenda acoplada
    save_figure(fig, out_dir / f"{metric}_stage_progression.png", overwrite=overwrite)

    # salva legendas separadas
    legends_dir = ensure_dir(out_dir / "legends")
    save_standalone_legend(
        color_handles,
        color_labels,
        title="Grupos",
        path=legends_dir / f"{metric}_legend_groups.png",
        overwrite=overwrite,
    )
    save_standalone_legend(
        marker_handles,
        marker_labels,
        title="Marcador por modelo",
        path=legends_dir / f"{metric}_legend_models.png",
        overwrite=overwrite,
    )


def plot_all_stage_progressions(stage_df: pd.DataFrame, out_dir: Path, overwrite: bool) -> None:
    if stage_df.empty:
        return

    for metric in STAGE_METRICS:
        plot_stage_progression_for_metric(
            stage_df,
            metric,
            out_dir,
            overwrite=overwrite,
            top_k=3,
        )


def export_stage_support_table(stage_df: pd.DataFrame, out_dir: Path) -> None:
    ensure_dir(out_dir)
    export_cols = [
        "scenario_id",
        "scenario_label",
        "base_letter",
        "is_calculated",
        "model_label",
        "variation_label",
        "variation_label_norm",
        "stage_group",
        "stage_position",
        "pr_auc",
        "roc_auc",
        "recall",
        "precision",
        "f1",
        "specificity",
        "brier_score",
        "accuracy",
        "tp",
        "tn",
        "fp",
        "fn",
        "threshold",
        "timestamp_dt",
    ]
    export_cols = [c for c in export_cols if c in stage_df.columns]
    stage_df[export_cols].to_csv(
        out_dir / "stage_progression_points.csv",
        index=False,
        encoding="utf-8-sig",
    )


# ============================================================
# EXPORT DE TABELAS AUXILIARES
# ============================================================

def export_support_tables(df: pd.DataFrame, df_cross: pd.DataFrame, df_scored: pd.DataFrame, out_dir: Path) -> None:
    ensure_dir(out_dir)

    ranking = (
        df_scored.sort_values(["scenario_label", "composite_score"], ascending=[True, False])
        .groupby("scenario_label", as_index=False)
        .head(5)
        .reset_index(drop=True)
    )

    ranking.to_csv(out_dir / "top5_por_cenario_score_composto.csv", index=False, encoding="utf-8-sig")
    df_cross.to_csv(out_dir / "medias_por_modelo_e_cenario.csv", index=False, encoding="utf-8-sig")


# ============================================================
# PIPELINE
# ============================================================

def run_visualization_pipeline() -> None:
    log = get_logger("results.visualizer", kind="results_viz", per_run_file=True)

    overwrite = ask_overwrite_existing()

    df_raw = load_results_table()
    df = prepare_dataframe(df_raw)

    modeling_dir = get_path("paths", "data", "modeling")
    results_dir = ensure_dir(Path(modeling_dir) / "results")
    viz_dir = ensure_dir(results_dir / "visualizations")

    per_base_bars_dir = ensure_dir(viz_dir / "01_per_base_metric_bars")
    per_base_heatmaps_dir = ensure_dir(viz_dir / "02_per_base_heatmaps")
    per_base_tradeoffs_dir = ensure_dir(viz_dir / "03_per_base_tradeoffs")
    cross_base_dir = ensure_dir(viz_dir / "04_cross_base")
    support_dir = ensure_dir(viz_dir / "05_support_tables")
    stage_dir = ensure_dir(viz_dir / "06_stage_progression")

    log.info(f"[LOAD] linhas após filtro de modelos inteligentes: {len(df)}")
    log.info(f"[OVERWRITE] sobrescrever arquivos existentes: {overwrite}")

    # por cenário
    for scenario, group in df.groupby("scenario_label"):
        slug = scenario_slug(scenario)

        plot_metric_bars_for_scenario(group, ensure_dir(per_base_bars_dir / slug), overwrite=overwrite)
        plot_heatmap_for_scenario(group, ensure_dir(per_base_heatmaps_dir / slug), overwrite=overwrite)
        plot_precision_recall_tradeoff(group, ensure_dir(per_base_tradeoffs_dir / slug), overwrite=overwrite)
        plot_pr_vs_roc(group, ensure_dir(per_base_tradeoffs_dir / slug), overwrite=overwrite)

        log.info(f"[PLOT] cenário concluído: {scenario}")

    # entre cenários
    df_cross = build_cross_base_mean_by_model(df)
    plot_cross_base_metric_lines(df_cross, cross_base_dir, overwrite=overwrite)
    plot_cross_base_heatmaps(df_cross, cross_base_dir, overwrite=overwrite)

    # score composto
    df_scored = build_composite_score(df)
    plot_composite_score_by_scenario(df_scored, cross_base_dir, overwrite=overwrite)

    # novas visualizações por estágio
    stage_df = build_stage_progression_dataframe(df_raw)
    plot_all_stage_progressions(stage_df, stage_dir, overwrite=overwrite)
    export_stage_support_table(stage_df, stage_dir)

    # tabelas auxiliares
    export_support_tables(df, df_cross, df_scored, support_dir)

    log.info(f"[DONE] visualizações salvas em: {viz_dir}")


def main() -> None:
    run_visualization_pipeline()


if __name__ == "__main__":
    main()