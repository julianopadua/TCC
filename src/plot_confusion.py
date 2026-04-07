# src/plot_confusion.py
from __future__ import annotations

from pathlib import Path
import re

import matplotlib.pyplot as plt
import numpy as np


OUTPUT_DIR = Path(r"D:\Projetos\TCC\doc\tcc\imagens")


MODELS = [
    {
        "scenario_id": "base_F_calculated",
        "scenario_label": "Cenário F",
        "model_label": "XGBoost",
        "variation_label": "GridSearchCV + balanceamento por peso",
        "tn": 1_558_936,
        "fp": 411_944,
        "fn": 4_594,
        "tp": 24_526,
    },
    {
        "scenario_id": "base_D_calculated",
        "scenario_label": "Cenário D",
        "model_label": "XGBoost",
        "variation_label": "GridSearchCV + balanceamento por peso",
        "tn": 1_563_051,
        "fp": 408_361,
        "fn": 4_588,
        "tp": 24_000,
    },
    {
        "scenario_id": "base_F_calculated",
        "scenario_label": "Cenário F",
        "model_label": "XGBoost",
        "variation_label": "GridSearchCV + SMOTE",
        "tn": 1_929_441,
        "fp": 41_439,
        "fn": 22_220,
        "tp": 6_900,
    },
]


def slugify(text: str) -> str:
    text = text.lower().strip()
    text = (
        text.replace("ã", "a")
        .replace("á", "a")
        .replace("à", "a")
        .replace("â", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("õ", "o")
        .replace("ú", "u")
        .replace("ç", "c")
    )
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def build_filename(scenario_label: str, model_label: str, variation_label: str) -> str:
    scenario_slug = slugify(scenario_label)
    model_slug = slugify(model_label)
    variation_slug = slugify(variation_label)
    return f"cm_{scenario_slug}_{model_slug}_{variation_slug}.png"


def plot_confusion_matrix_binary(
    tn: int,
    fp: int,
    fn: int,
    tp: int,
    title: str,
    output_path: Path,
) -> None:
    cm = np.array([[tn, fp], [fn, tp]], dtype=np.int64)
    total = cm.sum()
    percentages = cm / total * 100.0

    fig, ax = plt.subplots(figsize=(6.6, 5.6), dpi=220)
    im = ax.imshow(cm, cmap="Blues")

    # Título e rótulos
    ax.set_title(title, fontsize=15, pad=12)
    ax.set_xlabel("Classe prevista", fontsize=15)
    ax.set_ylabel("Classe real", fontsize=15)

    class_names = ["Sem foco", "Com foco"]
    ax.set_xticks([0, 1], labels=class_names, fontsize=13)
    ax.set_yticks([0, 1], labels=class_names, fontsize=13)

    # Grade suave entre células
    ax.set_xticks(np.arange(-0.5, 2, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, 2, 1), minor=True)
    ax.grid(which="minor", color="#d9d9d9", linestyle="-", linewidth=1.2)
    ax.tick_params(which="minor", bottom=False, left=False)

    # Cor do texto adaptativa
    threshold = cm.max() * 0.55
    for i in range(2):
        for j in range(2):
            value = cm[i, j]
            pct = percentages[i, j]
            color = "white" if value > threshold else "#1f1f1f"
            label = f"{value:,}\n({pct:.2f}%)".replace(",", ".")
            ax.text(
                j,
                i,
                label,
                ha="center",
                va="center",
                fontsize=12,
                color=color,
                fontweight="semibold",
            )

    # Barra de cor
    cbar = fig.colorbar(im, ax=ax, fraction=0.046, pad=0.04)
    cbar.ax.tick_params(labelsize=9)

    # Moldura mais limpa
    for spine in ax.spines.values():
        spine.set_visible(False)

    fig.tight_layout()
    fig.savefig(output_path, bbox_inches="tight", dpi=300)
    plt.close(fig)


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    for item in MODELS:
        title = (
            f"{item['model_label']} "
            f"({item['scenario_label']}, {item['variation_label']})"
        )
        filename = build_filename(
            scenario_label=item["scenario_label"],
            model_label=item["model_label"],
            variation_label=item["variation_label"],
        )
        output_path = OUTPUT_DIR / filename

        plot_confusion_matrix_binary(
            tn=item["tn"],
            fp=item["fp"],
            fn=item["fn"],
            tp=item["tp"],
            title=title,
            output_path=output_path,
        )

        print(f"Imagem salva em: {output_path}")


if __name__ == "__main__":
    main()