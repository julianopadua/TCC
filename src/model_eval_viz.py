from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import joblib
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    precision_recall_curve,
    roc_auc_score,
    roc_curve,
)

_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.ml.eval_data import ScenarioEvalData, load_scenario_eval_data
from src.utils import get_logger, loadConfig


@dataclass
class RunArtifact:
    source: str  # "tcc" | "article"
    model_type: str
    variation: str
    scenario: str
    timestamp: str
    metrics_path: Path
    model_path: Optional[Path]
    payload: Dict[str, Any]

    @property
    def label(self) -> str:
        return f"{self.model_type}/{self.variation}/{self.scenario}@{self.timestamp}"


@dataclass
class EvalResult:
    run: RunArtifact
    y_true: np.ndarray
    y_score: np.ndarray
    y_pred: np.ndarray
    threshold: float
    roc_auc: Optional[float]
    pr_auc: Optional[float]
    fpr: np.ndarray
    tpr: np.ndarray
    roc_thr: np.ndarray
    precision: np.ndarray
    recall: np.ndarray
    pr_thr: np.ndarray
    importance_df: Optional[pd.DataFrame]


def _sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.asarray(x, dtype=float)
    x = np.clip(np.nan_to_num(x, nan=0.0, posinf=50.0, neginf=-50.0), -50.0, 50.0)
    return 1.0 / (1.0 + np.exp(-x))


def _predict_scores(model: Any, X: pd.DataFrame) -> np.ndarray:
    if hasattr(model, "predict_proba"):
        p = model.predict_proba(X)[:, 1]
    elif hasattr(model, "decision_function"):
        p = _sigmoid(model.decision_function(X))
    else:
        raise AttributeError("Modelo sem predict_proba/decision_function.")
    p = np.asarray(p, dtype=float)
    return np.clip(np.nan_to_num(p, nan=0.5, posinf=1.0, neginf=0.0), 0.0, 1.0)


def _extract_importance(model: Any, feature_names: List[str]) -> Optional[pd.DataFrame]:
    vals: Optional[np.ndarray] = None
    if hasattr(model, "feature_importances_"):
        vals = np.asarray(model.feature_importances_, dtype=float)
    elif hasattr(model, "coef_"):
        coef = np.asarray(model.coef_)
        if coef.ndim == 2:
            coef = coef[0]
        vals = np.abs(coef.astype(float))
    if vals is None:
        return None
    n = min(len(vals), len(feature_names))
    df = pd.DataFrame({"feature": feature_names[:n], "importance": vals[:n]})
    return df.sort_values("importance", ascending=False).reset_index(drop=True)


def _parse_ts(name: str, prefix: str) -> Optional[str]:
    if not (name.startswith(prefix) and name.endswith(".json" if prefix == "metrics_" else ".joblib")):
        return None
    ts = name[len(prefix):].split(".")[0]
    return ts if ts else None


def _discover_runs_for_root(
    root: Path,
    source: str,
    scenarios: Optional[set[str]],
    models: Optional[set[str]],
    variations: Optional[set[str]],
) -> List[RunArtifact]:
    out: List[RunArtifact] = []
    if not root.is_dir():
        return out
    for model_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        model_type = model_dir.name
        if models and model_type not in models:
            continue
        for var_dir in sorted(p for p in model_dir.iterdir() if p.is_dir()):
            variation = var_dir.name
            if variations and variation not in variations:
                continue
            for sc_dir in sorted(p for p in var_dir.iterdir() if p.is_dir()):
                scenario = sc_dir.name
                if scenarios and scenario not in scenarios:
                    continue
                metrics_by_ts: Dict[str, Path] = {}
                model_by_ts: Dict[str, Path] = {}
                for fp in sc_dir.glob("metrics_*.json"):
                    ts = _parse_ts(fp.name, "metrics_")
                    if ts:
                        metrics_by_ts[ts] = fp
                for fp in sc_dir.glob("model_*.joblib"):
                    ts = _parse_ts(fp.name, "model_")
                    if ts:
                        model_by_ts[ts] = fp
                all_ts = sorted(set(metrics_by_ts).union(model_by_ts))
                for ts in all_ts:
                    mp = metrics_by_ts.get(ts)
                    if mp is None:
                        continue
                    try:
                        payload = json.loads(mp.read_text(encoding="utf-8"))
                    except Exception:
                        payload = {}
                    out.append(
                        RunArtifact(
                            source=source,
                            model_type=model_type,
                            variation=variation,
                            scenario=scenario,
                            timestamp=ts,
                            metrics_path=mp,
                            model_path=model_by_ts.get(ts),
                            payload=payload,
                        )
                    )
    return out


def discover_runs(
    cfg: Dict[str, Any],
    source: str,
    scenarios: Optional[Iterable[str]],
    models: Optional[Iterable[str]],
    variations: Optional[Iterable[str]],
) -> List[RunArtifact]:
    scenarios_set = set(scenarios) if scenarios else None
    models_set = set(models) if models else None
    vars_set = set(variations) if variations else None
    if source == "article":
        sub = str((cfg.get("article_pipeline") or {}).get("train_runner_results_subdir", "_results"))
        root = Path(cfg["paths"]["data"]["article"]) / sub
    else:
        root = Path(cfg["paths"]["data"]["modeling"]) / "results"
    return _discover_runs_for_root(root, source, scenarios_set, models_set, vars_set)


def _pick_runs(runs: List[RunArtifact], pick: str) -> List[RunArtifact]:
    if pick == "all":
        return runs
    grouped: Dict[Tuple[str, str, str, str], List[RunArtifact]] = {}
    for r in runs:
        key = (r.source, r.model_type, r.variation, r.scenario)
        grouped.setdefault(key, []).append(r)
    out: List[RunArtifact] = []
    for group in grouped.values():
        group = sorted(group, key=lambda x: x.timestamp)
        out.append(group[-1] if pick == "latest" else group[0])
    return sorted(out, key=lambda x: (x.model_type, x.variation, x.scenario, x.timestamp))


def _resolve_eval_data(
    cache: Dict[Tuple[str, str], ScenarioEvalData],
    run: RunArtifact,
    batch_rows: Optional[int],
    max_train_rows: Optional[int],
    max_test_rows: Optional[int],
) -> ScenarioEvalData:
    key = (run.source, run.scenario)
    if key not in cache:
        cache[key] = load_scenario_eval_data(
            run.scenario,
            use_article_data=(run.source == "article"),
            batch_rows=batch_rows,
            max_train_rows=max_train_rows,
            max_test_rows=max_test_rows,
        )
    return cache[key]


def evaluate_run(
    run: RunArtifact,
    eval_data: ScenarioEvalData,
    threshold: Optional[float] = None,
) -> EvalResult:
    if run.model_path is None:
        raise FileNotFoundError(f"model_*.joblib ausente para {run.label}")
    model = joblib.load(run.model_path)
    thr = float(
        threshold
        if threshold is not None
        else ((run.payload.get("metrics") or {}).get("threshold", 0.5))
    )
    y_true = eval_data.y_test.to_numpy(dtype=np.int8, copy=False)
    y_score = _predict_scores(model, eval_data.X_test)
    y_pred = (y_score >= thr).astype(np.int8)
    fpr, tpr, roc_thr = roc_curve(y_true, y_score)
    precision, recall, pr_thr = precision_recall_curve(y_true, y_score)
    roc_auc = float(roc_auc_score(y_true, y_score)) if len(np.unique(y_true)) > 1 else None
    pr_auc = float(average_precision_score(y_true, y_score)) if len(np.unique(y_true)) > 1 else None
    imp = _extract_importance(model, eval_data.valid_features)
    return EvalResult(
        run=run,
        y_true=y_true,
        y_score=y_score,
        y_pred=y_pred,
        threshold=thr,
        roc_auc=roc_auc,
        pr_auc=pr_auc,
        fpr=fpr,
        tpr=tpr,
        roc_thr=roc_thr,
        precision=precision,
        recall=recall,
        pr_thr=pr_thr,
        importance_df=imp,
    )


def _safe_slug(s: str) -> str:
    return "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in s)


def _plot_roc_single(res: EvalResult, out_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(res.fpr, res.tpr, label=f"ROC AUC={res.roc_auc:.4f}" if res.roc_auc is not None else "ROC")
    ax.plot([0, 1], [0, 1], linestyle="--")
    ax.set_xlabel("FPR")
    ax.set_ylabel("TPR")
    ax.set_title(f"ROC - {res.run.label}")
    ax.legend(loc="lower right")
    ax.grid(True, alpha=0.3)
    p = out_dir / f"roc_{_safe_slug(res.run.label)}.png"
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def _plot_pr_single(res: EvalResult, out_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(res.recall, res.precision, label=f"PR AUC={res.pr_auc:.4f}" if res.pr_auc is not None else "PR")
    ax.set_xlabel("Recall")
    ax.set_ylabel("Precision")
    ax.set_title(f"PR - {res.run.label}")
    ax.legend(loc="lower left")
    ax.grid(True, alpha=0.3)
    p = out_dir / f"pr_{_safe_slug(res.run.label)}.png"
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def _plot_overlay(results: List[EvalResult], kind: str, out_dir: Path) -> Path:
    fig, ax = plt.subplots(figsize=(8, 6))
    for res in results:
        if kind == "roc":
            label = f"{res.run.label} | AUC={res.roc_auc:.4f}" if res.roc_auc is not None else res.run.label
            ax.plot(res.fpr, res.tpr, label=label)
        else:
            label = f"{res.run.label} | AUC={res.pr_auc:.4f}" if res.pr_auc is not None else res.run.label
            ax.plot(res.recall, res.precision, label=label)
    if kind == "roc":
        ax.plot([0, 1], [0, 1], linestyle="--")
        ax.set_xlabel("FPR")
        ax.set_ylabel("TPR")
        ax.set_title("ROC Overlay")
    else:
        ax.set_xlabel("Recall")
        ax.set_ylabel("Precision")
        ax.set_title("PR Overlay")
    ax.grid(True, alpha=0.3)
    ax.legend(loc="best", fontsize=8)
    p = out_dir / f"{kind}_overlay.png"
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def _plot_importance(res: EvalResult, out_dir: Path, top_k: int = 25) -> Optional[Path]:
    if res.importance_df is None or res.importance_df.empty:
        return None
    df = res.importance_df.head(top_k).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, max(4, 0.25 * len(df))))
    ax.barh(df["feature"], df["importance"])
    ax.set_title(f"Feature Importance - {res.run.label}")
    ax.set_xlabel("Importance")
    p = out_dir / f"fi_{_safe_slug(res.run.label)}.png"
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def _plot_shap_summary(
    res: EvalResult,
    eval_data: ScenarioEvalData,
    out_dir: Path,
    max_samples: int,
) -> Optional[Path]:
    try:
        import shap  # type: ignore
    except Exception:
        return None
    model = joblib.load(res.run.model_path)
    n = len(eval_data.X_test)
    use_n = min(n, max_samples) if max_samples > 0 else n
    Xs = eval_data.X_test.iloc[:use_n]
    try:
        explainer = shap.Explainer(model, Xs)
        shap_values = explainer(Xs)
    except Exception:
        return None
    p = out_dir / f"shap_{_safe_slug(res.run.label)}.png"
    plt.figure(figsize=(10, 6))
    shap.plots.beeswarm(shap_values, max_display=20, show=False)
    plt.tight_layout()
    plt.savefig(p, dpi=150)
    plt.close()
    return p


def _write_csv_artifacts(res: EvalResult, out_dir: Path) -> Dict[str, Path]:
    paths: Dict[str, Path] = {}
    pred = pd.DataFrame(
        {
            "y_true": res.y_true,
            "y_score": res.y_score,
            "y_pred": res.y_pred,
        }
    )
    p_pred = out_dir / f"predictions_{_safe_slug(res.run.label)}.csv"
    pred.to_csv(p_pred, index=False, encoding="utf-8")
    paths["predictions"] = p_pred

    roc_df = pd.DataFrame({"fpr": res.fpr, "tpr": res.tpr, "threshold": res.roc_thr})
    p_roc = out_dir / f"roc_curve_points_{_safe_slug(res.run.label)}.csv"
    roc_df.to_csv(p_roc, index=False, encoding="utf-8")
    paths["roc_curve"] = p_roc

    pr_df = pd.DataFrame({"recall": res.recall, "precision": res.precision})
    if len(res.pr_thr) > 0:
        thr = np.append(res.pr_thr, np.nan)
        pr_df["threshold"] = thr[: len(pr_df)]
    p_pr = out_dir / f"pr_curve_points_{_safe_slug(res.run.label)}.csv"
    pr_df.to_csv(p_pr, index=False, encoding="utf-8")
    paths["pr_curve"] = p_pr
    return paths


def _metrics_rows(
    runs: List[RunArtifact],
    eval_by_label: Dict[str, EvalResult],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for run in runs:
        ev = eval_by_label.get(run.label)
        m = run.payload.get("metrics") or {}
        rows.append(
            {
                "source": run.source,
                "model_type": run.model_type,
                "variation": run.variation,
                "scenario": run.scenario,
                "timestamp": run.timestamp,
                "model_available": bool(run.model_path),
                "roc_auc_eval": None if ev is None else ev.roc_auc,
                "pr_auc_eval": None if ev is None else ev.pr_auc,
                "roc_auc_saved": m.get("roc_auc"),
                "pr_auc_saved": m.get("pr_auc"),
                "threshold": None if ev is None else ev.threshold,
            }
        )
    return rows


def _plot_model_performance_comparison(rows: pd.DataFrame, out_dir: Path) -> Path:
    # Nao e 5-fold CV: comparacao de metricas do teste temporal por run.
    fig, ax = plt.subplots(figsize=(10, 6))
    x = np.arange(len(rows))
    ax.bar(x - 0.2, rows["pr_auc_eval"], width=0.4, label="PR AUC (test temporal)")
    ax.bar(x + 0.2, rows["roc_auc_eval"], width=0.4, label="ROC AUC (test temporal)")
    ax.set_xticks(x)
    ax.set_xticklabels(
        [f"{m}/{v}\n{s}" for m, v, s in zip(rows["model_type"], rows["variation"], rows["scenario"])],
        rotation=45,
        ha="right",
        fontsize=8,
    )
    ax.set_ylim(0, min(1.0, max(rows["roc_auc_eval"].max(), rows["pr_auc_eval"].max()) + 0.05))
    ax.set_title("Model Performance Comparison (temporal holdout, not 5-fold CV)")
    ax.grid(axis="y", alpha=0.3)
    ax.legend()
    p = out_dir / "model_performance_comparison.png"
    fig.tight_layout()
    fig.savefig(p, dpi=150)
    plt.close(fig)
    return p


def _render_markdown(
    runs: List[RunArtifact],
    eval_by_label: Dict[str, EvalResult],
    out_dir: Path,
    csv_paths: Dict[str, Path],
    image_paths: List[Path],
    comparison_png: Optional[Path],
) -> str:
    rows = _metrics_rows(runs, eval_by_label)
    lines: List[str] = []
    lines.append("# Model evaluation report")
    lines.append("")
    lines.append(f"- Generated at `{datetime.now().isoformat(timespec='seconds')}`")
    lines.append(f"- Runs: `{len(runs)}`")
    lines.append(
        "- Note: `model_performance_comparison.png` is temporal holdout comparison; "
        "it is not 5-fold CV uncertainty."
    )
    lines.append("")
    lines.append("## Metrics")
    lines.append("")
    lines.append("| source | model | variation | scenario | ts | model | PR AUC(eval) | ROC AUC(eval) | PR AUC(saved) | ROC AUC(saved) |")
    lines.append("|---|---|---|---|---|---|---:|---:|---:|---:|")
    for r in rows:
        pr_eval = "" if r["pr_auc_eval"] is None else f"{float(r['pr_auc_eval']):.6f}"
        roc_eval = "" if r["roc_auc_eval"] is None else f"{float(r['roc_auc_eval']):.6f}"
        pr_saved = "" if r["pr_auc_saved"] is None else f"{float(r['pr_auc_saved']):.6f}"
        roc_saved = "" if r["roc_auc_saved"] is None else f"{float(r['roc_auc_saved']):.6f}"
        lines.append(
            f"| {r['source']} | {r['model_type']} | {r['variation']} | {r['scenario']} | {r['timestamp']} | "
            f"{'yes' if r['model_available'] else 'no'} | {pr_eval} | {roc_eval} | {pr_saved} | {roc_saved} |"
        )
    lines.append("")
    if comparison_png:
        lines.append(f"- Performance comparison: `{comparison_png.name}`")
    if image_paths:
        lines.append("")
        lines.append("## Figures")
        lines.append("")
        for p in image_paths:
            lines.append(f"- `{p.name}`")
    if csv_paths:
        lines.append("")
        lines.append("## CSV artifacts")
        lines.append("")
        for k, p in sorted(csv_paths.items()):
            lines.append(f"- `{k}`: `{p.name}`")
    return "\n".join(lines) + "\n"


def _ask_select(title: str, options: List[str]) -> List[str]:
    print(f"\n{title}")
    if not options:
        return []
    for i, op in enumerate(options, start=1):
        print(f"[{i}] {op}")
    raw = input("Selecione (ex.: 1,3 ou vazio para todos): ").strip()
    if not raw:
        return options
    idxs = set()
    for tok in raw.replace(";", ",").split(","):
        tok = tok.strip()
        if tok.isdigit():
            idxs.add(int(tok))
    return [op for i, op in enumerate(options, start=1) if i in idxs]


def _interactive_select(cfg: Dict[str, Any]) -> Tuple[str, List[str], List[str], List[str]]:
    print("\n--- Fonte dos resultados ---")
    print("[1] TCC")
    print("[2] Article")
    src = input(">> Fonte [1]: ").strip().lower()
    source = "article" if src in ("2", "a", "article") else "tcc"
    runs = discover_runs(cfg, source, None, None, None)
    scenarios = sorted({r.scenario for r in runs})
    models = sorted({r.model_type for r in runs})
    variations = sorted({r.variation for r in runs})
    sel_scenarios = _ask_select("Cenarios", scenarios)
    sel_models = _ask_select("Modelos", models)
    sel_variations = _ask_select("Variacoes", variations)
    return source, sel_scenarios, sel_models, sel_variations


def run(args: argparse.Namespace) -> Path:
    cfg = loadConfig()
    log = get_logger("model.eval.viz")

    source = args.source
    scenarios = args.scenarios
    models = args.models
    variations = args.variations
    if args.interactive:
        source, scenarios, models, variations = _interactive_select(cfg)

    discovered = discover_runs(cfg, source, scenarios, models, variations)
    runs = _pick_runs(discovered, args.pick)
    if not runs:
        raise RuntimeError("Nenhum run encontrado para os filtros informados.")
    log.info(f"runs selecionados: {len(runs)}")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    if source == "article":
        base = Path(cfg["paths"]["data"]["article"]) / "_model_eval_reports"
    else:
        base = Path(cfg["paths"]["data"]["modeling"]) / "results" / "model_eval_reports"
    out_dir = base / ts
    out_dir.mkdir(parents=True, exist_ok=True)

    cache: Dict[Tuple[str, str], ScenarioEvalData] = {}
    results: List[EvalResult] = []
    eval_by_label: Dict[str, EvalResult] = {}
    image_paths: List[Path] = []
    csv_map: Dict[str, Path] = {}

    for run_art in runs:
        log.info(f"avaliando {run_art.label}")
        if run_art.model_path is None:
            log.warning(f"skip eval sem model_*.joblib: {run_art.label}")
            continue
        sd = _resolve_eval_data(
            cache,
            run_art,
            batch_rows=args.batch_rows,
            max_train_rows=args.max_train_rows,
            max_test_rows=args.max_test_rows,
        )
        res = evaluate_run(run_art, sd, threshold=args.threshold)
        results.append(res)
        eval_by_label[run_art.label] = res

        if args.roc or args.both:
            image_paths.append(_plot_roc_single(res, out_dir))
        if args.pr or args.both:
            image_paths.append(_plot_pr_single(res, out_dir))
        if args.feature_importance:
            p = _plot_importance(res, out_dir)
            if p:
                image_paths.append(p)
        if args.shap:
            p = _plot_shap_summary(res, sd, out_dir, max_samples=args.shap_max_samples)
            if p:
                image_paths.append(p)

        if args.export_csv:
            artifacts = _write_csv_artifacts(res, out_dir)
            for k, p in artifacts.items():
                csv_map[f"{run_art.label}:{k}"] = p

    if args.overlay and results:
        if args.roc or args.both:
            image_paths.append(_plot_overlay(results, "roc", out_dir))
        if args.pr or args.both:
            image_paths.append(_plot_overlay(results, "pr", out_dir))

    rows_df = pd.DataFrame(_metrics_rows(runs, eval_by_label))
    cmp_csv = out_dir / "metrics_comparison.csv"
    rows_df.to_csv(cmp_csv, index=False, encoding="utf-8")
    csv_map["metrics_comparison"] = cmp_csv
    comparison_png: Optional[Path] = None
    if not rows_df.empty and rows_df["roc_auc_eval"].notna().any() and rows_df["pr_auc_eval"].notna().any():
        cmp_rows = rows_df[rows_df["roc_auc_eval"].notna() & rows_df["pr_auc_eval"].notna()].reset_index(drop=True)
        if not cmp_rows.empty:
            comparison_png = _plot_model_performance_comparison(cmp_rows, out_dir)
            image_paths.append(comparison_png)

    if args.report_md:
        md = _render_markdown(runs, eval_by_label, out_dir, csv_map, image_paths, comparison_png)
        (out_dir / "report.md").write_text(md, encoding="utf-8")

    latest = base / "LATEST.md"
    latest.write_text(
        "# Latest model eval report\n\n"
        f"- Report dir: `{out_dir}`\n"
        f"- Summary file: `{out_dir / 'report.md'}`\n",
        encoding="utf-8",
    )
    log.info(f"saida -> {out_dir}")
    return out_dir


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--source", choices=["tcc", "article"], default="tcc")
    p.add_argument("--interactive", action="store_true", help="Selecao interativa (fonte/cenarios/modelos/variacoes).")
    p.add_argument("--scenarios", nargs="+", default=None)
    p.add_argument("--models", nargs="+", default=None)
    p.add_argument("--variations", nargs="+", default=None)
    p.add_argument("--pick", choices=["latest", "all", "oldest"], default="latest")

    p.add_argument("--roc", action="store_true", help="Gerar curva ROC por run.")
    p.add_argument("--pr", action="store_true", help="Gerar curva PR por run.")
    p.add_argument("--both", action="store_true", help="Gerar ROC e PR por run.")
    p.add_argument("--overlay", action="store_true", help="Gerar overlays ROC/PR dos runs selecionados.")
    p.add_argument("--feature-importance", action="store_true")
    p.add_argument("--shap", action="store_true")
    p.add_argument("--shap-max-samples", type=int, default=10000)
    p.add_argument("--export-csv", action="store_true")
    p.add_argument("--report-md", action="store_true", default=True)
    p.add_argument("--threshold", type=float, default=None)
    p.add_argument("--batch-rows", type=int, default=None)
    p.add_argument("--max-train-rows", type=int, default=None)
    p.add_argument("--max-test-rows", type=int, default=None)
    return p


def main() -> None:
    p = build_arg_parser()
    args = p.parse_args()
    if not (args.roc or args.pr or args.both):
        args.both = True
    run(args)


if __name__ == "__main__":
    main()
