# src/ml/core.py
# =============================================================================
# NUCLEO DE MACHINE LEARNING - PROJETO TCC (REFATORADO)
# =============================================================================

from __future__ import annotations

import os

os.environ.setdefault("MPLBACKEND", "Agg")

import json
import time
import warnings
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import joblib
import numpy as np
import pandas as pd

# Optional deps (robustez)
try:
    import psutil  # type: ignore
except Exception:
    psutil = None

try:
    import matplotlib.pyplot as plt  # type: ignore
    import seaborn as sns  # type: ignore
except Exception:
    plt = None
    sns = None

# Scikit-Learn
from sklearn.base import BaseEstimator
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.preprocessing import StandardScaler

# Imbalanced-learn (opcional)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None

# Utils (obrigatorio no seu projeto)
try:
    import src.utils as utils  # type: ignore
except Exception:
    try:
        import utils  # type: ignore
    except Exception:
        utils = None


# -----------------------------------------------------------------------------
# 1. MONITORAMENTO
# -----------------------------------------------------------------------------
class MemoryMonitor:
    @staticmethod
    def get_usage() -> str:
        if psutil is None:
            return "psutil_not_installed"
        p = psutil.Process(os.getpid())
        return f"{p.memory_info().rss / (1024 ** 3):.2f} GB"

    @staticmethod
    def log_usage(log, ctx: str = "") -> None:
        log.info(f"[MEMORIA] {ctx}: {MemoryMonitor.get_usage()}")


# -----------------------------------------------------------------------------
# 2. METRICAS
# -----------------------------------------------------------------------------
class TCCMetrics:
    @staticmethod
    def _safe_metric(fn, default=None, **kwargs):
        try:
            return fn(**kwargs)
        except Exception:
            return default

    @staticmethod
    def _clip_proba(arr: np.ndarray) -> np.ndarray:
        # Evita NaN e valores fora de [0, 1] por efeitos numericos
        a = np.asarray(arr, dtype=float)
        a = np.nan_to_num(a, nan=0.0, posinf=1.0, neginf=0.0)
        return np.clip(a, 0.0, 1.0)

    @staticmethod
    def calculate(y_true, y_pred, y_proba) -> Dict[str, Any]:
        if y_true is None or len(y_true) == 0:
            return {}

        y_true = np.asarray(y_true).astype(int)
        y_pred = np.asarray(y_pred).astype(int)
        y_proba = TCCMetrics._clip_proba(np.asarray(y_proba, dtype=float))

        cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel()

        denom = (tp + tn + fp + fn)
        acc = (tp + tn) / denom if denom > 0 else 0.0
        spec = tn / (tn + fp) if (tn + fp) > 0 else 0.0

        roc_auc = TCCMetrics._safe_metric(roc_auc_score, default=None, y_true=y_true, y_score=y_proba)
        pr_auc = TCCMetrics._safe_metric(average_precision_score, default=None, y_true=y_true, y_score=y_proba)
        brier = TCCMetrics._safe_metric(brier_score_loss, default=None, y_true=y_true, y_prob=y_proba)

        return {
            "accuracy": float(acc),
            "precision": float(precision_score(y_true, y_pred, zero_division=0)),
            "recall": float(recall_score(y_true, y_pred, zero_division=0)),
            "f1": float(f1_score(y_true, y_pred, zero_division=0)),
            "specificity": float(spec),
            "roc_auc": None if roc_auc is None else float(roc_auc),
            "pr_auc": None if pr_auc is None else float(pr_auc),
            "brier_score": None if brier is None else float(brier),
            "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
        }

    @staticmethod
    def plot_cm(cm: Dict[str, int], path: Path, title: str) -> None:
        if plt is None or sns is None:
            return

        arr = np.array([[cm["tn"], cm["fp"]], [cm["fn"], cm["tp"]]])
        plt.figure(figsize=(5, 4))
        sns.heatmap(arr, annot=True, fmt="d", cmap="Blues", cbar=False)
        plt.title(title)
        plt.tight_layout()
        plt.savefig(path)
        plt.close()


# -----------------------------------------------------------------------------
# 3. SPLITTER
# -----------------------------------------------------------------------------
class TemporalSplitter:
    def __init__(self, test_size_years: int = 2, gap_years: int = 0):
        self.test = int(test_size_years)
        self.gap = int(gap_years)

    def split_holdout(self, df: pd.DataFrame, col: str = "ANO") -> Tuple[pd.DataFrame, pd.DataFrame]:
        years = sorted(df[col].unique())
        if len(years) < self.test + 1:
            raise ValueError("Anos insuficientes para split temporal.")
        cut = years[-self.test]
        train = df[df[col] < (cut - self.gap)].copy()
        test = df[df[col] >= cut].copy()
        return train, test


# -----------------------------------------------------------------------------
# 4. OPTIMIZER (GRIDSEARCH)
# -----------------------------------------------------------------------------
class ModelOptimizer:
    """
    GridSearchCV com TimeSeriesSplit.
    Melhorias:
      - Log explicito do equivalente a: "Fitting K folds for each of N candidates, totalling K*N fits"
      - Diagnostico de folds sem positivo (eventos raros + TimeSeriesSplit)
      - Captura e log de warnings relevantes
      - Permite fit_params (ex: model__sample_weight) no search.fit
      - Defaults conservadores para CPU/RAM
    """

    def __init__(self, estimator: BaseEstimator, grid: Dict[str, Any], log, seed: int = 42):
        self.est = estimator
        self.grid = grid
        self.log = log
        self.seed = int(seed)
        self.last_search_meta: Dict[str, Any] = {}

    @staticmethod
    def _grid_candidates(grid: Dict[str, Any]) -> int:
        total = 1
        for _, v in grid.items():
            try:
                total *= len(v)
            except Exception:
                total *= 1
        return int(total)

    @staticmethod
    def _normalize_y(y) -> np.ndarray:
        # Reduz RAM (int8) e evita int64 gigante
        arr = np.asarray(y)
        if arr.dtype == np.int64:
            return arr.astype(np.int8)
        if arr.dtype not in (np.int8, np.int32):
            return arr.astype(np.int8)
        return arr

    @staticmethod
    def _fold_pos_counts(tscv: TimeSeriesSplit, y: np.ndarray) -> Tuple[int, list]:
        # Retorna (num_folds_com_zero_pos, lista_de_dicts_por_fold)
        details = []
        zero_pos = 0
        for i, (_, te_idx) in enumerate(tscv.split(np.zeros(len(y))), start=1):
            y_te = y[te_idx]
            pos = int(np.sum(y_te == 1))
            neg = int(np.sum(y_te == 0))
            if pos == 0:
                zero_pos += 1
            details.append({"fold": i, "test_size": int(len(te_idx)), "pos": pos, "neg": neg})
        return zero_pos, details

    def optimize(
        self,
        X,
        y,
        cv_splits: int = 3,
        use_smote: bool = False,
        use_scaler: bool = True,
        scoring: str = "average_precision",
        n_jobs: Optional[int] = None,
        verbose: int = 0,
        smote_sampling_strategy: float = 0.1,
        smote_k_neighbors: int = 5,
        pre_dispatch: str = "1*n_jobs",
        refit: bool = True,
        fit_params: Optional[Dict[str, Any]] = None,
        **_kwargs,
    ):
        # Compat: permitir "smote=True" legado
        if "smote" in _kwargs and "use_smote" not in _kwargs:
            use_smote = bool(_kwargs["smote"])

        steps = []

        if use_smote:
            if SMOTE is None or ImbPipeline is None:
                raise ImportError("SMOTE solicitado, mas imbalanced-learn nao esta instalado.")
            steps.append(
                (
                    "smote",
                    SMOTE(
                        sampling_strategy=float(smote_sampling_strategy),
                        random_state=self.seed,
                        k_neighbors=int(smote_k_neighbors),
                    ),
                )
            )

        if use_scaler:
            steps.append(("scaler", StandardScaler()))

        steps.append(("model", self.est))
        pipe = (ImbPipeline if use_smote else SkPipeline)(steps)

        # Param grid no formato do Pipeline (prefixo model__)
        params = {f"model__{k}": v for k, v in self.grid.items()}

        if n_jobs is None:
            n_jobs = 1

        y_norm = self._normalize_y(y)

        # Checagem de folds (TimeSeriesSplit)
        effective_cv = int(cv_splits)
        while True:
            if effective_cv < 2:
                break
            tscv0 = TimeSeriesSplit(n_splits=effective_cv)
            zero_pos, fold_details = self._fold_pos_counts(tscv0, y_norm)
            if zero_pos == 0:
                break
            self.log.warning(
                f"[GridSearch][WARN] TimeSeriesSplit com cv_splits={effective_cv} gerou {zero_pos} fold(s) sem positivos no y_true do teste. "
                f"Isto degrada o scoring={scoring}. Folds: {fold_details}"
            )
            effective_cv -= 1

        if effective_cv < 2:
            raise RuntimeError(
                f"[GridSearch][ERRO] Nao foi possivel montar CV temporal com folds contendo positivos. "
                f"cv_splits original={cv_splits}. Sugestao: usar menos splits, ajustar janela temporal, ou usar SMOTE/estrategia por ano."
            )

        tscv = TimeSeriesSplit(n_splits=effective_cv)

        cand = self._grid_candidates(self.grid)
        total_fits = int(effective_cv) * int(cand)

        self.log.info(f"[GridSearch] Fitting {effective_cv} folds for each of {cand} candidates, totalling {total_fits} fits")
        self.log.info(
            f"[GridSearch] scoring={scoring} | cv_splits={effective_cv} | candidates≈{cand} | "
            f"use_smote={use_smote} | use_scaler={use_scaler} | n_jobs={n_jobs} | pre_dispatch={pre_dispatch}"
        )
        if fit_params:
            self.log.info(f"[GridSearch] fit_params_keys={list(fit_params.keys())}")

        MemoryMonitor.log_usage(self.log, "antes do GridSearch")

        search = GridSearchCV(
            estimator=pipe,
            param_grid=params,
            cv=tscv,
            scoring=scoring,
            n_jobs=int(n_jobs),
            verbose=int(verbose),
            refit=bool(refit),
            return_train_score=False,
            pre_dispatch=pre_dispatch,
            error_score="raise",
        )

        warn_counts: Dict[str, int] = {"no_positive_class": 0, "convergence": 0, "other": 0}

        t0 = time.time()
        with warnings.catch_warnings(record=True) as wlist:
            warnings.simplefilter("always")
            if fit_params:
                search.fit(X, y_norm, **fit_params)
            else:
                search.fit(X, y_norm)

            for w in wlist:
                msg = str(w.message)
                cat = getattr(w, "category", None)
                cname = cat.__name__ if cat is not None else "Warning"

                if "No positive class found in y_true" in msg:
                    warn_counts["no_positive_class"] += 1
                    self.log.warning(f"[GridSearch][WARN] {cname}: {msg}")
                elif "did not converge" in msg or "max_iter was reached" in msg:
                    warn_counts["convergence"] += 1
                    self.log.warning(f"[GridSearch][WARN] {cname}: {msg}")
                else:
                    warn_counts["other"] += 1
                    self.log.info(f"[GridSearch][WARN-OTHER] {cname}: {msg}")

        dt = time.time() - t0

        self.last_search_meta = {
            "scoring": scoring,
            "cv_splits_requested": int(cv_splits),
            "cv_splits_effective": int(effective_cv),
            "candidates_approx": int(cand),
            "use_smote": bool(use_smote),
            "use_scaler": bool(use_scaler),
            "n_jobs": int(n_jobs),
            "pre_dispatch": str(pre_dispatch),
            "refit": bool(refit),
            "elapsed_s": float(dt),
            "best_score": None if getattr(search, "best_score_", None) is None else float(search.best_score_),
            "best_params": getattr(search, "best_params_", None),
            "warnings": warn_counts,
            "fit_params_keys": [] if not fit_params else list(fit_params.keys()),
        }

        self.log.info(
            f"[GridSearch] concluido em {dt:.1f}s | best_score={search.best_score_:.6f} | best_params={search.best_params_} | warnings={warn_counts}"
        )
        MemoryMonitor.log_usage(self.log, "apos GridSearch")

        return search.best_estimator_


# -----------------------------------------------------------------------------
# 5. TRAINER BASE
# -----------------------------------------------------------------------------
class BaseModelTrainer(ABC):
    """
    Convencao de pastas:
      data/modeling/results/<MODEL_TYPE>/<VARIATION>/<SCENARIO>/
    Com ``article_results=True``:
      data/_article/<train_runner_results_subdir>/<MODEL_TYPE>/<VARIATION>/<SCENARIO>/
      (subdir default: ``_results``, ver ``article_pipeline.train_runner_results_subdir``).
    """

    def __init__(
        self,
        scenario_name: str,
        model_type: str,
        random_state: int = 42,
        *,
        article_results: bool = False,
    ):
        if utils is None:
            raise ImportError("Falha ao importar utils (src/utils.py). Ajuste seu PYTHONPATH/execucao como pacote.")

        self.cfg = utils.loadConfig()
        self.log = utils.get_logger(f"ml.{model_type}", kind="train", per_run_file=True)

        self.scenario = scenario_name
        self.model_type = model_type
        self.run_name = "base"
        self.random_state = int(random_state)
        self.article_results = bool(article_results)

        self.variation_tags = []
        self.variation_desc = "Base (sem SMOTE, sem GridSearch, sem pesos)"

        self.model: Optional[Any] = None

        self._custom_run_name = False
        self._update_path()

    @staticmethod
    def _article_train_results_subdir(cfg: Dict[str, Any]) -> Path:
        ap = cfg.get("article_pipeline") or {}
        raw = str(ap.get("train_runner_results_subdir", "_results")).strip() or "_results"
        p = Path(raw)
        if p.is_absolute() or ".." in p.parts:
            return Path("_results")
        return p

    def _update_path(self) -> None:
        if self.article_results:
            base = Path(self.cfg["paths"]["data"]["article"])
            sub = self._article_train_results_subdir(self.cfg)
            self.output_dir = base / sub / self.model_type / self.run_name / self.scenario
        else:
            base = Path(self.cfg["paths"]["data"]["modeling"])
            self.output_dir = base / "results" / self.model_type / self.run_name / self.scenario
        utils.ensure_dir(self.output_dir)

    def set_custom_folder_name(self, variation_name: str) -> None:
        self.run_name = str(variation_name)
        self._custom_run_name = True
        self._update_path()

    def _auto_set_variation(self, optimize: bool, use_smote: bool, use_scale: bool) -> None:
        if self._custom_run_name:
            return

        tags = []
        if optimize:
            tags.append("gridsearch")
        if use_smote:
            tags.append("smote")
        if use_scale:
            tags.append("weight")

        self.variation_tags = tags[:]
        self.run_name = "base" if not tags else "_".join(tags)

        if not tags:
            self.variation_desc = "Base (sem SMOTE, sem GridSearch, sem balanceamento por peso)"
        else:
            mapping = {"gridsearch": "GridSearchCV", "smote": "SMOTE", "weight": "balanceamento por peso"}
            self.variation_desc = " + ".join(mapping[t] for t in tags)

        self._update_path()

    def _log_dataset_header(self, X: pd.DataFrame, y: pd.Series) -> None:
        n = len(y)
        pos = int(np.sum(y))
        rate = (pos / n) if n > 0 else 0.0
        self.log.info(f"[RUN] model_type={self.model_type} | variation={self.run_name} | scenario={self.scenario}")
        self.log.info(f"[DATA] X={getattr(X, 'shape', None)} | y={getattr(y, 'shape', None)} | pos={pos}/{n} ({rate:.4%})")
        MemoryMonitor.log_usage(self.log, "inicio do treino")

    @abstractmethod
    def train(self, X, y, **kwargs):
        pass

    @staticmethod
    def _sigmoid(x: np.ndarray) -> np.ndarray:
        x = np.asarray(x, dtype=float)
        x = np.nan_to_num(x, nan=0.0, posinf=50.0, neginf=-50.0)
        x = np.clip(x, -50.0, 50.0)
        return 1.0 / (1.0 + np.exp(-x))

    def _predict_proba_like(self, X_test) -> Tuple[np.ndarray, str]:
        """
        Retorna (probs, source).
        Preferencia:
          1) predict_proba
          2) decision_function -> sigmoid (probabilidade aproximada)
        """
        if self.model is None:
            raise ValueError("Modelo nulo. Treine antes de avaliar.")

        if hasattr(self.model, "predict_proba"):
            p = self.model.predict_proba(X_test)[:, 1]
            return TCCMetrics._clip_proba(p), "predict_proba"

        if hasattr(self.model, "decision_function"):
            s = self.model.decision_function(X_test)
            p = self._sigmoid(s)
            return TCCMetrics._clip_proba(p), "decision_function_sigmoid"

        raise AttributeError("O modelo nao expoe predict_proba nem decision_function (necessario para este pipeline).")

    def evaluate(self, X_test, y_test, thr: float = 0.5) -> Dict[str, Any]:
        probs, source = self._predict_proba_like(X_test)

        preds = (probs >= float(thr)).astype(int)
        metrics = TCCMetrics.calculate(y_test, preds, probs)

        metrics["proba_source"] = source
        metrics["threshold"] = float(thr)

        pr_auc = metrics.get("pr_auc", None)
        roc_auc = metrics.get("roc_auc", None)
        brier = metrics.get("brier_score", None)

        self.log.info(f"[EVAL] thr={thr:.3f} | pr_auc={pr_auc} | roc_auc={roc_auc} | brier={brier} | proba_source={source}")

        cm = metrics.get("confusion_matrix", None)
        if cm is not None:
            cm_path = self.output_dir / f"cm_{datetime.now():%Y%m%d_%H%M%S}.png"
            TCCMetrics.plot_cm(cm, cm_path, f"{self.model_type} | {self.run_name}\n{self.scenario}")

        return metrics

    def save_artifacts(
        self,
        metrics: Dict[str, Any],
        feat_imp: Optional[Dict[str, Any]] = None,
        run_meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        if self.model is None:
            raise ValueError("Modelo nulo. Nada para salvar.")

        ts = datetime.now().strftime("%Y%m%d_%H%M%S")

        model_path = self.output_dir / f"model_{ts}.joblib"
        joblib.dump(self.model, model_path, compress=1)

        payload = {
            "model_type": self.model_type,
            "variation": self.run_name,
            "variation_tags": self.variation_tags,
            "variation_desc": self.variation_desc,
            "scenario": self.scenario,
            "timestamp": ts,
            "metrics": metrics,
            "importance": feat_imp,
            "run_meta": run_meta or {},
        }

        metrics_path = self.output_dir / f"metrics_{ts}.json"
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=4, ensure_ascii=False)

        self.log.info(f"[SAVE] model={model_path.name} | metrics={metrics_path.name} | dir={self.output_dir}")
        MemoryMonitor.log_usage(self.log, "apos salvar")
