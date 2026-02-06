# src/ml/core.py
# =============================================================================
# NÚCLEO DE MACHINE LEARNING — PROJETO TCC (REFATORADO)
# =============================================================================

from __future__ import annotations

import json
import os
import time
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

# Utils (obrigatório no seu projeto)
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
    def calculate(y_true, y_pred, y_proba) -> Dict[str, Any]:
        """
        Metricas binarias robustas.
        - confusion_matrix com labels fixos garante sempre 2x2
        - ROC-AUC/PR-AUC protegidos quando y_true tem 1 classe no teste
        """
        if y_true is None or len(y_true) == 0:
            return {}

        y_true = np.asarray(y_true).astype(int)
        y_pred = np.asarray(y_pred).astype(int)
        y_proba = np.asarray(y_proba, dtype=float)

        cm = confusion_matrix(y_true, y_pred, labels=[0, 1])
        tn, fp, fn, tp = cm.ravel()
        denom = (tp + tn + fp + fn)
        acc = (tp + tn) / denom if denom > 0 else 0.0
        spec = tn / (tn + fp) if (tn + fp) > 0 else 0.0

        roc_auc = TCCMetrics._safe_metric(
            roc_auc_score, default=None, y_true=y_true, y_score=y_proba
        )
        pr_auc = TCCMetrics._safe_metric(
            average_precision_score, default=None, y_true=y_true, y_score=y_proba
        )
        brier = TCCMetrics._safe_metric(
            brier_score_loss, default=None, y_true=y_true, y_prob=y_proba
        )

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
# 4. OPTIMIZER (GRIDSEARCH) - ajustes de robustez e metadados
# -----------------------------------------------------------------------------
class ModelOptimizer:
    """
    GridSearchCV com TimeSeriesSplit.
    - SMOTE opcional (exige imbalanced-learn apenas quando ativado)
    - Scaler opcional (util p/ modelos lineares; desnecessario em arvores)
    - Ajustes para reduzir risco de estouro de memoria:
        * default n_jobs=1 (paralelismo costuma explodir RAM em CV)
        * pre_dispatch controlado
        * return_train_score=False (menos armazenamento)
        * error_score='raise' para falhar cedo e logar melhor
    - Exponibiliza metadados do ultimo GridSearch em self.last_search_meta
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
        """
        Evita y gigante como int64 sem necessidade.
        Importante para reduzir RAM quando sklearn faz copias internas.
        """
        arr = np.asarray(y)
        if arr.dtype != np.int8 and arr.dtype != np.int32 and arr.dtype != np.int64:
            arr = arr.astype(np.int8)
        else:
            # se vier int64, rebaixa
            if arr.dtype == np.int64:
                arr = arr.astype(np.int8)
        return arr

    def optimize(
        self,
        X,
        y,
        cv_splits: int = 3,
        use_smote: bool = False,
        use_scaler: bool = True,
        scoring: str = "average_precision",
        n_jobs: Optional[int] = None,
        verbose: int = 1,
        smote_sampling_strategy: float = 0.1,
        smote_k_neighbors: int = 5,
        pre_dispatch: str = "1*n_jobs",
        refit: bool = True,
        **_kwargs,
    ):
        # Compatibilidade com chamadas antigas: smote=True/False
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

        params = {f"model__{k}": v for k, v in self.grid.items()}
        tscv = TimeSeriesSplit(n_splits=int(cv_splits))

        # Importante: default conservador. Paralelismo de CV em dataset grande é receita pra RAM estourar.
        if n_jobs is None:
            n_jobs = 1

        cand = self._grid_candidates(self.grid)
        self.log.info(
            f"[GridSearch] scoring={scoring} | cv_splits={cv_splits} | candidates≈{cand} | "
            f"use_smote={use_smote} | use_scaler={use_scaler} | n_jobs={n_jobs} | pre_dispatch={pre_dispatch}"
        )
        MemoryMonitor.log_usage(self.log, "antes do GridSearch")

        y_norm = self._normalize_y(y)

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

        t0 = time.time()
        search.fit(X, y_norm)
        dt = time.time() - t0

        self.last_search_meta = {
            "scoring": scoring,
            "cv_splits": int(cv_splits),
            "candidates_approx": int(cand),
            "use_smote": bool(use_smote),
            "use_scaler": bool(use_scaler),
            "n_jobs": int(n_jobs),
            "pre_dispatch": str(pre_dispatch),
            "refit": bool(refit),
            "elapsed_s": float(dt),
            "best_score": None if getattr(search, "best_score_", None) is None else float(search.best_score_),
            "best_params": getattr(search, "best_params_", None),
        }

        self.log.info(
            f"[GridSearch] concluido em {dt:.1f}s | best_score={search.best_score_:.6f} | best_params={search.best_params_}"
        )
        MemoryMonitor.log_usage(self.log, "apos GridSearch")

        return search.best_estimator_


# -----------------------------------------------------------------------------
# 5. TRAINER BASE - variacoes profissionais + hierarquia de pastas consistente
# -----------------------------------------------------------------------------
class BaseModelTrainer(ABC):
    """
    Convencao de pastas:
      data/modeling/results/<MODEL_TYPE>/<VARIATION>/<SCENARIO>/
    """

    def __init__(self, scenario_name: str, model_type: str, random_state: int = 42):
        if utils is None:
            raise ImportError("Falha ao importar utils (src/utils.py). Ajuste seu PYTHONPATH/execucao como pacote.")

        self.cfg = utils.loadConfig()
        self.log = utils.get_logger(f"ml.{model_type}", kind="train", per_run_file=True)

        self.scenario = scenario_name
        self.model_type = model_type
        self.run_name = "base"
        self.random_state = int(random_state)

        self.variation_tags = []
        self.variation_desc = "Base (sem SMOTE, sem GridSearch, sem pesos)"

        self.model: Optional[Any] = None

        self._custom_run_name = False
        self._update_path()

    def _update_path(self) -> None:
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
            mapping = {
                "gridsearch": "GridSearchCV",
                "smote": "SMOTE",
                "weight": "balanceamento por peso",
            }
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

    def evaluate(self, X_test, y_test, thr: float = 0.5) -> Dict[str, Any]:
        if self.model is None:
            raise ValueError("Modelo nulo. Treine antes de avaliar.")

        if hasattr(self.model, "predict_proba"):
            probs = self.model.predict_proba(X_test)[:, 1]
        else:
            raise AttributeError("O modelo nao expoe predict_proba (necessario para este pipeline).")

        preds = (probs >= float(thr)).astype(int)
        metrics = TCCMetrics.calculate(y_test, preds, probs)

        pr_auc = metrics.get("pr_auc", None)
        roc_auc = metrics.get("roc_auc", None)
        brier = metrics.get("brier_score", None)

        self.log.info(f"[EVAL] thr={thr:.3f} | pr_auc={pr_auc} | roc_auc={roc_auc} | brier={brier}")

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
        # compress pequeno para nao inflar disco; se quiser, pode subir para 3/5 depois
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
