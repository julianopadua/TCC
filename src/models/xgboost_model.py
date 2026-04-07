# src/models/xgboost_model.py
# =============================================================================
# MODELO: XGBOOST (SMOTE / PESOS / GRIDSEARCH) - NOMENCLATURA PROFISSIONAL
# =============================================================================

import os
import time
from typing import Optional

import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None


class XGBoostTrainer(BaseModelTrainer):
    """
    XGBoost com variações consistentes:
      - base
      - weight (scale_pos_weight)
      - smote
      - smote_weight
      - gridsearch
      - gridsearch_smote
      - gridsearch_weight
      - gridsearch_smote_weight

    Novos controles:
      - grid_mode: "full" ou "fast" (reduz candidatos e fits)
      - model_n_jobs: threads no fit do XGBoost (GridSearch segue serial em n_jobs=1 para poupar RAM)
    """

    def __init__(self, scenario_name: str, random_state: int = 42):
        super().__init__(scenario_name, "XGBoost", random_state)

        # Grid FULL (o seu atual)
        self.param_grid_full = {
            "n_estimators": [200, 400],
            "max_depth": [3, 6, 10],
            "learning_rate": [0.01, 0.1],
            "subsample": [0.8, 1.0],
            "colsample_bytree": [0.8, 1.0],
        }

        # Grid FAST (bem menor: tipicamente 16 candidatos; com cv=2 => ~32 fits)
        self.param_grid_fast = {
            "n_estimators": [200],
            "max_depth": [3, 6],
            "learning_rate": [0.05, 0.1],
            "subsample": [0.9, 1.0],
            "colsample_bytree": [0.9, 1.0],
        }

    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        optimize: bool = False,
        use_smote: bool = False,
        use_scale: bool = True,
        cv_splits: int = 3,
        scoring: str = "average_precision",
        smote_sampling_strategy: float = 0.1,
        smote_k_neighbors: int = 5,
        grid_mode: str = "full",
        model_n_jobs: Optional[int] = None,
        **kwargs,
    ):
        """
        Args:
            optimize: ativa GridSearchCV com TimeSeriesSplit.
            use_smote: ativa SMOTE dentro do pipeline (fast ou grid).
            use_scale: aqui significa balanceamento por peso (scale_pos_weight).
            grid_mode: "full" ou "fast".
            model_n_jobs: threads no fit do XGBoost.
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        n_pos = int(np.sum(y_train))
        n_neg = int(len(y_train) - n_pos)

        scale_pos_weight = 1.0
        if use_scale and n_pos > 0:
            scale_pos_weight = n_neg / n_pos

        # Para grid, usa mais CPU dentro do fit (GridSearch continua serial para evitar RAM explodir)
        if model_n_jobs is None:
            cpu = os.cpu_count() or 2
            model_n_jobs = max(1, min(8, cpu - 1))

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(scale_pos_weight)={use_scale} | "
            f"scale_pos_weight={scale_pos_weight:.4f} | cv_splits={cv_splits} | scoring={scoring} | "
            f"grid_mode={grid_mode} | model_n_jobs={model_n_jobs}"
        )

        # Observação: scaler não é necessário para árvores/boosting (economiza custo).
        use_scaler_in_optimizer = False

        # Escolhe grade
        grid_mode_norm = str(grid_mode or "full").strip().lower()
        param_grid = self.param_grid_fast if grid_mode_norm == "fast" else self.param_grid_full

        # n_jobs do modelo:
        # - em GridSearch: multithread dentro do fit, mas GridSearch n_jobs=1 (no optimizer)
        # - em treino direto: pode usar -1 (todas as threads) se quiser, mas aqui mantemos model_n_jobs
        n_jobs_for_model = int(model_n_jobs) if optimize else int(model_n_jobs)

        base_model = XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            subsample=1.0,
            colsample_bytree=1.0,
            scale_pos_weight=float(scale_pos_weight),
            objective="binary:logistic",
            eval_metric="aucpr",
            tree_method="hist",
            random_state=self.random_state,
            n_jobs=n_jobs_for_model,
            verbosity=0,
        )

        t0 = time.time()

        if optimize:
            optimizer = ModelOptimizer(base_model, param_grid, self.log, seed=self.random_state)
            self.model = optimizer.optimize(
                X_train,
                y_train,
                cv_splits=cv_splits,
                use_smote=use_smote,
                use_scaler=use_scaler_in_optimizer,
                scoring=scoring,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
                n_jobs=1,   # importante: serial no CV para poupar RAM
                verbose=1,
            )
        else:
            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn não está instalado.")
                self.model = ImbPipeline(
                    [
                        (
                            "smote",
                            SMOTE(
                                sampling_strategy=float(smote_sampling_strategy),
                                random_state=self.random_state,
                                k_neighbors=int(smote_k_neighbors),
                            ),
                        ),
                        ("model", base_model),
                    ]
                )
                self.model.fit(X_train, y_train)
            else:
                self.model = base_model
                self.model.fit(X_train, y_train)

            self.log.info("[TRAIN] Treinamento direto concluído (fast).")

        dt = time.time() - t0
        MemoryMonitor.log_usage(self.log, f"apos treino ({dt:.1f}s)")

        self._log_importances(getattr(X_train, "columns", None))

    def _log_importances(self, feature_names: Optional[pd.Index]):
        """Loga top importâncias por feature_importances_ (rápido e compatível)."""
        if feature_names is None:
            return

        try:
            model_obj = self.model
            if hasattr(model_obj, "named_steps") and "model" in model_obj.named_steps:
                booster = model_obj.named_steps["model"]
            else:
                booster = model_obj

            if hasattr(booster, "feature_importances_"):
                importances = booster.feature_importances_
                feat_dict = dict(zip(list(feature_names), importances))
                top = sorted(feat_dict.items(), key=lambda x: x[1], reverse=True)[:10]
                self.log.info(f"[FI] Top 10 features (feature_importances_): {top}")
        except Exception as e:
            self.log.warning(f"[FI] Falha ao extrair importâncias: {e}")
