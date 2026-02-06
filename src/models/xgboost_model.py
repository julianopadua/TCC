# src/models/xgboost_model.py
# =============================================================================
# MODELO: XGBOOST (SMOTE / PESOS / GRIDSEARCH) — NOMENCLATURA PROFISSIONAL
# =============================================================================

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
    """

    def __init__(self, scenario_name: str, random_state: int = 42):
        super().__init__(scenario_name, "XGBoost", random_state)

        # Grid enxuto (evita "monster grid" sem controle)
        self.param_grid = {
            "n_estimators": [200, 400],
            "max_depth": [3, 6, 10],
            "learning_rate": [0.01, 0.1],
            "subsample": [0.8, 1.0],
            "colsample_bytree": [0.8, 1.0],
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
        **kwargs,
    ):
        """
        Args:
            optimize: ativa GridSearchCV com TimeSeriesSplit.
            use_smote: ativa SMOTE dentro do pipeline (fast ou grid).
            use_scale: aqui significa balanceamento por peso (scale_pos_weight).
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        n_pos = int(np.sum(y_train))
        n_neg = int(len(y_train) - n_pos)

        scale_pos_weight = 1.0
        if use_scale and n_pos > 0:
            scale_pos_weight = n_neg / n_pos

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(scale_pos_weight)={use_scale} | "
            f"scale_pos_weight={scale_pos_weight:.4f} | cv_splits={cv_splits} | scoring={scoring}"
        )

        # Observação: scaler não é necessário para árvores/boosting (economiza custo).
        use_scaler_in_optimizer = False

        # n_jobs: em GridSearch, manter 1 costuma evitar explosão de memória
        model_n_jobs = 1 if optimize else -1

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
            n_jobs=model_n_jobs,
            verbosity=0,
        )

        t0 = time.time()

        if optimize:
            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, seed=self.random_state)
            self.model = optimizer.optimize(
                X_train,
                y_train,
                cv_splits=cv_splits,
                use_smote=use_smote,
                use_scaler=use_scaler_in_optimizer,
                scoring=scoring,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
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
        MemoryMonitor.log_usage(self.log, f"após treino ({dt:.1f}s)")

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
