# src/models/logistic.py
# =============================================================================
# MODELO: REGRESSÃO LOGÍSTICA (SMOTE / PESOS / GRIDSEARCH) — NOMENCLATURA PROFISSIONAL
# =============================================================================

import time
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.preprocessing import StandardScaler

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None


class LogisticTrainer(BaseModelTrainer):
    """
    Regressão Logística com variações consistentes:
      - base
      - weight (class_weight='balanced')
      - smote
      - smote_weight
      - gridsearch
      - gridsearch_smote
      - gridsearch_weight
      - gridsearch_smote_weight
    """

    def __init__(
        self,
        scenario_name: str,
        random_state: int = 42,
        C: float = 1.0,
        max_iter: int = 1000,
    ):
        super().__init__(scenario_name, "LogisticRegression", random_state)
        self.C = float(C)
        self.max_iter = int(max_iter)

        # Grid mais "acadêmico" e controlado (sem explosão combinatória)
        self.param_grid = {
            "C": [0.01, 0.1, 1.0, 10.0, 100.0],
            "penalty": ["l2", "l1"],
            "max_iter": [1000, 2000],
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
        feature_scaling: bool = True,
        **kwargs,
    ):
        """
        Args:
            optimize: ativa GridSearchCV com TimeSeriesSplit.
            use_smote: ativa SMOTE dentro do pipeline (fast ou grid).
            use_scale: aqui significa balanceamento por peso (class_weight='balanced').
            feature_scaling: StandardScaler (recomendado para modelos lineares).
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(class_weight)={use_scale} | "
            f"feature_scaling={feature_scaling} | cv_splits={cv_splits} | scoring={scoring}"
        )

        class_weight = "balanced" if use_scale else None

        base_model = LogisticRegression(
            C=self.C,
            penalty="l2",              # Grid pode trocar para l1 também
            class_weight=class_weight,
            solver="saga",             # suporta l1/l2, bom em datasets maiores
            max_iter=self.max_iter,
            random_state=self.random_state,
            n_jobs=-1,
        )

        t0 = time.time()

        if optimize:
            # GridSearch: SMOTE e scaler são decididos por parâmetros do optimizer
            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, seed=self.random_state)
            self.model = optimizer.optimize(
                X_train,
                y_train,
                cv_splits=cv_splits,
                use_smote=use_smote,
                use_scaler=feature_scaling,
                scoring=scoring,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )
        else:
            # Fast: pipeline (com ou sem SMOTE) funcionando de verdade
            steps = []
            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn não está instalado.")
                steps.append(
                    (
                        "smote",
                        SMOTE(
                            sampling_strategy=float(smote_sampling_strategy),
                            random_state=self.random_state,
                            k_neighbors=int(smote_k_neighbors),
                        ),
                    )
                )

            if feature_scaling:
                steps.append(("scaler", StandardScaler()))

            steps.append(("model", base_model))

            pipe = (ImbPipeline if use_smote else SkPipeline)(steps)
            self.model = pipe
            self.model.fit(X_train, y_train)

            self.log.info("[TRAIN] Treinamento direto concluído (fast).")

        dt = time.time() - t0
        MemoryMonitor.log_usage(self.log, f"após treino ({dt:.1f}s)")

        self._log_coefficients(getattr(X_train, "columns", None))

    def _log_coefficients(self, feature_names: Optional[pd.Index]):
        """Extrai e loga top coeficientes (em módulo)."""
        if feature_names is None:
            return

        try:
            model_obj = self.model

            # Se vier de pipeline
            if hasattr(model_obj, "named_steps") and "model" in model_obj.named_steps:
                clf = model_obj.named_steps["model"]
            else:
                clf = model_obj

            if hasattr(clf, "coef_"):
                coefs = clf.coef_[0]
                coef_dict = dict(zip(list(feature_names), coefs))
                top = sorted(coef_dict.items(), key=lambda x: abs(x[1]), reverse=True)[:10]
                self.log.info(f"[COEF] Top 10 coeficientes (|coef|): {top}")
        except Exception as e:
            self.log.warning(f"[COEF] Não foi possível extrair coeficientes: {e}")
