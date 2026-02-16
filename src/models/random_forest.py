# src/models/random_forest.py
# =============================================================================
# MODELO: RANDOM FOREST (SMOTE / PESOS / GRIDSEARCH) - PADRAO DO PROJETO
# =============================================================================

import time
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None

# sklearn pipeline (para manter estrutura consistente quando nao usar SMOTE)
from sklearn.pipeline import Pipeline as SkPipeline


class RandomForestTrainer(BaseModelTrainer):
    """
    Random Forest com variacoes consistentes:
      - base
      - weight (class_weight)
      - smote
      - smote_weight
      - gridsearch
      - gridsearch_smote
      - gridsearch_weight
      - gridsearch_smote_weight

    Observacoes praticas:
      - Para bases muito grandes, RF pode consumir bastante RAM.
      - Em GridSearch, forcar n_jobs=1 no modelo ajuda a evitar picos.
      - SMOTE em RF e possivel, mas costuma ser caro; weight geralmente e o primeiro a tentar.
    """

    def __init__(
        self,
        scenario_name: str,
        random_state: int = 42,
        n_estimators: int = 300,
        max_depth: Optional[int] = None,
        min_samples_leaf: int = 1,
    ):
        super().__init__(scenario_name, "RandomForest", random_state)

        self.n_estimators = int(n_estimators)
        self.max_depth = max_depth
        self.min_samples_leaf = int(min_samples_leaf)

        # Grid conservador para nao explodir custo em dataset grande
        self.param_grid = {
            "n_estimators": [200, 400],
            "max_depth": [None, 12, 24],
            "min_samples_leaf": [1, 3, 5],
            "max_features": ["sqrt", 0.5],
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
        class_weight_mode: str = "balanced_subsample",
        **kwargs,
    ):
        """
        Args:
            optimize: ativa GridSearchCV com TimeSeriesSplit.
            use_smote: ativa SMOTE dentro do pipeline (fast ou grid).
            use_scale: aqui significa balanceamento por peso (class_weight).
            class_weight_mode: "balanced" ou "balanced_subsample".
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        cw = None
        if use_scale:
            mode = str(class_weight_mode or "balanced_subsample").strip().lower()
            cw = "balanced" if mode == "balanced" else "balanced_subsample"

        # Em GridSearch, manter RF serial reduz pico de CPU e RAM
        model_n_jobs = 1 if optimize else -1

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(class_weight)={use_scale} | "
            f"class_weight={cw} | cv_splits={cv_splits} | scoring={scoring} | model_n_jobs={model_n_jobs}"
        )

        base_model = RandomForestClassifier(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            min_samples_leaf=self.min_samples_leaf,
            class_weight=cw,
            random_state=self.random_state,
            n_jobs=model_n_jobs,
            # defaults explicitos para reduzir custo extra
            oob_score=False,
            bootstrap=True,
        )

        t0 = time.time()

        if optimize:
            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, seed=self.random_state)
            self.model = optimizer.optimize(
                X_train,
                y_train,
                cv_splits=cv_splits,
                use_smote=use_smote,
                use_scaler=False,  # RF nao precisa scaler
                scoring=scoring,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
                n_jobs=1,          # importante: serial no CV para poupar RAM
                verbose=1,
            )
        else:
            steps = []
            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn nao esta instalado.")
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

            steps.append(("model", base_model))
            pipe = (ImbPipeline if use_smote else SkPipeline)(steps)
            self.model = pipe
            self.model.fit(X_train, y_train)

            self.log.info("[TRAIN] Treinamento direto concluido (fast).")

        dt = time.time() - t0
        MemoryMonitor.log_usage(self.log, f"apos treino ({dt:.1f}s)")

        self._log_importances(getattr(X_train, "columns", None))

    def _log_importances(self, feature_names: Optional[pd.Index]):
        """Loga top importancias por feature_importances_."""
        if feature_names is None:
            return

        try:
            model_obj = self.model
            if hasattr(model_obj, "named_steps") and "model" in model_obj.named_steps:
                clf = model_obj.named_steps["model"]
            else:
                clf = model_obj

            if hasattr(clf, "feature_importances_"):
                importances = clf.feature_importances_
                feat_dict = dict(zip(list(feature_names), importances))
                top = sorted(feat_dict.items(), key=lambda x: x[1], reverse=True)[:10]
                self.log.info(f"[FI] Top 10 features (feature_importances_): {top}")
        except Exception as e:
            self.log.warning(f"[FI] Falha ao extrair importancias: {e}")
