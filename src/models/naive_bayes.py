# src/models/naive_bayes.py
# =============================================================================
# MODELO: NAIVE BAYES (GAUSSIANNB) - SMOTE / PESOS / GRIDSEARCH (PADRAO DO PROJETO)
# =============================================================================

import time
from typing import Optional

import numpy as np
import pandas as pd
from sklearn.naive_bayes import GaussianNB

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None

# sklearn pipeline
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.preprocessing import StandardScaler


class NaiveBayesTrainer(BaseModelTrainer):
    """
    Naive Bayes Gaussiano com variacoes consistentes:
      - base
      - weight (amostragem por peso via sample_weight)
      - smote
      - smote_weight
      - gridsearch
      - gridsearch_smote
      - gridsearch_weight
      - gridsearch_smote_weight

    Observacoes:
      - GaussianNB suporta predict_proba, entao encaixa direto no pipeline do projeto.
      - Nao tem class_weight nativo; aqui implementamos "weight" via sample_weight no fit.
      - Em GridSearch, o sklearn Pipeline nao passa sample_weight automaticamente para o step model,
        entao a variante "weight" com optimize=True e tratada como no-op (log explicito).
      - SMOTE e opcional, mas para NB pode ajudar quando o evento e muito raro.
    """

    def __init__(
        self,
        scenario_name: str,
        random_state: int = 42,
        var_smoothing: float = 1e-9,
    ):
        super().__init__(scenario_name, "NaiveBayes", random_state)
        self.var_smoothing = float(var_smoothing)

        # Grid pequeno e barato
        self.param_grid = {"var_smoothing": [1e-12, 1e-10, 1e-9, 1e-8]}

    @staticmethod
    def _build_sample_weight(y: pd.Series) -> np.ndarray:
        """
        Pondera para aproximar equilibrio:
          w_pos = n_neg / n_pos
          w_neg = 1
        """
        yv = np.asarray(y).astype(int)
        n_pos = int(np.sum(yv == 1))
        n_neg = int(np.sum(yv == 0))
        if n_pos <= 0:
            return np.ones_like(yv, dtype="float32")
        w_pos = float(n_neg) / float(n_pos)
        w = np.ones_like(yv, dtype="float32")
        w[yv == 1] = w_pos
        return w

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
            use_scale: aqui significa "weight" via sample_weight (nao class_weight).
            feature_scaling: StandardScaler (recomendado para GaussianNB).
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(sample_weight)={use_scale} | "
            f"feature_scaling={feature_scaling} | cv_splits={cv_splits} | scoring={scoring}"
        )

        base_model = GaussianNB(var_smoothing=self.var_smoothing)

        t0 = time.time()

        if optimize:
            # IMPORTANTE:
            # GridSearchCV via sklearn Pipeline nao repassa sample_weight para "model" automaticamente.
            # Portanto, weight em modo optimize=True nao e aplicado.
            if use_scale:
                self.log.warning(
                    "[NB][WARN] optimize=True com use_scale=True: sample_weight nao e aplicado no GridSearchCV. "
                    "Sugestao: rode 'gridsearch' sem weight ou use 'fast' para avaliar weight."
                )

            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, seed=self.random_state)
            self.model = optimizer.optimize(
                X_train,
                y_train,
                cv_splits=cv_splits,
                use_smote=use_smote,
                use_scaler=bool(feature_scaling),
                scoring=scoring,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
                n_jobs=1,
                verbose=0,
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

            if feature_scaling:
                steps.append(("scaler", StandardScaler()))

            steps.append(("model", base_model))

            pipe = (ImbPipeline if use_smote else SkPipeline)(steps)
            self.model = pipe

            fit_kwargs = {}
            if use_scale:
                w = self._build_sample_weight(y_train)
                fit_kwargs["model__sample_weight"] = w

            self.model.fit(X_train, y_train, **fit_kwargs)
            self.log.info("[TRAIN] Treinamento direto concluido (fast).")

        dt = time.time() - t0
        MemoryMonitor.log_usage(self.log, f"apos treino ({dt:.1f}s)")

        self._log_nb_params()

    def _log_nb_params(self) -> None:
        """Loga parametros relevantes do GaussianNB."""
        try:
            model_obj = self.model
            if hasattr(model_obj, "named_steps") and "model" in model_obj.named_steps:
                nb = model_obj.named_steps["model"]
            else:
                nb = model_obj

            if hasattr(nb, "var_smoothing"):
                self.log.info(f"[NB] var_smoothing={getattr(nb, 'var_smoothing', None)}")
        except Exception as e:
            self.log.warning(f"[NB] Falha ao logar parametros: {e}")
