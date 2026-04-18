# src/models/svm_linear.py
# =============================================================================
# MODELO: SVM LINEAR (CALIBRADO) - SMOTE / PESOS / GRIDSEARCH (PADRAO DO PROJETO)
# =============================================================================
# Nota tecnica:
# - O pipeline do projeto exige predict_proba (BaseModelTrainer.evaluate).
# - LinearSVC nao expoe predict_proba, entao usamos CalibratedClassifierCV.
# - Isso aumenta custo (calibracao por CV), portanto mantemos defaults conservadores.
# =============================================================================

import time
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.svm import LinearSVC

from src.ml import BaseModelTrainer, MemoryMonitor
from sklearn.pipeline import Pipeline as SkPipeline
from sklearn.preprocessing import StandardScaler

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None


class SVMLinearTrainer(BaseModelTrainer):
    """
    SVM linear com calibracao para probabilidade.
    Variacoes consistentes:
      - base
      - weight (class_weight='balanced')
      - smote
      - smote_weight
      - gridsearch
      - gridsearch_smote
      - gridsearch_weight
      - gridsearch_smote_weight

    Escolhas para estabilidade:
      - Sempre usa StandardScaler (modelo linear).
      - Sempre serial (n_jobs=1) para previsibilidade (LinearSVC + calibracao ja e pesado).
      - Em optimize=True: fazemos uma busca pequena manual (sem GridSearchCV do core),
        porque o ModelOptimizer do core assume estimador "model" com params simples e
        CalibratedClassifierCV dificulta param_grid e repasse de sample_weight.
    """

    def __init__(
        self,
        scenario_name: str,
        random_state: int = 42,
        C: float = 1.0,
        max_iter: int = 5000,
        *,
        article_results: bool = False,
    ):
        super().__init__(scenario_name, "SVMLinear", random_state, article_results=article_results)
        self.C = float(C)
        self.max_iter = int(max_iter)

        # Grade pequena (manual) para nao explodir custo
        self.param_grid_small = {
            "C": [0.1, 1.0, 10.0],
        }

    def _build_model(
        self,
        C: float,
        use_weight: bool,
        calibrate_method: str = "sigmoid",
        calibrate_cv: int = 3,
    ) -> CalibratedClassifierCV:
        class_weight = "balanced" if use_weight else None

        base = LinearSVC(
            C=float(C),
            class_weight=class_weight,
            max_iter=int(self.max_iter),
            random_state=self.random_state,
        )

        # CalibratedClassifierCV adiciona predict_proba ao SVM linear
        cal = CalibratedClassifierCV(
            estimator=base,
            method=str(calibrate_method),
            cv=int(calibrate_cv),
            n_jobs=1,
        )
        return cal

    def _fit_pipeline(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        model: Any,
        use_smote: bool,
        smote_sampling_strategy: float,
        smote_k_neighbors: int,
    ) -> Any:
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

        steps.append(("scaler", StandardScaler()))
        steps.append(("model", model))

        pipe = (ImbPipeline if use_smote else SkPipeline)(steps)
        pipe.fit(X_train, y_train)
        return pipe

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
        calibrate_method: str = "sigmoid",
        calibrate_cv: int = 3,
        **kwargs,
    ):
        """
        Args:
            optimize: ativa busca de hiperparametros (manual, pequena).
            use_smote: ativa SMOTE dentro do pipeline.
            use_scale: aqui significa class_weight='balanced'.
            scoring: mantido por compatibilidade (busca manual usa PR-AUC via average_precision_score).
            calibrate_method: 'sigmoid' (mais estavel) ou 'isotonic' (mais caro).
            calibrate_cv: folds internos da calibracao (default 3).
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(class_weight)={use_scale} | "
            f"cv_splits={cv_splits} | scoring={scoring} | calibrate_method={calibrate_method} | calibrate_cv={calibrate_cv}"
        )

        # Padrao conservador:
        # - GridSearch CV externo com SVM + calibracao pode ser proibitivo.
        # - Aqui fazemos busca pequena apenas em C, usando validação temporal simples.
        # - Para manter baixo o custo, usamos apenas 1 split temporal interno (ultimo bloco).
        t0 = time.time()

        if not optimize:
            model = self._build_model(C=self.C, use_weight=use_scale, calibrate_method=calibrate_method, calibrate_cv=calibrate_cv)
            self.model = self._fit_pipeline(
                X_train=X_train,
                y_train=y_train,
                model=model,
                use_smote=use_smote,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )
            self.log.info("[TRAIN] Treinamento direto concluido (fast).")
        else:
            # Busca manual com holdout temporal interno (ultimo 20% como validacao).
            # Mantem previsivel e evita explosao de fits.
            from sklearn.metrics import average_precision_score  # import local para manter deps enxutas

            n = int(len(y_train))
            if n < 10_000:
                split = max(1, int(n * 0.8))
            else:
                split = int(n * 0.8)

            X_tr_i = X_train.iloc[:split]
            y_tr_i = y_train.iloc[:split]
            X_va_i = X_train.iloc[split:]
            y_va_i = y_train.iloc[split:]

            best_score = -1.0
            best_C = None
            best_pipe = None

            cand_C = self.param_grid_small.get("C", [self.C])

            self.log.info(f"[SVM][OPT] candidatos C={cand_C} | inner_split={split}/{n}")

            for c in cand_C:
                try:
                    model = self._build_model(C=float(c), use_weight=use_scale, calibrate_method=calibrate_method, calibrate_cv=calibrate_cv)
                    pipe = self._fit_pipeline(
                        X_train=X_tr_i,
                        y_train=y_tr_i,
                        model=model,
                        use_smote=use_smote,
                        smote_sampling_strategy=smote_sampling_strategy,
                        smote_k_neighbors=smote_k_neighbors,
                    )

                    probs = pipe.predict_proba(X_va_i)[:, 1]
                    score = float(average_precision_score(np.asarray(y_va_i).astype(int), probs))

                    self.log.info(f"[SVM][OPT] C={c} | pr_auc={score:.6f}")

                    if score > best_score:
                        best_score = score
                        best_C = float(c)
                        best_pipe = pipe

                except Exception as e:
                    self.log.warning(f"[SVM][OPT][WARN] C={c} falhou: {e}")

            if best_pipe is None:
                raise RuntimeError("[SVM][OPT][ERRO] nenhum candidato treinou com sucesso.")

            # Refit final no treino completo com melhor C
            self.log.info(f"[SVM][OPT] melhor C={best_C} | pr_auc_valid={best_score:.6f} | refit no treino completo")
            final_model = self._build_model(C=float(best_C), use_weight=use_scale, calibrate_method=calibrate_method, calibrate_cv=calibrate_cv)
            self.model = self._fit_pipeline(
                X_train=X_train,
                y_train=y_train,
                model=final_model,
                use_smote=use_smote,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )

            # Registro do melhor C para auditoria
            self.log.info(f"[SVM][OPT] refit concluido | best_C={best_C}")

        dt = time.time() - t0
        MemoryMonitor.log_usage(self.log, f"apos treino ({dt:.1f}s)")

        self._log_svm_params()

    def _log_svm_params(self) -> None:
        """Loga parametros relevantes do estimador interno."""
        try:
            model_obj = self.model
            if hasattr(model_obj, "named_steps") and "model" in model_obj.named_steps:
                cal = model_obj.named_steps["model"]
            else:
                cal = model_obj

            # CalibratedClassifierCV(estimator=LinearSVC(...))
            est = getattr(cal, "estimator", None)
            C = getattr(est, "C", None) if est is not None else None
            cw = getattr(est, "class_weight", None) if est is not None else None
            self.log.info(f"[SVM] C={C} | class_weight={cw} | calibrated=True")
        except Exception as e:
            self.log.warning(f"[SVM] Falha ao logar parametros: {e}")
