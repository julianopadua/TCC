# src/models/random_forest.py
# =============================================================================
# MODELO: RANDOM FOREST (SMOTE / PESOS / GRIDSEARCH) - PADRAO DO PROJETO
# =============================================================================

import time
from typing import Optional, Dict, List

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.pipeline import Pipeline as SkPipeline

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None


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
      - RF e caro em dataset grande: prefira grid pequeno e cv_splits=2.
      - Em GridSearch, forcar n_jobs=1 no modelo ajuda a evitar picos.
      - SMOTE em RF costuma ser caro; weight geralmente e o primeiro a tentar.

    Ajustes adicionais nesta versao:
      - Quando optimize=True, o GridSearch e executado em um subconjunto
        amostrado por passo (stride) do dataset de treino, com tamanho maximo
        max_gs_samples (por exemplo, 2 milhoes de linhas), preservando a ordem temporal.
      - Após a escolha dos melhores hiperparametros, um novo RandomForest eh
        treinado com esses parametros utilizando fallback de memoria:
          * para bases muito grandes (n_rows > 5M), o fit final tenta fracoes
            0.60, 0.40, 0.25 do conjunto de treino, em ordem decrescente,
            reduzindo o risco de OOM tanto em SMOTE quanto em weight;
          * para bases menores (n_rows <= 5M), o padrao eh usar 100% (frac=1.0).
      - Os best_params do GridSearch sao armazenados em cache por
        (scenario, grid_mode) e podem ser reutilizados em outras variacoes
        (por exemplo, gridsearch_smote e gridsearch_weight para a mesma base),
        evitando repetir a busca exaustiva.
    """

    # Cache em memoria para melhores parametros por (cenario, grid_mode)
    _gs_best_params_cache: Dict[str, Dict[str, object]] = {}

    def __init__(
        self,
        scenario_name: str,
        random_state: int = 42,
        n_estimators: int = 300,
        max_depth: Optional[int] = None,
        min_samples_leaf: int = 1,
        *,
        article_results: bool = False,
    ):
        super().__init__(scenario_name, "RandomForest", random_state, article_results=article_results)

        self.n_estimators = int(n_estimators)
        self.max_depth = max_depth
        self.min_samples_leaf = int(min_samples_leaf)

        # Grid FULL (18 candidatos). Com cv=2 => 36 fits. Com cv=3 => 54 fits.
        self.param_grid_full = {
            "n_estimators": [200, 400, 600],  # 3
            "max_depth": [None, 16, 24],      # 3
            "min_samples_leaf": [1, 3],       # 2
        }

        # Grid FAST (12 candidatos). Com cv=2 => 24 fits. Com cv=3 => 36 fits.
        self.param_grid_fast = {
            "n_estimators": [200, 400],       # 2
            "max_depth": [None, 16, 24],      # 3
            "min_samples_leaf": [1, 3],       # 2
        }

        # Limite maximo de linhas para GridSearch (subset).
        # O modelo final eh treinado com fallback de fracoes do treino.
        self.max_gs_samples = 2_000_000

    # -------------------------------------------------------------------------
    # AUXILIAR: fit com fallback de fracoes para evitar OOM
    # -------------------------------------------------------------------------
    def _fit_with_fraction_fallback(
        self,
        rf_kwargs: Dict[str, object],
        X_train: pd.DataFrame,
        y_train: pd.Series,
        use_smote: bool,
        smote_sampling_strategy: float,
        smote_k_neighbors: int,
        fractions: Optional[List[float]] = None,
    ) -> None:
        """
        Tenta ajustar RandomForest (com ou sem SMOTE) em fracoes decrescentes
        do dataset de treino, para reduzir risco de estouro de memoria.

        Tipicamente:
          - Para bases grandes (n_rows > 5M), usamos algo como [0.60, 0.40, 0.25];
          - Para bases menores, podemos usar [1.0].

        Se todas as tentativas falharem por OOM, levanta RuntimeError.
        """
        n_total = int(len(y_train))
        if n_total == 0:
            raise ValueError("Dataset de treino vazio em _fit_with_fraction_fallback.")

        if fractions is None:
            # Por seguranca, padrao: fracoes conservadoras
            fractions = [0.6, 0.4, 0.25]

        # Garante fracoes validas e ordenadas (maior -> menor)
        fractions = sorted({f for f in fractions if 0.0 < f <= 1.0}, reverse=True)

        for frac in fractions:
            n_keep = max(1, int(n_total * frac))

            if n_keep == n_total:
                X_sub = X_train
                y_sub = y_train
            else:
                step = max(1, n_total // n_keep)
                idx = np.arange(0, n_total, step, dtype=int)
                if len(idx) > n_keep:
                    idx = idx[:n_keep]
                X_sub = X_train.iloc[idx]
                y_sub = y_train.iloc[idx]

            self.log.info(
                f"[FALLBACK] tentativa fit RF{' + SMOTE' if use_smote else ''} "
                f"frac={frac:.2f} | n_rows_sub={len(y_sub)} de {n_total}"
            )

            # Instancia um novo modelo para cada tentativa.
            rf = RandomForestClassifier(**rf_kwargs)  # type: ignore[arg-type]

            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn nao esta instalado.")
                steps = [
                    (
                        "smote",
                        SMOTE(
                            sampling_strategy=float(smote_sampling_strategy),
                            random_state=self.random_state,
                            k_neighbors=int(smote_k_neighbors),
                        ),
                    ),
                    ("model", rf),
                ]
                model = ImbPipeline(steps)
            else:
                model = rf

            try:
                model.fit(X_sub, y_sub)
                self.log.info(
                    f"[FALLBACK] fit concluido com frac={frac:.2f} | "
                    f"n_rows_sub={len(y_sub)} de {n_total}"
                )
                self.model = model
                return
            except MemoryError as e:
                self.log.error(
                    f"[FALLBACK][ERRO] MemoryError em frac={frac:.2f} | "
                    f"n_rows_sub={len(y_sub)} de {n_total}: {e}"
                )
                time.sleep(5)
            except Exception as e:
                msg = str(e)
                if "Unable to allocate" in msg or "ArrayMemoryError" in msg:
                    self.log.error(
                        f"[FALLBACK][ERRO] OOM em frac={frac:.2f} | "
                        f"n_rows_sub={len(y_sub)} de {n_total}: {e}"
                    )
                    time.sleep(5)
                else:
                    # Erro nao relacionado a memoria: relancar
                    raise

        raise RuntimeError(
            f"[FALLBACK] Nao foi possivel treinar RandomForest, mesmo com fracoes={fractions}. "
            f"Ultima tentativa frac={fractions[-1]:.2f}."
        )

    # -------------------------------------------------------------------------
    # TREINO
    # -------------------------------------------------------------------------
    def train(
        self,
        X_train: pd.DataFrame,
        y_train: pd.Series,
        optimize: bool = False,
        use_smote: bool = False,
        use_scale: bool = True,
        cv_splits: int = 2,
        scoring: str = "average_precision",
        smote_sampling_strategy: float = 0.1,
        smote_k_neighbors: int = 5,
        class_weight_mode: str = "balanced_subsample",
        grid_mode: str = "full",
        **kwargs,
    ):
        """
        Args:
            optimize: ativa GridSearchCV com TimeSeriesSplit.
            use_smote: ativa SMOTE dentro do pipeline (fast ou grid).
            use_scale: aqui significa balanceamento por peso (class_weight).
            class_weight_mode: "balanced" ou "balanced_subsample".
            grid_mode: "full" (54 fits se cv=3, 36 fits se cv=2) ou "fast".
        """
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        n_rows = int(len(y_train))

        cw = None
        if use_scale:
            mode = str(class_weight_mode or "balanced_subsample").strip().lower()
            cw = "balanced" if mode == "balanced" else "balanced_subsample"

        grid_mode_norm = str(grid_mode or "full").strip().lower()
        param_grid = self.param_grid_fast if grid_mode_norm == "fast" else self.param_grid_full

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(class_weight)={use_scale} | "
            f"class_weight={cw} | cv_splits={cv_splits} | scoring={scoring} | grid_mode={grid_mode_norm}"
        )

        # Definicao conservadora de n_jobs:
        # - se use_smote=True OU dataset muito grande (>5M), usamos n_jobs=1;
        # - caso contrario (dataset menor, sem SMOTE), podemos usar n_jobs=-1.
        if use_smote or n_rows > 5_000_000:
            rf_n_jobs = 1
        else:
            rf_n_jobs = -1

        # Modelo base para o GridSearch (sempre com n_jobs=1 para poupar RAM).
        base_model = RandomForestClassifier(
            n_estimators=self.n_estimators,
            max_depth=self.max_depth,
            min_samples_leaf=self.min_samples_leaf,
            class_weight=cw,
            random_state=self.random_state,
            n_jobs=1,
            oob_score=False,
            bootstrap=True,
        )

        t0 = time.time()

        # ---------------------------------------------------------------------
        # BRANCH 1: optimize=True  -> GridSearch em subset + refit com fallback
        # ---------------------------------------------------------------------
        if optimize:
            # Subamostragem sistematica por passo (stride) para GridSearch,
            # preservando a ordem temporal (TimeSeriesSplit ainda faz sentido).
            X_gs = X_train
            y_gs = y_train

            if n_rows > self.max_gs_samples:
                step = max(1, n_rows // self.max_gs_samples)
                idx = np.arange(0, n_rows, step, dtype=int)
                if len(idx) > self.max_gs_samples:
                    idx = idx[: self.max_gs_samples]

                X_gs = X_train.iloc[idx]
                y_gs = y_train.iloc[idx]

                self.log.info(
                    f"[GS-SUBSAMPLE] n_rows_full={n_rows} | max_gs_samples={self.max_gs_samples} | "
                    f"step={step} | n_rows_gs={len(y_gs)}"
                )

            cache_key = f"{self.scenario}::{grid_mode_norm}"
            best_params = None

            if cache_key in RandomForestTrainer._gs_best_params_cache:
                best_params = RandomForestTrainer._gs_best_params_cache[cache_key]
                self.log.info(
                    f"[GS-CACHE] Reutilizando best_params em cache para "
                    f"scenario={self.scenario} grid_mode={grid_mode_norm}: {best_params}"
                )
            else:
                optimizer = ModelOptimizer(base_model, param_grid, self.log, seed=self.random_state)
                _ = optimizer.optimize(
                    X_gs,
                    y_gs,
                    cv_splits=int(cv_splits),
                    use_smote=use_smote,
                    use_scaler=False,  # RF nao precisa scaler
                    scoring=scoring,
                    smote_sampling_strategy=smote_sampling_strategy,
                    smote_k_neighbors=smote_k_neighbors,
                    n_jobs=1,          # importante: serial no CV para poupar RAM
                    verbose=1,
                )

                best_params = optimizer.last_search_meta.get("best_params", None)
                if isinstance(best_params, dict):
                    RandomForestTrainer._gs_best_params_cache[cache_key] = best_params
                    self.log.info(
                        f"[GS-CACHE] Armazenando best_params em cache para "
                        f"scenario={self.scenario} grid_mode={grid_mode_norm}: {best_params}"
                    )

            # Parametros finais do RF (fit com fallback de fracoes).
            rf_kwargs: Dict[str, object] = {
                "n_estimators": self.n_estimators,
                "max_depth": self.max_depth,
                "min_samples_leaf": self.min_samples_leaf,
                "class_weight": cw,
                "random_state": self.random_state,
                "n_jobs": rf_n_jobs,
                "oob_score": False,
                "bootstrap": True,
            }

            if isinstance(best_params, dict):
                for key, value in best_params.items():
                    if key.startswith("model__"):
                        name = key[len("model__") :]
                        if name in rf_kwargs:
                            rf_kwargs[name] = value

                self.log.info(f"[GS] best_params extraidos (RF): {rf_kwargs}")
            else:
                self.log.warning("[GS] best_params vazio/indisponivel; usando hiperparametros base para refit final.")

            # Fracoes para fallback: bases gigantes -> 0.60, 0.40, 0.25; bases menores -> 1.0
            if n_rows > 5_000_000:
                fractions = [0.6, 0.4, 0.25]
            else:
                fractions = [1.0]

            self._fit_with_fraction_fallback(
                rf_kwargs=rf_kwargs,
                X_train=X_train,
                y_train=y_train,
                use_smote=use_smote,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
                fractions=fractions,
            )

        # ---------------------------------------------------------------------
        # BRANCH 2: optimize=False -> treino direto (fast) com fallback
        # ---------------------------------------------------------------------
        else:
            rf_kwargs_fast: Dict[str, object] = {
                "n_estimators": self.n_estimators,
                "max_depth": self.max_depth,
                "min_samples_leaf": self.min_samples_leaf,
                "class_weight": cw,
                "random_state": self.random_state,
                "n_jobs": rf_n_jobs,
                "oob_score": False,
                "bootstrap": True,
            }

            if n_rows > 5_000_000:
                fractions = [0.6, 0.4, 0.25]
            else:
                fractions = [1.0]

            self._fit_with_fraction_fallback(
                rf_kwargs=rf_kwargs_fast,
                X_train=X_train,
                y_train=y_train,
                use_smote=use_smote,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
                fractions=fractions,
            )

            self.log.info(
                "[TRAIN] Treinamento direto concluido (fast) "
                f"| use_smote={use_smote}"
            )

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
                self.log.info(f"[FI] Top 10 features (feature_importancias_): {top}")
        except Exception as e:
            self.log.warning(f"[FI] Falha ao extrair importancias: {e}")