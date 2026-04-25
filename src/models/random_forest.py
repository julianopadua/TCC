# src/models/random_forest.py
# =============================================================================
# MODELO: RANDOM FOREST (SMOTE / PESOS / GRIDSEARCH) - PADRAO DO PROJETO
# =============================================================================
#
# Objetivo desta versao:
#   1) Aproveitar agressivamente CPU+RAM (Ryzen 5 3600, 16 GB) sem estourar.
#      n_jobs e calculado por RAM disponivel via _resource.recommend_n_jobs.
#   2) GridSearchCV NUNCA repetir-se para mesmo (model, scenario, grid_mode):
#      cache em memoria + cache em disco (json sob _caches/gridsearch/),
#      compartilhado entre variacoes 3 (smote+grid) e 4 (weight+grid).
#   3) Bulletproof contra OOM:
#        - SMOTE: subsamplear input para um cap calculado da RAM disponivel
#          (evita o vstack 2x do imblearn explodir).
#        - Fit RF: fallback de fracoes adaptativo ao tamanho do dataset.
#        - n_jobs reduz quando o dataset cresce.
#   4) Visibilidade: logs claros sobre cada decisao (n_jobs, cap SMOTE,
#      hits/miss de cache, fracoes tentadas).
# =============================================================================

import time
from typing import Optional, Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor, resource, gs_cache

# imbalanced-learn (opcional, apenas se usar SMOTE)
try:
    from imblearn.over_sampling import SMOTE  # type: ignore
    from imblearn.pipeline import Pipeline as ImbPipeline  # type: ignore
except Exception:
    SMOTE = None
    ImbPipeline = None


class RandomForestTrainer(BaseModelTrainer):
    """Random Forest com variacoes consistentes:
      - base / weight / smote / smote_weight
      - gridsearch / gridsearch_smote / gridsearch_weight / gridsearch_smote_weight

    Notas de performance:
      - GridSearch e a operacao mais cara. O melhor param_set por
        (model, scenario, grid_mode) e cacheado em disco e reutilizado
        por TODAS as variacoes que usam grid (3 e 4). Isso elimina a
        repeticao de gridsearch entre variacoes da mesma base.
      - n_jobs do RF e dimensionado pela RAM disponivel + cores fisicos.
        Em datasets grandes (minirocket 7M+ linhas), n_jobs cai para
        evitar estouro; em datasets menores, sobe para usar todos os cores.
      - SMOTE so recebe ate `resource.smote_input_cap()` linhas — alem
        disso, np.vstack interno do imblearn estoura RAM.
    """

    # Cache em memoria (process-local) por (cenario, grid_mode).
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

        # Grid FULL (18 candidatos). Com cv=2 => 36 fits.
        self.param_grid_full = {
            "n_estimators": [200, 400, 600],
            "max_depth": [None, 16, 24],
            "min_samples_leaf": [1, 3],
        }
        # Grid FAST (12 candidatos). Com cv=2 => 24 fits.
        self.param_grid_fast = {
            "n_estimators": [200, 400],
            "max_depth": [None, 16, 24],
            "min_samples_leaf": [1, 3],
        }

        # Limite maximo de linhas para o GridSearch propriamente dito.
        # 2M e suficiente para selecionar bons hiperparametros e evita o
        # custo combinatorio de fits sobre dataset full.
        self.max_gs_samples = 2_000_000

    # -------------------------------------------------------------------------
    # SMOTE pre-cap: subsamplear ANTES do fit_resample para nao estourar RAM
    # -------------------------------------------------------------------------
    def _maybe_cap_for_smote(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        *,
        n_features: int,
    ) -> Tuple[pd.DataFrame, pd.Series, bool]:
        """Aplica cap por RAM disponivel se SMOTE estiver no caminho.

        Retorna (X_capped, y_capped, capped_bool). Mantem ordem temporal
        via stride uniforme (compativel com TimeSeriesSplit).
        """
        cap = resource.smote_input_cap(n_features=n_features, log=self.log)
        n_total = int(len(y))
        if n_total <= cap:
            return X, y, False

        idx = resource.systematic_subsample_indices(n_total, cap)
        X_sub = X.iloc[idx]
        y_sub = y.iloc[idx]
        n_pos = int((y_sub == 1).sum())
        self.log.warning(
            f"[SMOTE-CAP] dataset {n_total:,} -> {len(y_sub):,} (cap={cap:,}) "
            f"para evitar OOM no fit_resample | pos no subset={n_pos:,}"
        )
        return X_sub, y_sub, True

    # -------------------------------------------------------------------------
    # Fit com fallback de fracoes (resiliente a OOM)
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
        n_total = int(len(y_train))
        if n_total == 0:
            raise ValueError("Dataset de treino vazio em _fit_with_fraction_fallback.")

        if fractions is None:
            fractions = resource.fractions_for_dataset(n_total)
        fractions = sorted({f for f in fractions if 0.0 < f <= 1.0}, reverse=True)

        n_features = int(X_train.shape[1])

        for frac in fractions:
            n_keep = max(1, int(n_total * frac))

            if n_keep == n_total:
                X_sub, y_sub = X_train, y_train
            else:
                idx = resource.systematic_subsample_indices(n_total, n_keep)
                X_sub = X_train.iloc[idx]
                y_sub = y_train.iloc[idx]

            # SMOTE precisa de cap adicional (vstack 2x).
            if use_smote:
                X_sub, y_sub, _ = self._maybe_cap_for_smote(X_sub, y_sub, n_features=n_features)

            self.log.info(
                f"[FALLBACK] tentativa fit RF{' + SMOTE' if use_smote else ''} "
                f"frac={frac:.2f} | n_rows_sub={len(y_sub):,} de {n_total:,} | "
                f"n_jobs={rf_kwargs.get('n_jobs')}"
            )
            MemoryMonitor.log_usage(self.log, f"pre-fit frac={frac:.2f}")

            rf = RandomForestClassifier(**rf_kwargs)  # type: ignore[arg-type]

            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn nao esta instalado.")
                model = ImbPipeline([
                    ("smote", SMOTE(
                        sampling_strategy=float(smote_sampling_strategy),
                        random_state=self.random_state,
                        k_neighbors=int(smote_k_neighbors),
                    )),
                    ("model", rf),
                ])
            else:
                model = rf

            try:
                model.fit(X_sub, y_sub)
                self.log.info(
                    f"[FALLBACK] fit OK | frac={frac:.2f} | rows={len(y_sub):,}"
                )
                self.model = model
                return
            except MemoryError as e:
                self.log.error(f"[FALLBACK][OOM] frac={frac:.2f}: {e}")
                # Reduz n_jobs antes da proxima tentativa para baixar pico.
                cur_n_jobs = int(rf_kwargs.get("n_jobs", 1) or 1)
                if cur_n_jobs > 1:
                    rf_kwargs["n_jobs"] = max(1, cur_n_jobs // 2)
                    self.log.warning(
                        f"[FALLBACK] reduzindo n_jobs {cur_n_jobs} -> {rf_kwargs['n_jobs']} "
                        f"para a proxima tentativa"
                    )
                time.sleep(2)
            except Exception as e:
                msg = str(e)
                if "Unable to allocate" in msg or "ArrayMemoryError" in msg:
                    self.log.error(f"[FALLBACK][OOM-numpy] frac={frac:.2f}: {e}")
                    cur_n_jobs = int(rf_kwargs.get("n_jobs", 1) or 1)
                    if cur_n_jobs > 1:
                        rf_kwargs["n_jobs"] = max(1, cur_n_jobs // 2)
                        self.log.warning(
                            f"[FALLBACK] reduzindo n_jobs {cur_n_jobs} -> {rf_kwargs['n_jobs']}"
                        )
                    time.sleep(2)
                else:
                    raise

        raise RuntimeError(
            f"[FALLBACK] Nao foi possivel treinar RandomForest com fracoes={fractions}."
        )

    # -------------------------------------------------------------------------
    # Helper: resolve best_params do cache (memoria -> disco) ou roda GS
    # -------------------------------------------------------------------------
    def _resolve_best_params(
        self,
        *,
        scenario: str,
        grid_mode: str,
        param_grid: Dict[str, list],
        base_model: RandomForestClassifier,
        X_gs: pd.DataFrame,
        y_gs: pd.Series,
        cv_splits: int,
        scoring: str,
        use_smote_in_grid: bool,
        smote_sampling_strategy: float,
        smote_k_neighbors: int,
    ) -> Optional[Dict[str, object]]:
        """Tenta cache em memoria, depois disco; se ambos miss, executa GS e
        salva nos dois niveis.
        """
        mem_key = f"{scenario}::{grid_mode}"

        if mem_key in RandomForestTrainer._gs_best_params_cache:
            bp = RandomForestTrainer._gs_best_params_cache[mem_key]
            self.log.info(f"[GS-CACHE] HIT memoria | scenario={scenario} grid_mode={grid_mode}: {bp}")
            return bp

        bp_disk = gs_cache.load_best_params(
            model="RandomForest",
            scenario=scenario,
            grid_mode=grid_mode,
            grid=param_grid,
            log=self.log,
        )
        if bp_disk is not None:
            RandomForestTrainer._gs_best_params_cache[mem_key] = bp_disk
            return bp_disk

        # MISS: roda GridSearch (uma unica vez por scenario+grid_mode).
        self.log.info(
            f"[GS-CACHE] MISS | rodando GridSearch para scenario={scenario} grid_mode={grid_mode}"
        )

        # Para o GS em si, se SMOTE estiver no pipe, cap o subset.
        if use_smote_in_grid:
            X_gs, y_gs, _ = self._maybe_cap_for_smote(X_gs, y_gs, n_features=int(X_gs.shape[1]))

        optimizer = ModelOptimizer(base_model, param_grid, self.log, seed=self.random_state)
        _ = optimizer.optimize(
            X_gs,
            y_gs,
            cv_splits=int(cv_splits),
            use_smote=use_smote_in_grid,
            use_scaler=False,
            scoring=scoring,
            smote_sampling_strategy=smote_sampling_strategy,
            smote_k_neighbors=smote_k_neighbors,
            n_jobs=1,           # CV serial: n_jobs paralelo no fit final
            verbose=1,
        )

        meta = optimizer.last_search_meta or {}
        bp = meta.get("best_params") or None

        if isinstance(bp, dict):
            RandomForestTrainer._gs_best_params_cache[mem_key] = bp
            try:
                gs_cache.save_best_params(
                    model="RandomForest",
                    scenario=scenario,
                    grid_mode=grid_mode,
                    grid=param_grid,
                    best_params=bp,
                    best_score=meta.get("best_score"),
                    scoring=meta.get("scoring"),
                    log=self.log,
                )
            except Exception as e:
                self.log.warning(f"[GS-CACHE] falha ao persistir cache em disco: {e}")
            return bp

        self.log.warning("[GS] best_params indisponivel; refit usara hiperparametros base.")
        return None

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
        self._auto_set_variation(optimize=optimize, use_smote=use_smote, use_scale=use_scale)
        self._log_dataset_header(X_train, y_train)

        n_rows = int(len(y_train))
        n_features = int(X_train.shape[1])

        cw = None
        if use_scale:
            mode = str(class_weight_mode or "balanced_subsample").strip().lower()
            cw = "balanced" if mode == "balanced" else "balanced_subsample"

        grid_mode_norm = str(grid_mode or "full").strip().lower()
        param_grid = self.param_grid_fast if grid_mode_norm == "fast" else self.param_grid_full

        # n_jobs adaptativo a RAM disponivel: usa o orcamento total.
        # Para o REFIT final, queremos paralelismo; quando SMOTE esta no
        # pipeline, o pico real fica no SMOTE -> paralelismo deve ser menor.
        rf_n_jobs = resource.recommend_n_jobs(
            n_rows=n_rows,
            n_features=n_features,
            target_usage=(0.70 if use_smote else 0.85),
            log=self.log,
        )

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight={use_scale} | "
            f"class_weight={cw} | cv_splits={cv_splits} | scoring={scoring} | "
            f"grid_mode={grid_mode_norm} | rf_n_jobs={rf_n_jobs}"
        )

        # Modelo base usado pelo GridSearch (n_jobs=1 dentro de cada CV fit).
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
        # BRANCH 1: optimize=True -> GS (com cache) + refit com fallback
        # ---------------------------------------------------------------------
        if optimize:
            # Subsample temporal-stride para o GridSearch.
            if n_rows > self.max_gs_samples:
                idx = resource.systematic_subsample_indices(n_rows, self.max_gs_samples)
                X_gs, y_gs = X_train.iloc[idx], y_train.iloc[idx]
                self.log.info(
                    f"[GS-SUBSAMPLE] {n_rows:,} -> {len(y_gs):,} via stride "
                    f"(max_gs_samples={self.max_gs_samples:,})"
                )
            else:
                X_gs, y_gs = X_train, y_train

            best_params = self._resolve_best_params(
                scenario=str(self.scenario),
                grid_mode=grid_mode_norm,
                param_grid=param_grid,
                base_model=base_model,
                X_gs=X_gs,
                y_gs=y_gs,
                cv_splits=int(cv_splits),
                scoring=scoring,
                use_smote_in_grid=bool(use_smote),
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )

            # Hiperparametros para refit final.
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
                        name = key[len("model__"):]
                        if name in rf_kwargs:
                            rf_kwargs[name] = value
                self.log.info(f"[GS] best_params aplicados: {rf_kwargs}")

            self._fit_with_fraction_fallback(
                rf_kwargs=rf_kwargs,
                X_train=X_train,
                y_train=y_train,
                use_smote=use_smote,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )

        # ---------------------------------------------------------------------
        # BRANCH 2: optimize=False -> treino direto com fallback
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

            self._fit_with_fraction_fallback(
                rf_kwargs=rf_kwargs_fast,
                X_train=X_train,
                y_train=y_train,
                use_smote=use_smote,
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )

            self.log.info(f"[TRAIN] fast direto OK | use_smote={use_smote}")

        dt = time.time() - t0
        MemoryMonitor.log_usage(self.log, f"apos treino ({dt:.1f}s)")

        self._log_importances(getattr(X_train, "columns", None))

    def _log_importances(self, feature_names: Optional[pd.Index]):
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
                self.log.info(f"[FI] Top 10 features: {top}")
        except Exception as e:
            self.log.warning(f"[FI] Falha ao extrair importancias: {e}")
