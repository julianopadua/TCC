# src/models/xgboost_model.py
# =============================================================================
# MODELO: XGBOOST (SMOTE / PESOS / GRIDSEARCH) - NOMENCLATURA PROFISSIONAL
# =============================================================================

import os
import time
from typing import Optional, Tuple

import numpy as np
import pandas as pd
from xgboost import XGBClassifier

from src.ml import BaseModelTrainer, ModelOptimizer, MemoryMonitor, resource, gs_cache

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

    # Cache em memoria por (cenario, grid_mode) — espelha o do RF e evita
    # repetir GridSearch entre variacoes 3 (smote+grid) e 4 (weight+grid).
    _gs_best_params_cache: dict = {}

    def __init__(self, scenario_name: str, random_state: int = 42, *, article_results: bool = False):
        super().__init__(scenario_name, "XGBoost", random_state, article_results=article_results)

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

    # -------------------------------------------------------------------------
    # SMOTE pre-cap: limita o input do fit_resample para evitar OOM no vstack
    # -------------------------------------------------------------------------
    def _maybe_cap_for_smote(
        self,
        X: pd.DataFrame,
        y: pd.Series,
    ) -> Tuple[pd.DataFrame, pd.Series, bool]:
        n_features = int(X.shape[1])
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
    # Resolver best_params via cache (memoria -> disco) ou rodar GS
    # -------------------------------------------------------------------------
    def _resolve_best_params(
        self,
        *,
        scenario: str,
        grid_mode: str,
        param_grid: dict,
        base_model: XGBClassifier,
        X_gs: pd.DataFrame,
        y_gs: pd.Series,
        cv_splits: int,
        scoring: str,
        use_smote_in_grid: bool,
        smote_sampling_strategy: float,
        smote_k_neighbors: int,
    ) -> Optional[dict]:
        mem_key = f"{scenario}::{grid_mode}"

        if mem_key in XGBoostTrainer._gs_best_params_cache:
            bp = XGBoostTrainer._gs_best_params_cache[mem_key]
            self.log.info(f"[GS-CACHE] HIT memoria | scenario={scenario} grid_mode={grid_mode}: {bp}")
            return bp

        bp_disk = gs_cache.load_best_params(
            model="XGBoost",
            scenario=scenario,
            grid_mode=grid_mode,
            grid=param_grid,
            log=self.log,
        )
        if bp_disk is not None:
            XGBoostTrainer._gs_best_params_cache[mem_key] = bp_disk
            return bp_disk

        self.log.info(
            f"[GS-CACHE] MISS | rodando GridSearch para scenario={scenario} grid_mode={grid_mode}"
        )

        if use_smote_in_grid:
            X_gs, y_gs, _ = self._maybe_cap_for_smote(X_gs, y_gs)

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
            n_jobs=1,
            verbose=1,
        )

        meta = optimizer.last_search_meta or {}
        bp = meta.get("best_params") or None
        if isinstance(bp, dict):
            XGBoostTrainer._gs_best_params_cache[mem_key] = bp
            try:
                gs_cache.save_best_params(
                    model="XGBoost",
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

        n_rows = int(len(y_train))
        n_features = int(X_train.shape[1])

        # XGB compartilha RAM entre threads (sem fork). Usar todos os cores
        # fisicos com seguranca; SMT (HT) raramente acelera trees.
        if model_n_jobs is None:
            model_n_jobs = resource.estimate_xgb_workers(n_rows, n_features)

        self.log.info(
            f"[CFG] optimize={optimize} | use_smote={use_smote} | use_weight(scale_pos_weight)={use_scale} | "
            f"scale_pos_weight={scale_pos_weight:.4f} | cv_splits={cv_splits} | scoring={scoring} | "
            f"grid_mode={grid_mode} | model_n_jobs={model_n_jobs} | n_rows={n_rows:,} | n_features={n_features}"
        )

        use_scaler_in_optimizer = False
        grid_mode_norm = str(grid_mode or "full").strip().lower()
        param_grid = self.param_grid_fast if grid_mode_norm == "fast" else self.param_grid_full

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
            n_jobs=int(model_n_jobs),
            verbosity=0,
        )

        t0 = time.time()

        # ---------------------------------------------------------------------
        # BRANCH 1: optimize=True -> GS (com cache mem+disco) + refit final
        # ---------------------------------------------------------------------
        if optimize:
            best_params = self._resolve_best_params(
                scenario=str(self.scenario),
                grid_mode=grid_mode_norm,
                param_grid=param_grid,
                base_model=base_model,
                X_gs=X_train,
                y_gs=y_train,
                cv_splits=int(cv_splits),
                scoring=scoring,
                use_smote_in_grid=bool(use_smote),
                smote_sampling_strategy=smote_sampling_strategy,
                smote_k_neighbors=smote_k_neighbors,
            )

            # Refit final com best_params (sem repetir GridSearchCV).
            xgb_kwargs = dict(
                n_estimators=200, max_depth=6, learning_rate=0.1,
                subsample=1.0, colsample_bytree=1.0,
                scale_pos_weight=float(scale_pos_weight),
                objective="binary:logistic", eval_metric="aucpr",
                tree_method="hist", random_state=self.random_state,
                n_jobs=int(model_n_jobs), verbosity=0,
            )
            if isinstance(best_params, dict):
                for key, value in best_params.items():
                    name = key[len("model__"):] if key.startswith("model__") else key
                    if name in xgb_kwargs:
                        xgb_kwargs[name] = value
                self.log.info(f"[GS] best_params aplicados ao refit: {best_params}")

            xgb_final = XGBClassifier(**xgb_kwargs)

            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn nao esta instalado.")
                X_fit, y_fit, _ = self._maybe_cap_for_smote(X_train, y_train)
                self.model = ImbPipeline([
                    ("smote", SMOTE(
                        sampling_strategy=float(smote_sampling_strategy),
                        random_state=self.random_state,
                        k_neighbors=int(smote_k_neighbors),
                    )),
                    ("model", xgb_final),
                ])
                self.log.info("[TRAIN] refit ImbPipeline com best_params iniciando...")
                self.model.fit(X_fit, y_fit)
            else:
                self.model = xgb_final
                self.log.info("[TRAIN] refit XGBClassifier com best_params iniciando...")
                self.model.fit(X_train, y_train)

        # ---------------------------------------------------------------------
        # BRANCH 2: optimize=False -> treino direto
        # ---------------------------------------------------------------------
        else:
            if use_smote:
                if SMOTE is None or ImbPipeline is None:
                    raise ImportError("SMOTE solicitado, mas imbalanced-learn nao esta instalado.")
                X_fit, y_fit, _ = self._maybe_cap_for_smote(X_train, y_train)
                self.model = ImbPipeline([
                    ("smote", SMOTE(
                        sampling_strategy=float(smote_sampling_strategy),
                        random_state=self.random_state,
                        k_neighbors=int(smote_k_neighbors),
                    )),
                    ("model", base_model),
                ])
                self.log.info("[TRAIN] ImbPipeline.fit (fast) iniciando...")
                self.model.fit(X_fit, y_fit)
            else:
                self.model = base_model
                self.log.info("[TRAIN] XGBClassifier.fit (fast) iniciando...")
                self.model.fit(X_train, y_train)

            self.log.info("[TRAIN] Treinamento direto concluido (fast).")

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
