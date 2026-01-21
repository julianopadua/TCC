# src/models/xgboost_model.py
# =============================================================================
# MODELO: XGBOOST (GRADIENT BOOSTING)
# =============================================================================

import pandas as pd
import numpy as np
from xgboost import XGBClassifier
from src.ml import BaseModelTrainer, ModelOptimizer

class XGBoostTrainer(BaseModelTrainer):
    """
    Implementação do XGBoost.
    Estado da arte para dados tabulares desbalanceados.
    """
    
    def __init__(self, 
                 scenario_name: str, 
                 random_state: int = 42):
        
        super().__init__(scenario_name, "XGBoost", random_state)
        
        # Grid para o modo 'Turbo' (GridSearch)
        # Focado nos parametros que mais impactam overfitting e underfitting
        self.param_grid = {
            'n_estimators': [100, 200],
            'max_depth': [3, 6, 10],      # Profundidade da árvore
            'learning_rate': [0.01, 0.1], # Tamanho do passo
            'subsample': [0.8, 1.0]       # Previne overfitting
        }

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, optimize: bool = False, **kwargs):
        self.log.info(f"Iniciando treinamento XGBoost (Otimização: {optimize})...")
        
        # 1. Cálculo dinâmico do scale_pos_weight (Vital para dados desbalanceados)
        # Fórmula: sum(negatives) / sum(positives)
        n_pos = y_train.sum()
        n_neg = len(y_train) - n_pos
        scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0
        
        self.log.info(f"Balanceamento detectado: 1 positivo para cada {scale_pos_weight:.2f} negativos.")
        self.log.info(f"Usando scale_pos_weight={scale_pos_weight:.2f}")

        # 2. Definição do Estimador Base
        # tree_method='hist' é muito mais rápido e usa menos memória para grandes datasets
        base_model = XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            scale_pos_weight=scale_pos_weight,
            tree_method='hist',  # Otimização de memória/velocidade
            random_state=self.random_state,
            n_jobs=-1,
            verbosity=1
        )

        if optimize:
            # Modo Turbo: GridSearch + SMOTE
            # Nota: No GridSearch, o scale_pos_weight fixo pode brigar com o SMOTE.
            # Geralmente, se usa SMOTE, reduz-se o scale_pos_weight.
            # Mas vamos deixar o GridSearch encontrar o melhor conjunto.
            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, self.random_state)
            self.model = optimizer.optimize(X_train, y_train)
        else:
            # Modo Rápido: Treino Direto
            self.model = base_model
            self.model.fit(X_train, y_train)
            self.log.info("Treinamento direto concluído.")

        # 3. Logar Feature Importance (Ganho de Informação)
        self._log_importances(X_train.columns)

    def _log_importances(self, feature_names):
        """Extrai a importância das features nativa do XGBoost."""
        try:
            # Se for Pipeline (do optimizer), pega o passo final
            if hasattr(self.model, 'named_steps') and 'model' in self.model.named_steps:
                booster = self.model.named_steps['model']
            else:
                booster = self.model

            # XGBoost tem feature_importances_ direto
            if hasattr(booster, 'feature_importances_'):
                importances = booster.feature_importances_
                feat_dict = dict(zip(feature_names, importances))
                
                # Top 5
                sorted_feats = sorted(feat_dict.items(), key=lambda x: x[1], reverse=True)[:5]
                self.log.info(f"Top 5 Features (Gain): {sorted_feats}")
                
        except Exception as e:
            self.log.warning(f"Não foi possível extrair feature importance: {e}")