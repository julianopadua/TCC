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
    Implementação do XGBoost flexível.
    Suporta combinações granulares de SMOTE e Scale_Pos_Weight.
    """
    
    def __init__(self, 
                 scenario_name: str, 
                 random_state: int = 42):
        
        super().__init__(scenario_name, "XGBoost", random_state)
        
        # Grid para GridSearch
        self.param_grid = {
            'n_estimators': [100, 200],
            'max_depth': [3, 6, 10],      
            'learning_rate': [0.01, 0.1], 
            'subsample': [0.8, 1.0]       
        }

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, 
              optimize: bool = False, 
              use_smote: bool = False, 
              use_scale: bool = True, 
              **kwargs):
        
        self.log.info(f"Configuração XGBoost >> Optimize: {optimize} | SMOTE: {use_smote} | Scale Weight: {use_scale}")
        
        # 1. Configuração do Scale Pos Weight (Balanceamento via Peso)
        scale_pos_weight = 1.0
        if use_scale:
            n_pos = y_train.sum()
            n_neg = len(y_train) - n_pos
            scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0
            self.log.info(f"Balanceamento por Peso ATIVADO: {scale_pos_weight:.2f}")
        else:
            self.log.info("Balanceamento por Peso DESATIVADO (Peso=1.0)")

        # 2. Definição do Estimador Base
        base_model = XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            scale_pos_weight=scale_pos_weight, # Usa o valor decidido acima
            tree_method='hist',  
            random_state=self.random_state,
            n_jobs=1, # 1 job para evitar crash de memória no GridSearch
            verbosity=1
        )

        if optimize:
            # Modo Otimizado (GridSearch)
            # Passamos o use_smote para o optimizer decidir se aplica a técnica sintética
            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, self.random_state)
            self.model = optimizer.optimize(X_train, y_train, use_smote=use_smote)
        else:
            # Modo Rápido: Treino Direto
            self.model = base_model
            self.model.fit(X_train, y_train)
            self.log.info("Treinamento direto concluído.")

        # 3. Logar Feature Importance
        self._log_importances(X_train.columns)

    def _log_importances(self, feature_names):
        try:
            if hasattr(self.model, 'named_steps') and 'model' in self.model.named_steps:
                booster = self.model.named_steps['model']
            else:
                booster = self.model

            if hasattr(booster, 'feature_importances_'):
                importances = booster.feature_importances_
                feat_dict = dict(zip(feature_names, importances))
                sorted_feats = sorted(feat_dict.items(), key=lambda x: x[1], reverse=True)[:5]
                self.log.info(f"Top 5 Features (Gain): {sorted_feats}")
        except Exception:
            pass