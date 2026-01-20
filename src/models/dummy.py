# src/models/dummy.py
# =============================================================================
# MODELO: DUMMY CLASSIFIER (BASELINES)
# =============================================================================

import pandas as pd
from sklearn.dummy import DummyClassifier
from sklearn.pipeline import Pipeline

from src.ml import BaseModelTrainer

class DummyTrainer(BaseModelTrainer):
    """
    Treina modelos ingênuos para estabelecer o 'piso' de performance.
    """
    
    def __init__(self, 
                 scenario_name: str, 
                 strategy: str = 'stratified',
                 random_state: int = 42):
        """
        Args:
            strategy: 
                - 'stratified': Respeita a distribuição de classes do treino.
                - 'most_frequent': Preve sempre a classe majoritária (0).
                - 'uniform': Preve aleatoriamente (50/50).
                - 'prior': Preve sempre a probabilidade a priori da classe (ótimo para Brier Score).
        """
        # Nome do modelo inclui a estratégia para diferenciar nos logs/pastas
        super().__init__(scenario_name, f"DummyClassifier_{strategy}", random_state)
        self.strategy = strategy

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, **kwargs):
        self.log.info(f"Treinando Dummy Classifier (Estratégia: {self.strategy})...")
        
        # Dummy não precisa de StandardScaler, mas mantemos no Pipeline por padronização
        self.model = DummyClassifier(
            strategy=self.strategy,
            random_state=self.random_state
        )
        
        self.model.fit(X_train, y_train)
        self.log.info("Treinamento concluído (instantâneo).")