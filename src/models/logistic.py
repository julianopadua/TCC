# src/models/logistic.py
# =============================================================================
# MODELO: REGRESSÃO LOGÍSTICA (BASELINE)
# =============================================================================

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Importa nossa classe base abstrata e utilitários
from src.ml import BaseModelTrainer

class LogisticTrainer(BaseModelTrainer):
    """
    Implementação da Regressão Logística para o TCC.
    Herda de BaseModelTrainer para ganhar automação de logs e métricas.
    """
    
    def __init__(self, 
                 scenario_name: str, 
                 random_state: int = 42,
                 C: float = 1.0, 
                 max_iter: int = 1000):
        """
        Args:
            scenario_name: Identificador da base (ex: 'base_A').
            random_state: Semente para reprodutibilidade.
            C: Força da regularização (inverso de lambda). Menor C = Maior regularização.
            max_iter: Número máximo de iterações para convergência.
        """
        # Inicializa a classe pai (BaseModelTrainer)
        super().__init__(scenario_name, "LogisticRegression", random_state)
        
        # Salva hiperparâmetros
        self.C = C
        self.max_iter = max_iter

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, **kwargs):
        """
        Treyina o pipeline (Scaler + LogReg).
        """
        self.log.info(f"Iniciando treinamento da Regressão Logística (C={self.C})...")
        self.log.info(f"Dimensões de Treino: {X_train.shape}")

        # Definição do Pipeline Robusto
        # 1. StandardScaler: Obrigatório para Regressão Logística convergir bem.
        # 2. LogisticRegression: Configurada para lidar com desbalanceamento.
        self.model = Pipeline([
            ('scaler', StandardScaler()),
            ('clf', LogisticRegression(
                C=self.C,
                class_weight='balanced',  # Crucial para o TCC (focos raros)
                solver='saga',            # Eficiente para dados grandes
                max_iter=self.max_iter,
                random_state=self.random_state,
                n_jobs=-1                 # Usa todos os núcleos da CPU
            ))
        ])

        # Ajuste do modelo
        self.model.fit(X_train, y_train)
        self.log.info("Treinamento concluído.")
        
        # (Opcional) Logar coeficientes das variáveis mais importantes
        # Isso ajuda na interpretabilidade citada no TCC (Odds Ratio)
        if hasattr(self.model['clf'], 'coef_'):
            # Pega os coeficientes e mapeia para os nomes das colunas
            coefs = self.model['clf'].coef_[0]
            feature_names = X_train.columns
            
            # Cria um dicionário simples dos top 5 positivos e negativos
            coef_dict = dict(zip(feature_names, coefs))
            sorted_coefs = sorted(coef_dict.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
            
            self.log.info(f"Top 5 Variáveis de Maior Impacto (Absoluto): {sorted_coefs}")