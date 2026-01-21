# src/models/logistic.py
# =============================================================================
# MODELO: REGRESSÃO LOGÍSTICA
# =============================================================================

import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.ml import BaseModelTrainer, ModelOptimizer

class LogisticTrainer(BaseModelTrainer):
    """
    Implementação da Regressão Logística.
    Suporta treinamento direto ou otimização via GridSearch+SMOTE.
    """
    
    def __init__(self, 
                 scenario_name: str, 
                 random_state: int = 42,
                 C: float = 1.0, 
                 max_iter: int = 1000):
        
        super().__init__(scenario_name, "LogisticRegression", random_state)
        self.C = C
        self.max_iter = max_iter
        
        # Grid de hiperparâmetros para quando optimize=True
        self.param_grid = {
            'C': [0.01, 0.1, 1.0, 10.0],
            'class_weight': ['balanced', None] # Testa se o peso manual é melhor que nada
        }

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, optimize: bool = False, **kwargs):
        """
        Treina o modelo. Se optimize=True, usa GridSearch + SMOTE.
        """
        self.log.info(f"Iniciando treinamento (Otimização: {optimize})...")
        
        # Definição do estimador base
        base_model = LogisticRegression(
            C=self.C,
            class_weight='balanced',
            solver='saga',
            max_iter=self.max_iter,
            random_state=self.random_state,
            n_jobs=-1
        )

        if optimize:
            # Modo Turbo: GridSearch + SMOTE (Gerenciado pelo Core)
            optimizer = ModelOptimizer(base_model, self.param_grid, self.log, self.random_state)
            self.model = optimizer.optimize(X_train, y_train)
        else:
            # Modo Rápido: Pipeline Padrão (Scaler -> LogReg)
            self.model = Pipeline([
                ('scaler', StandardScaler()),
                ('model', base_model) # Nome 'model' para padronizar com o optimizer
            ])
            self.model.fit(X_train, y_train)
            self.log.info("Treinamento direto concluído.")

        # Logar coeficientes (Feature Importance Linear)
        self._log_coefficients(X_train.columns)

    def _log_coefficients(self, feature_names):
        """Helper para extrair e logar os coeficientes."""
        try:
            # Tenta pegar o passo 'model' (funciona tanto pro Pipeline sklearn quanto imblearn)
            if 'model' in self.model.named_steps:
                classifier = self.model.named_steps['model']
            else:
                return

            if hasattr(classifier, 'coef_'):
                coefs = classifier.coef_[0]
                coef_dict = dict(zip(feature_names, coefs))
                # Top 5 positivos e negativos absolutos
                sorted_coefs = sorted(coef_dict.items(), key=lambda x: abs(x[1]), reverse=True)[:5]
                self.log.info(f"Top 5 Variáveis de Impacto: {sorted_coefs}")
        except Exception as e:
            self.log.warning(f"Não foi possível extrair coeficientes: {e}")