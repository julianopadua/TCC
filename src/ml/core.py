# src/ml/core.py
# =============================================================================
# NÚCLEO DE MACHINE LEARNING — PROJETO TCC (CERRADO WILDFIRE PREDICTION)
# =============================================================================
# Este módulo fornece as classes base e utilitários compartilhados por todos
# os modelos (LogReg, XGBoost, RF, etc.), garantindo consistência metodológica.
# =============================================================================

import pandas as pd
import numpy as np
import joblib
import json
import matplotlib.pyplot as plt
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List
from abc import ABC, abstractmethod
from datetime import datetime

# Métricas Scikit-Learn
from sklearn.metrics import (
    precision_score, recall_score, f1_score, 
    roc_auc_score, average_precision_score, 
    brier_score_loss, confusion_matrix
)
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit

# Importa utilitários do projeto para logging e config
try:
    import utils
except ImportError:
    import src.utils as utils


class TCCMetrics:
    """
    Calculadora central de métricas alinhada aos objetivos do TCC.
    Foca em problemas desbalanceados (0.4% classe positiva).
    
    Métricas Prioritárias:
    - PR-AUC (Average Precision): Mais robusta que ROC-AUC para classes raras.
    - Brier Score: Avalia a calibração das probabilidades (essencial para risco).
    """
    
    @staticmethod
    def calculate(y_true: np.ndarray, 
                  y_pred_class: np.ndarray, 
                  y_pred_proba: np.ndarray) -> Dict[str, float]:
        """
        Calcula o conjunto padrão de métricas.
        
        Args:
            y_true: Array com labels reais (0 ou 1).
            y_pred_class: Array com predições binárias (0 ou 1) após threshold.
            y_pred_proba: Array com probabilidades da classe positiva (float 0-1).
        """
        # Proteção contra vetores vazios
        if len(y_true) == 0:
            return {}

        # Matriz de Confusão para métricas básicas
        tn, fp, fn, tp = confusion_matrix(y_true, y_pred_class).ravel()
        
        # Cálculo seguro (evita divisão por zero)
        metrics = {
            "accuracy": (tp + tn) / (tp + tn + fp + fn),
            "precision": precision_score(y_true, y_pred_class, zero_division=0),
            "recall": recall_score(y_true, y_pred_class, zero_division=0),
            "f1": f1_score(y_true, y_pred_class, zero_division=0),
            "specificity": tn / (tn + fp) if (tn + fp) > 0 else 0.0,
            
            # Métricas Probabilísticas (As mais importantes para o TCC)
            "roc_auc": roc_auc_score(y_true, y_pred_proba),
            "pr_auc": average_precision_score(y_true, y_pred_proba), # Area Under Precision-Recall Curve
            "brier_score": brier_score_loss(y_true, y_pred_proba)
        }
        
        return metrics


class TemporalSplitter:
    """
    Gerenciador de divisões de treino/teste respeitando a cronologia.
    Evita Data Leakage temporal (treinar no futuro, testar no passado).
    """
    
    def __init__(self, test_size_years: int = 2, gap_years: int = 0):
        """
        Args:
            test_size_years: Quantos anos finais reservar para teste (Holdout).
            gap_years: Anos de intervalo entre treino e teste (safety gap).
        """
        self.test_size = test_size_years
        self.gap = gap_years

    def split_holdout(self, df: pd.DataFrame, year_col: str = 'ANO') -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Realiza um split cronológico simples (Holdout).
        Ex: Se dados vão até 2024 e test_size=2 -> Teste = 2023, 2024.
        """
        unique_years = sorted(df[year_col].unique())
        
        if len(unique_years) < (self.test_size + 1):
            raise ValueError(f"Dados insuficientes ({len(unique_years)} anos) para teste de {self.test_size} anos.")
            
        split_year = unique_years[-self.test_size]
        
        # Máscaras temporais
        mask_train = df[year_col] < (split_year - self.gap)
        mask_test = df[year_col] >= split_year
        
        train_data = df[mask_train].copy()
        test_data = df[mask_test].copy()
        
        return train_data, test_data

    def get_cv_splitter(self, n_splits: int = 5):
        """
        Retorna um objeto TimeSeriesSplit do sklearn para validação cruzada
        dentro do conjunto de treino (Nested CV).
        """
        return TimeSeriesSplit(n_splits=n_splits)


class BaseModelTrainer(ABC):
    """
    Classe Abstrata (Skeleton) para treinamento de modelos.
    Define o contrato que LogReg, XGBoost e RF devem seguir.
    """
    
    def __init__(self, 
                 scenario_name: str, 
                 model_name: str, 
                 random_state: int = 42):
        
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger(f"ml.{model_name}", kind="train", per_run_file=True)
        
        self.scenario = scenario_name
        self.model_name = model_name
        self.random_state = random_state
        
        # Diretórios de saída padronizados
        # ex: data/modeling/results/LogisticRegression/base_A/
        self.output_dir = (Path(self.cfg['paths']['data']['modeling']) / 
                           "results" / 
                           self.model_name / 
                           self.scenario)
        utils.ensure_dir(self.output_dir)
        
        self.model = None
        self.scaler = None

    def get_scaler(self):
        """Retorna um StandardScaler configurado (utilitário para LogReg/SVM)."""
        return StandardScaler()

    @abstractmethod
    def train(self, X_train: pd.DataFrame, y_train: pd.Series, **kwargs):
        """
        Método abstrato: deve ser implementado pela classe filha.
        Deve preencher self.model.
        """
        pass

    def evaluate(self, X_test: pd.DataFrame, y_test: pd.Series, threshold: float = 0.5) -> Dict[str, Any]:
        """
        Avalia o modelo treinado nos dados de teste.
        """
        if self.model is None:
            raise ValueError("Modelo não treinado. Execute .train() primeiro.")
            
        self.log.info(f"Avaliando modelo {self.model_name} no cenário {self.scenario}...")
        
        # Predições
        # Assume que o modelo tem predict_proba (padrão sklearn)
        y_proba = self.model.predict_proba(X_test)[:, 1]
        y_pred = (y_proba >= threshold).astype(int)
        
        # Cálculo de métricas
        metrics = TCCMetrics.calculate(y_test, y_pred, y_proba)
        
        # Log das principais métricas
        self.log.info(f"Resultado Final >> PR-AUC: {metrics['pr_auc']:.4f} | Brier: {metrics['brier_score']:.4f}")
        
        return metrics

    def save_artifacts(self, metrics: Dict[str, float], feature_importance: Optional[Dict[str, float]] = None):
        """
        Salva o modelo serializado (.joblib) e as métricas (.json).
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Salvar Modelo
        model_path = self.output_dir / f"model_{timestamp}.joblib"
        joblib.dump(self.model, model_path)
        self.log.info(f"Modelo salvo em: {model_path}")
        
        # 2. Salvar Scaler (se existir)
        if self.scaler:
            scaler_path = self.output_dir / f"scaler_{timestamp}.joblib"
            joblib.dump(self.scaler, scaler_path)
        
        # 3. Salvar Métricas (JSON)
        metrics_path = self.output_dir / f"metrics_{timestamp}.json"
        
        report = {
            "model": self.model_name,
            "scenario": self.scenario,
            "timestamp": timestamp,
            "metrics": metrics,
            "feature_importance": feature_importance
        }
        
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)
            
        self.log.info(f"Relatório de métricas salvo em: {metrics_path}")