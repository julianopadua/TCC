# src/ml/core.py
# =============================================================================
# NÚCLEO DE MACHINE LEARNING — PROJETO TCC (CERRADO WILDFIRE PREDICTION)
# =============================================================================
# Contém: Métricas, Split Temporal, Trainer Base, Otimizador (Grid+SMOTE) 
# e Monitoramento de Memória.
# =============================================================================

import pandas as pd
import numpy as np
import joblib
import json
import psutil
import os
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from typing import Dict, Any, Tuple, Optional, List
from abc import ABC, abstractmethod
from datetime import datetime

# Scikit-Learn
from sklearn.metrics import (
    precision_score, recall_score, f1_score, 
    roc_auc_score, average_precision_score, 
    brier_score_loss, confusion_matrix
)
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import TimeSeriesSplit, GridSearchCV
from sklearn.base import BaseEstimator

# Imbalanced-learn (Pipeline seguro para SMOTE)
try:
    from imblearn.over_sampling import SMOTE
    from imblearn.pipeline import Pipeline as ImbPipeline
except ImportError:
    # Fallback apenas para não quebrar a importação, mas avisará no uso
    SMOTE = None
    ImbPipeline = None

# Importa utilitários do projeto
try:
    import src.utils as utils
except ImportError:
    # Tenta importar como módulo local se rodar direto
    try:
        import utils
    except ImportError:
        pass # Deixa que o runner lide com paths

# -----------------------------------------------------------------------------
# 1. MONITORAMENTO DE RECURSOS
# -----------------------------------------------------------------------------
class MemoryMonitor:
    @staticmethod
    def get_usage() -> str:
        """Retorna uso de RAM atual do processo em GB."""
        process = psutil.Process(os.getpid())
        mem_bytes = process.memory_info().rss
        return f"{mem_bytes / (1024 ** 3):.2f} GB"

    @staticmethod
    def log_usage(log, context: str = ""):
        usage = MemoryMonitor.get_usage()
        log.info(f"[MEMÓRIA] {context}: {usage}")

# -----------------------------------------------------------------------------
# 2. CÁLCULO DE MÉTRICAS E PLOTS
# -----------------------------------------------------------------------------
class TCCMetrics:
    """
    Calculadora central de métricas alinhada aos objetivos do TCC.
    Foca em problemas desbalanceados e calibração.
    """
    @staticmethod
    def calculate(y_true: np.ndarray, 
                  y_pred_class: np.ndarray, 
                  y_pred_proba: np.ndarray) -> Dict[str, Any]:
        
        if len(y_true) == 0:
            return {}

        tn, fp, fn, tp = confusion_matrix(y_true, y_pred_class).ravel()
        
        # Evita divisão por zero
        denom_spec = (tn + fp) if (tn + fp) > 0 else 1.0
        
        metrics = {
            "accuracy": (tp + tn) / (tp + tn + fp + fn),
            "precision": precision_score(y_true, y_pred_class, zero_division=0),
            "recall": recall_score(y_true, y_pred_class, zero_division=0),
            "f1": f1_score(y_true, y_pred_class, zero_division=0),
            "specificity": tn / denom_spec,
            
            # Métricas Probabilísticas (Prioridade TCC)
            "roc_auc": roc_auc_score(y_true, y_pred_proba),
            "pr_auc": average_precision_score(y_true, y_pred_proba),
            "brier_score": brier_score_loss(y_true, y_pred_proba),
            
            # Matriz Crua (para tabelas no LaTeX)
            "confusion_matrix": {
                "tn": int(tn), "fp": int(fp),
                "fn": int(fn), "tp": int(tp)
            }
        }
        return metrics

    @staticmethod
    def plot_confusion_matrix(cm_dict: Dict[str, int], output_path: Path, title: str):
        """Gera e salva o heatmap da matriz de confusão como imagem."""
        cm = np.array([
            [cm_dict['tn'], cm_dict['fp']],
            [cm_dict['fn'], cm_dict['tp']]
        ])
        
        plt.figure(figsize=(6, 5))
        sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', cbar=False,
                    xticklabels=['Pred: Sem Fogo', 'Pred: Fogo'],
                    yticklabels=['Real: Sem Fogo', 'Real: Fogo'])
        plt.title(f"Confusion Matrix\n{title}")
        plt.ylabel('Verdadeiro')
        plt.xlabel('Predito')
        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()

# -----------------------------------------------------------------------------
# 3. SPLITTER TEMPORAL (Validação Robusta)
# -----------------------------------------------------------------------------
class TemporalSplitter:
    def __init__(self, test_size_years: int = 2, gap_years: int = 0):
        self.test_size = test_size_years
        self.gap = gap_years

    def split_holdout(self, df: pd.DataFrame, year_col: str = 'ANO') -> Tuple[pd.DataFrame, pd.DataFrame]:
        unique_years = sorted(df[year_col].unique())
        if len(unique_years) < (self.test_size + 1):
            raise ValueError(f"Dados insuficientes para teste de {self.test_size} anos.")
            
        split_year = unique_years[-self.test_size]
        
        # Garante ordem cronológica estrita
        mask_train = df[year_col] < (split_year - self.gap)
        mask_test = df[year_col] >= split_year
        
        return df[mask_train].copy(), df[mask_test].copy()

    def get_cv_splitter(self, n_splits: int = 3):
        """Retorna TimeSeriesSplit para usar no GridSearchCV (Nested CV)."""
        return TimeSeriesSplit(n_splits=n_splits)

# -----------------------------------------------------------------------------
# 4. OTIMIZADOR DE MODELOS (GRID SEARCH + SMOTE)
# -----------------------------------------------------------------------------
class ModelOptimizer:
    """
    Orquestra a otimização de hiperparâmetros.
    Aplica SMOTE apenas dentro dos folds de treino para evitar data leakage.
    """
    def __init__(self, base_estimator: BaseEstimator, param_grid: Dict, log, random_state: int = 42):
        self.base_estimator = base_estimator
        self.param_grid = param_grid
        self.log = log
        self.random_state = random_state
        
        if ImbPipeline is None:
            raise ImportError("Instale 'imbalanced-learn' para usar otimização.")

    def optimize(self, X_train: pd.DataFrame, y_train: pd.Series, cv_splits: int = 3, use_smote: bool = True):
        """
        Executa GridSearchCV com TimeSeriesSplit.
        """
        MemoryMonitor.log_usage(self.log, "Início GridSearch")

        steps = []
        if use_smote:
            steps.append(('smote', SMOTE(random_state=self.random_state)))
        
        steps.append(('scaler', StandardScaler()))
        steps.append(('model', self.base_estimator))

        pipeline = ImbPipeline(steps)

        # Ajusta prefixo dos parâmetros (ex: 'C' -> 'model__C')
        grid_params = {f'model__{k}': v for k, v in self.param_grid.items()}

        # CV Temporal (respeita o tempo dentro do treino)
        cv = TimeSeriesSplit(n_splits=cv_splits)

        self.log.info(f"Iniciando busca em {cv_splits} folds temporais...")
        self.log.info(f"Espaço de busca: {grid_params}")

        # Otimiza para PR-AUC (foco em classe rara)
        grid_search = GridSearchCV(
            estimator=pipeline,
            param_grid=grid_params,
            cv=cv,
            scoring='average_precision', 
            n_jobs=-1,  # Usa todos os cores
            verbose=1
        )

        grid_search.fit(X_train, y_train)
        
        MemoryMonitor.log_usage(self.log, "Fim GridSearch")
        self.log.info(f"Melhor PR-AUC (CV): {grid_search.best_score_:.4f}")
        self.log.info(f"Melhores Parâmetros: {grid_search.best_params_}")

        return grid_search.best_estimator_

# -----------------------------------------------------------------------------
# 5. CLASSE BASE DE TREINAMENTO
# -----------------------------------------------------------------------------
class BaseModelTrainer(ABC):
    def __init__(self, scenario_name: str, model_name: str, random_state: int = 42):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger(f"ml.{model_name}", kind="train", per_run_file=True)
        self.scenario = scenario_name
        self.model_name = model_name
        self.random_state = random_state
        
        self.output_dir = (Path(self.cfg['paths']['data']['modeling']) / "results" / 
                           self.model_name / self.scenario)
        utils.ensure_dir(self.output_dir)
        self.model = None

    @abstractmethod
    def train(self, X_train: pd.DataFrame, y_train: pd.Series, **kwargs):
        pass

    def evaluate(self, X_test: pd.DataFrame, y_test: pd.Series, threshold: float = 0.5) -> Dict[str, Any]:
        if self.model is None:
            raise ValueError("Modelo não treinado.")
            
        self.log.info(f"Avaliando modelo em teste...")
        y_proba = self.model.predict_proba(X_test)[:, 1]
        y_pred = (y_proba >= threshold).astype(int)
        
        metrics = TCCMetrics.calculate(y_test, y_pred, y_proba)
        
        self.log.info(f"Resultados Finais >> PR-AUC: {metrics['pr_auc']:.4f} | Brier: {metrics['brier_score']:.4f}")
        
        # Gera e salva a matriz de confusão
        cm_filename = f"cm_{self.model_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        cm_path = self.output_dir / cm_filename
        
        TCCMetrics.plot_confusion_matrix(
            metrics['confusion_matrix'], 
            cm_path, 
            f"{self.model_name} | {self.scenario}"
        )
        self.log.info(f"Matriz de confusão salva em: {cm_path}")
        
        return metrics

    def save_artifacts(self, metrics: Dict[str, Any], feature_importance: Optional[Dict[str, float]] = None):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Salva o Pipeline
        joblib.dump(self.model, self.output_dir / f"model_{timestamp}.joblib")
        
        # Salva JSON
        report = {
            "model": self.model_name,
            "scenario": self.scenario,
            "timestamp": timestamp,
            "metrics": metrics,
            "feature_importance": feature_importance
        }
        
        with open(self.output_dir / f"metrics_{timestamp}.json", "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)
        
        self.log.info(f"Relatório JSON salvo em {self.output_dir}")