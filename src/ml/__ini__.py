# src/ml/__init__.py
from .core import TCCMetrics, TemporalSplitter, BaseModelTrainer

# Define o que é exportado quando alguém faz "from src.ml import *"
__all__ = ["TCCMetrics", "TemporalSplitter", "BaseModelTrainer"]