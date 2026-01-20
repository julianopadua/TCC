# src/ml/__init__.py

# Importa as classes do core.py para torná-las acessíveis via "src.ml"
from .core import BaseModelTrainer, TCCMetrics, TemporalSplitter

# Define o que é exportado quando alguém faz "from src.ml import *"
__all__ = ["BaseModelTrainer", "TCCMetrics", "TemporalSplitter"]