# src/ml/__init__.py

# Importa as classes do core.py, INCLUINDO o novo ModelOptimizer
from .core import BaseModelTrainer, TCCMetrics, TemporalSplitter, ModelOptimizer, MemoryMonitor

# Define o que é exportado quando alguém faz "from src.ml import *"
__all__ = [
    "BaseModelTrainer", 
    "TCCMetrics", 
    "TemporalSplitter", 
    "ModelOptimizer",
    "MemoryMonitor"
]