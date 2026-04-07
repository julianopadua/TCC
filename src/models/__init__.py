# src/models/__init__.py
# =============================================================================
# EXPORTS DOS MODELOS
# =============================================================================

# Modelos Obrigatórios (Core do TCC)
from .logistic import LogisticTrainer
from .xgboost_model import XGBoostTrainer
from .dummy import DummyTrainer

# Modelos Opcionais (dentro de try/except caso falte dependência ou arquivo)
try:
    from .naive_bayes import NaiveBayesTrainer
except ImportError:
    NaiveBayesTrainer = None

try:
    from .random_forest import RandomForestTrainer
except ImportError:
    RandomForestTrainer = None

try:
    # Mapeando SVMLinearTrainer para SVMTrainer para facilitar o runner
    from .svm_linear import SVMLinearTrainer as SVMTrainer
except ImportError:
    SVMTrainer = None

__all__ = [
    "LogisticTrainer",
    "XGBoostTrainer",
    "DummyTrainer",
    "NaiveBayesTrainer",
    "RandomForestTrainer",
    "SVMTrainer"
]