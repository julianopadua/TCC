# src/ml/__init__.py
# =============================================================================
# EXPORTS DO NUCLEO DE ML
# =============================================================================

from .core import (
    BaseModelTrainer,
    TCCMetrics,
    TemporalSplitter,
    ModelOptimizer,
    MemoryMonitor
)
from .scaling import ChunkedStandardScaler
from . import _resource as resource
from . import _gs_cache as gs_cache

__all__ = [
    "BaseModelTrainer",
    "TCCMetrics",
    "TemporalSplitter",
    "ModelOptimizer",
    "MemoryMonitor",
    "ChunkedStandardScaler",
    "resource",
    "gs_cache",
]