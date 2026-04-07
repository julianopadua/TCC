# src/models/dummy.py
# =============================================================================
# MODELO: DUMMY CLASSIFIER (BASELINES) — PASTAS CONSISTENTES
# =============================================================================

import time
import pandas as pd
from sklearn.dummy import DummyClassifier

from src.ml import BaseModelTrainer, MemoryMonitor


class DummyTrainer(BaseModelTrainer):
    """
    Baselines para estabelecer piso de performance.

    Pastas:
      data/modeling/results/DummyClassifier/<strategy>/<scenario>/
    """

    def __init__(self, scenario_name: str, strategy: str = "stratified", random_state: int = 42):
        super().__init__(scenario_name, "DummyClassifier", random_state)
        self.strategy = str(strategy)

        # strategy vira variação (subpasta)
        self.set_custom_folder_name(self.strategy)

    def train(self, X_train: pd.DataFrame, y_train: pd.Series, optimize: bool = False, **kwargs):
        self._log_dataset_header(X_train, y_train)

        if optimize:
            self.log.info("[CFG] optimize=True ignorado para DummyClassifier (sem GridSearch).")

        self.log.info(f"[CFG] strategy={self.strategy}")

        t0 = time.time()

        # random_state só afeta algumas estratégias, mas é ok manter.
        self.model = DummyClassifier(strategy=self.strategy, random_state=self.random_state)
        self.model.fit(X_train, y_train)

        dt = time.time() - t0
        self.log.info(f"[TRAIN] concluído em {dt:.4f}s (dummy é instantâneo).")
        MemoryMonitor.log_usage(self.log, "após treino")
