# src/ml/scaling.py
# =============================================================================
# SCALER MEMORY-SAFE PARA DATASETS LARGOS (LOGISTIC / SVM)
# =============================================================================
# Motivacao:
#   sklearn.preprocessing.StandardScaler chama _incremental_mean_and_var, que
#   internamente faz `temp = X - T` onde T e float64. Se X estiver em float32,
#   numpy promove o array inteiro para float64 -> aloca 8 bytes/celula.
#   Em cenarios largos (e.g. minirocket: 7.5M linhas x 183 colunas) isso
#   corresponde a ~10 GiB, estourando RAM mesmo com X ja em float32.
#
# Solucao:
#   Computar mean/std iterativamente em chunks usando acumuladores float64,
#   e aplicar a transformacao em chunks no proprio dtype original (float32).
#   Pico de RAM extra: ~chunk_rows * n_features * 8 bytes (~300 MB default).
# =============================================================================

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


class ChunkedStandardScaler(BaseEstimator, TransformerMixin):
    """StandardScaler memory-safe para arrays largos em float32.

    Compativel em interface com sklearn.preprocessing.StandardScaler
    (atributos: ``mean_``, ``scale_``, ``var_``, ``n_samples_seen_``,
    ``n_features_in_``).

    Args:
        chunk_rows: numero de linhas processadas por chunk em fit/transform.
            Default 200_000 -> pico extra ~ chunk_rows * n_features * 8 bytes.
        with_mean: se True, centraliza (subtrai mean).
        with_std: se True, escala dividindo por std.
        copy: se True (default), retorna array novo no transform; se False,
            sobrescreve em-place quando o dtype permitir.
    """

    def __init__(
        self,
        chunk_rows: int = 200_000,
        *,
        with_mean: bool = True,
        with_std: bool = True,
        copy: bool = True,
    ):
        self.chunk_rows = int(chunk_rows)
        self.with_mean = bool(with_mean)
        self.with_std = bool(with_std)
        self.copy = bool(copy)

    @staticmethod
    def _to_ndarray(X) -> np.ndarray:
        if isinstance(X, pd.DataFrame):
            return X.values
        return np.asarray(X)

    def fit(self, X, y=None):  # noqa: D401, ARG002
        arr = self._to_ndarray(X)
        if arr.ndim != 2:
            raise ValueError(f"ChunkedStandardScaler espera array 2D, recebeu shape={arr.shape}")

        n, d = arr.shape
        chunk = max(1, int(self.chunk_rows))

        sum_ = np.zeros(d, dtype=np.float64)
        sumsq = np.zeros(d, dtype=np.float64)
        cnt = 0

        for s in range(0, n, chunk):
            e = min(n, s + chunk)
            block = arr[s:e]
            # cast para float64 apenas no chunk (limita o pico de RAM)
            block64 = block.astype(np.float64, copy=False) if block.dtype != np.float64 else block
            sum_ += block64.sum(axis=0)
            sumsq += np.einsum("ij,ij->j", block64, block64)
            cnt += (e - s)

        if cnt == 0:
            raise ValueError("ChunkedStandardScaler.fit recebeu X vazio.")

        mean = sum_ / cnt
        var = sumsq / cnt - mean * mean
        # Numerical guard: variancia negativa minuscula vira 0
        var = np.maximum(var, 0.0)
        std = np.sqrt(var)
        # Evita divisao por zero (colunas constantes -> escala 1)
        std[std == 0.0] = 1.0

        # Mantem mean_/scale_ no dtype original do X para evitar upcast
        # implicito durante o transform.
        target_dtype = arr.dtype if np.issubdtype(arr.dtype, np.floating) else np.float32
        self.mean_ = mean.astype(target_dtype, copy=False)
        self.scale_ = std.astype(target_dtype, copy=False)
        self.var_ = var.astype(np.float64, copy=False)
        self.n_samples_seen_ = int(cnt)
        self.n_features_in_ = int(d)
        return self

    def transform(self, X):
        arr = self._to_ndarray(X)
        if arr.ndim != 2:
            raise ValueError(f"ChunkedStandardScaler espera array 2D, recebeu shape={arr.shape}")
        if arr.shape[1] != self.n_features_in_:
            raise ValueError(
                f"Numero de features incompativel: fit={self.n_features_in_} transform={arr.shape[1]}"
            )

        n = arr.shape[0]
        chunk = max(1, int(self.chunk_rows))

        if self.copy:
            out = np.empty_like(arr)
        else:
            # Transform in-place exige dtype compativel
            if arr.dtype != self.mean_.dtype:
                out = np.empty_like(arr)
            else:
                out = arr

        for s in range(0, n, chunk):
            e = min(n, s + chunk)
            block = arr[s:e]
            if out is arr:
                if self.with_mean:
                    block -= self.mean_
                if self.with_std:
                    block /= self.scale_
            else:
                if self.with_mean:
                    np.subtract(block, self.mean_, out=out[s:e], casting="same_kind")
                else:
                    out[s:e] = block
                if self.with_std:
                    out[s:e] /= self.scale_

        return out

    def fit_transform(self, X, y=None, **fit_params):  # noqa: ARG002
        return self.fit(X).transform(X)

    def inverse_transform(self, X):
        arr = self._to_ndarray(X)
        n = arr.shape[0]
        chunk = max(1, int(self.chunk_rows))
        out = np.empty_like(arr) if self.copy else arr
        for s in range(0, n, chunk):
            e = min(n, s + chunk)
            block = arr[s:e]
            if out is arr:
                if self.with_std:
                    block *= self.scale_
                if self.with_mean:
                    block += self.mean_
            else:
                if self.with_std:
                    np.multiply(block, self.scale_, out=out[s:e], casting="same_kind")
                else:
                    out[s:e] = block
                if self.with_mean:
                    out[s:e] += self.mean_
        return out
