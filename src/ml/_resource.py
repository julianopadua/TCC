"""Helpers de orcamento de RAM/CPU para treinos pesados.

Centraliza decisoes sobre n_jobs, subsampling pre-SMOTE, fracoes de fit, e
estimativas de pico de memoria, para que XGBoost e RandomForest sigam o
mesmo contrato e nao caiam em OOM em cenarios largos (e.g. minirocket com
~180 features e milhoes de linhas em maquinas de 16 GB).

Politica:
  - target_usage default = 0.85 da RAM total (sobra ~15% para SO/cache)
  - max_jobs default = ceil(physical_cores * 1.0); SMT (hyperthreading) NAO
    e contado, pois trees sklearn raramente ganham com SMT e dobram pressao
    de memoria/page-cache.
  - SMOTE: subsamplear ANTES do fit_resample para limitar shape do
    np.vstack interno do imblearn. Cap default = 1.5M linhas para
    cenarios com >=150 features.
"""
from __future__ import annotations

import math
import os
from typing import Optional, Tuple

try:
    import psutil  # type: ignore
except Exception:
    psutil = None  # type: ignore


# Bytes por celula: float32 default.
_BYTES_PER_CELL_F32 = 4

# Fator de overhead empirico por worker em n_jobs paralelo:
# - python interpreter + libs (~250-400 MB)
# - bootstrap sample por arvore RF (~1x dataset enquanto a arvore e construida)
# - copias temporarias durante feature selection no split
_PER_WORKER_BASE_MB = 350.0


def total_ram_gb() -> float:
    """RAM fisica total da maquina em GB. 0.0 se psutil indisponivel."""
    if psutil is None:
        return 0.0
    return float(psutil.virtual_memory().total) / (1024 ** 3)


def available_ram_gb() -> float:
    """RAM efetivamente disponivel agora (livre + cache reciclavel)."""
    if psutil is None:
        return 0.0
    return float(psutil.virtual_memory().available) / (1024 ** 3)


def physical_cores() -> int:
    """Cores fisicos (sem SMT). Fallback: os.cpu_count() // 2."""
    if psutil is not None:
        try:
            n = psutil.cpu_count(logical=False)
            if n:
                return int(n)
        except Exception:
            pass
    n_log = os.cpu_count() or 2
    return max(1, n_log // 2)


def estimate_dataset_gb(n_rows: int, n_features: int, bytes_per_cell: int = _BYTES_PER_CELL_F32) -> float:
    """Estima tamanho em GB de um array (n_rows, n_features) denso."""
    if n_rows <= 0 or n_features <= 0:
        return 0.0
    return (float(n_rows) * float(n_features) * float(bytes_per_cell)) / (1024 ** 3)


def recommend_n_jobs(
    n_rows: int,
    n_features: int,
    *,
    target_usage: float = 0.85,
    max_jobs: Optional[int] = None,
    per_worker_overhead_mb: float = _PER_WORKER_BASE_MB,
    bootstrap_overhead_factor: float = 0.15,
    log=None,
) -> int:
    """Recomenda n_jobs para sklearn ensemble considerando RAM disponivel.

    Modelo de custo por worker:
      base_overhead + bootstrap_overhead_factor * dataset_size_gb

    Sklearn (com joblib loky backend) memmapeia X grande automaticamente,
    entao a 'copia por worker' nao e linear; usa-se um fator empirico
    (default 15% do tamanho do dataset) para indices/buffers temporarios
    por arvore.

    Returns:
        n_jobs >= 1.
    """
    cores = physical_cores()
    if max_jobs is None:
        max_jobs = cores

    avail_gb = available_ram_gb()
    if avail_gb <= 0:
        # Sem psutil: politica conservadora.
        n = max(1, min(max_jobs, max(1, cores // 2)))
        if log is not None:
            log.info(f"[RES] n_jobs={n} (psutil indisponivel; fallback conservador)")
        return n

    ds_gb = estimate_dataset_gb(n_rows, n_features)
    per_worker_gb = (per_worker_overhead_mb / 1024.0) + bootstrap_overhead_factor * ds_gb
    budget_gb = max(0.5, avail_gb * float(target_usage))

    if per_worker_gb <= 0:
        n_fit = max_jobs
    else:
        n_fit = int(math.floor(budget_gb / per_worker_gb))

    n_jobs = max(1, min(max_jobs, n_fit))

    if log is not None:
        log.info(
            f"[RES] avail_ram={avail_gb:.1f}GB target={budget_gb:.1f}GB | "
            f"dataset={ds_gb:.2f}GB | per_worker~{per_worker_gb:.2f}GB | "
            f"cores_phys={cores} max_jobs={max_jobs} -> n_jobs={n_jobs}"
        )
    return n_jobs


def smote_input_cap(
    n_features: int,
    *,
    target_usage: float = 0.65,
    log=None,
) -> int:
    """Cap maximo de linhas que devem entrar no SMOTE.

    SMOTE.fit_resample faz np.vstack([X_orig, X_synthetic]) no final,
    o que mantem ambos vivos por um instante (~2x pico). Para nao
    estourar a RAM, limita-se o tamanho do X de entrada.

    Estrategia: descobrir quantas linhas cabem em ~target_usage da RAM
    disponivel, considerando que SMOTE precisa simultaneamente do
    array original + sintetico + concatenado.
    """
    if n_features <= 0:
        return 1_500_000

    avail_gb = available_ram_gb()
    if avail_gb <= 0:
        return 1_500_000

    # Pico estimado SMOTE: ~3x o tamanho do X de entrada (orig + synth + stacked).
    budget_gb = max(1.0, avail_gb * float(target_usage))
    bytes_per_row = float(n_features) * _BYTES_PER_CELL_F32
    rows_for_budget = int((budget_gb * 1024 ** 3) / (bytes_per_row * 3.0))
    cap = max(200_000, min(3_000_000, rows_for_budget))

    if log is not None:
        log.info(
            f"[RES] SMOTE cap | avail={avail_gb:.1f}GB budget={budget_gb:.1f}GB "
            f"feats={n_features} -> max_input_rows={cap:,}"
        )
    return cap


def systematic_subsample_indices(n_total: int, n_keep: int):
    """Indices preservando ordem temporal (stride uniforme).

    Equivalente ao stride usado no _fit_with_fraction_fallback, isolado
    aqui para reuso por SMOTE-cap e GS-subsample.
    """
    import numpy as np
    if n_keep >= n_total:
        return np.arange(n_total, dtype=int)
    step = max(1, n_total // n_keep)
    idx = np.arange(0, n_total, step, dtype=int)
    if len(idx) > n_keep:
        idx = idx[:n_keep]
    return idx


def fractions_for_dataset(n_rows: int) -> list:
    """Estrategia de fracoes para fallback de fit, dimensionada pela RAM.

    - Datasets pequenos: 1.0 unica tentativa
    - Medianos: [1.0, 0.7, 0.5]
    - Grandes (>5M ou que usem >50% da RAM): [0.6, 0.4, 0.25]
    """
    if n_rows <= 1_500_000:
        return [1.0]
    if n_rows <= 4_000_000:
        return [1.0, 0.7, 0.5]
    return [0.6, 0.4, 0.25]


def estimate_xgb_workers(n_rows: int, n_features: int, *, max_threads: Optional[int] = None) -> int:
    """Numero de threads para XGBoost (intra-fit). XGB compartilha RAM (nao
    fork de workers), entao podemos usar todos os cores fisicos com seguranca.
    """
    cores = physical_cores()
    if max_threads is None:
        max_threads = cores
    return max(1, min(max_threads, cores))
