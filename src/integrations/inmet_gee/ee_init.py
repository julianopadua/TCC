# src/integrations/inmet_gee/ee_init.py
# =============================================================================
# Inicialização compartilhada do Google Earth Engine (conta de serviço ou OAuth).
# Usado por GeeSampler e pelo pipeline do artigo (gee_biomass).
# =============================================================================
from __future__ import annotations

import json
import logging
import random
import time
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

T = TypeVar("T")


def initialize_earth_engine(
    log: logging.Logger,
    *,
    service_account_key_path: str = "",
    project_id: str = "",
    log_resource_name: str = "Earth Engine",
) -> Optional[Any]:
    """
    Inicializa o módulo `ee` e retorna o módulo importado, ou None em falha.

    Ordem:
      1) Se `service_account_key_path` apontar para arquivo existente:
         ServiceAccountCredentials + Initialize(credentials=..., project=...).
      2) Senão: ee.Initialize(project=...) se project_id, senão ee.Initialize().
    """
    try:
        import ee as _ee

        key_path = (service_account_key_path or "").strip()
        project = (project_id or "").strip() or None

        if key_path:
            kp = Path(key_path)
            if not kp.is_file():
                log.warning(
                    "Arquivo de conta de serviço GEE não encontrado: '%s'. "
                    "Verifique service_account_key_path ou GEE_SERVICE_ACCOUNT_JSON.",
                    key_path,
                )
                return None
            credentials = _ee.ServiceAccountCredentials(key_file=str(kp.resolve()))
            try:
                with kp.open("r", encoding="utf-8") as fh:
                    sa_meta = json.load(fh)
            except (OSError, json.JSONDecodeError):
                sa_meta = {}
            if not project:
                project = (sa_meta.get("project_id") or "").strip() or None
            if not project:
                log.warning(
                    "Conta de serviço em '%s', mas project_id está vazio. "
                    "Defina project_id no YAML ou GEE_PROJECT.",
                    kp.name,
                )
                return None
            sa_email = getattr(credentials, "service_account_email", None) or sa_meta.get(
                "client_email", "(desconhecido)"
            )
            _ee.Initialize(credentials=credentials, project=project)
            log.info(
                "Google Earth Engine inicializado (conta de serviço). "
                "E-mail: %s | Projeto: %s | Key: %s | %s.",
                sa_email,
                project,
                kp.name,
                log_resource_name,
            )
            return _ee

        if project:
            _ee.Initialize(project=project)
        else:
            _ee.Initialize()
        log.info(
            "Google Earth Engine inicializado (OAuth/ADC). Projeto: '%s' | %s.",
            project or "(padrão)",
            log_resource_name,
        )
        return _ee
    except Exception as exc:
        log.warning(
            "Falha ao inicializar Google Earth Engine: %s. "
            "Confira IAM no Cloud Console e registro da conta no Earth Engine.",
            exc,
        )
        return None


def call_gee_with_retry(
    log: logging.Logger,
    fn: Callable[[], T],
    *,
    max_attempts: int = 3,
    base_delay: float = 5.0,
    max_delay: float = 120.0,
) -> Optional[T]:
    """Executa fn() com backoff exponencial + jitter (útil para getInfo)."""
    attempt = 0
    delay = base_delay
    while attempt < max_attempts:
        attempt += 1
        try:
            return fn()
        except Exception as exc:
            exc_str = str(exc).lower()
            is_quota = any(
                kw in exc_str
                for kw in ("quota", "rate", "limit", "toomany", "429", "resource exhausted")
            )
            if attempt >= max_attempts:
                log.error("Falha GEE após %d tentativas: %s", max_attempts, exc)
                return None
            jitter = random.uniform(0, delay * 0.25)
            wait = min(delay + jitter, max_delay)
            if is_quota:
                log.warning(
                    "Limite da API GEE. Backoff %.0fs (tentativa %d/%d).",
                    wait,
                    attempt,
                    max_attempts,
                )
            else:
                log.warning(
                    "Erro GEE (tentativa %d/%d): %s. Aguardando %.0fs.",
                    attempt,
                    max_attempts,
                    exc,
                    wait,
                )
            time.sleep(wait)
            delay = min(delay * 2, max_delay)
    return None
