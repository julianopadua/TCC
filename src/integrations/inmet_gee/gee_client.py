# src/integrations/inmet_gee/gee_client.py
# =============================================================================
# CLIENTE GOOGLE EARTH ENGINE — Inicialização, validação de ponto, backoff
# Phase-2 hook: interface GeeSampler com método extract_ndvi_timeseries().
# =============================================================================
from __future__ import annotations

import json
import logging
import math
import random
import time
from pathlib import Path
from typing import Any, Dict, Optional

from .config import GeeConfig


class GeeSampler:
    """
    Encapsula toda a interação com a API do Google Earth Engine.

    Fase 1: valida se um ponto geográfico é amostrável na coleção de referência.
    Fase 2 (hook): extract_ndvi_timeseries() — stub documentado abaixo.

    Retries com exponential backoff + jitter são aplicados automaticamente em
    todas as chamadas que envolvem getInfo() ou computeValue().
    """

    def __init__(
        self,
        cfg: GeeConfig,
        log: logging.Logger,
        max_attempts: int = 3,
        base_delay: float = 5.0,
        max_delay: float = 120.0,
    ):
        self.cfg = cfg
        self.log = log
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._initialized = False
        self._ee: Optional[Any] = None

    def initialize(self) -> bool:
        """
        Inicializa o SDK do Earth Engine.

        Ordem de autenticação:
          1) Se `service_account_key_path` estiver definido (JSON da conta de serviço),
             usa `ee.ServiceAccountCredentials(key_file=...)` e `project` do YAML/env
             ou `project_id` dentro do JSON.
          2) Caso contrário, OAuth / ADC: `ee.Initialize(project=...)` se houver project_id,
             senão `ee.Initialize()` (exige projeto default ou credencial de usuário).

        Retorna False sem lançar exceção se a inicialização falhar (pipeline segue sem GEE).
        """
        try:
            import ee as _ee

            key_path = (self.cfg.service_account_key_path or "").strip()
            project = (self.cfg.project_id or "").strip() or None

            if key_path:
                kp = Path(key_path)
                if not kp.is_file():
                    self.log.warning(
                        "Arquivo de conta de serviço GEE não encontrado: '%s'. "
                        "Verifique inmet_gee_pipeline.gee.service_account_key_path ou "
                        "GEE_SERVICE_ACCOUNT_JSON.",
                        key_path,
                    )
                    return False
                credentials = _ee.ServiceAccountCredentials(key_file=str(kp.resolve()))
                try:
                    with kp.open("r", encoding="utf-8") as fh:
                        sa_meta = json.load(fh)
                except (OSError, json.JSONDecodeError):
                    sa_meta = {}
                if not project:
                    project = (sa_meta.get("project_id") or "").strip() or None
                if not project:
                    self.log.warning(
                        "Conta de serviço configurada em '%s', mas project_id está vazio. "
                        "Defina inmet_gee_pipeline.gee.project_id ou a variável GEE_PROJECT.",
                        kp.name,
                    )
                    return False
                sa_email = getattr(credentials, "service_account_email", None) or sa_meta.get(
                    "client_email", "(desconhecido)"
                )
                _ee.Initialize(credentials=credentials, project=project)
                self._ee = _ee
                self._initialized = True
                self.log.info(
                    "Google Earth Engine inicializado com conta de serviço. "
                    "E-mail: %s | Projeto: %s | Key: %s | Coleção: %s.",
                    sa_email,
                    project,
                    kp.name,
                    self.cfg.reference_image_collection,
                )
                return True

            if project:
                _ee.Initialize(project=project)
            else:
                _ee.Initialize()
            self._ee = _ee
            self._initialized = True
            self.log.info(
                "Google Earth Engine inicializado (OAuth/ADC). Projeto: '%s'. "
                "Coleção de referência: '%s'.",
                project or "(padrão)",
                self.cfg.reference_image_collection,
            )
            return True
        except Exception as exc:
            self.log.warning(
                "Falha ao inicializar Google Earth Engine: %s. "
                "Validação GEE será ignorada nesta execução. "
                "Se usar conta de serviço, confira IAM no Cloud Console "
                "(ex.: roles/serviceusage.serviceUsageConsumer no projeto) e "
                "registro da conta no Earth Engine.",
                exc,
            )
            return False

    def _call_with_retry(self, fn, *args, **kwargs) -> Optional[Any]:
        """
        Executa fn(*args, **kwargs) com retry exponencial + jitter.
        Usa a semântica de WARNING para tentativas intermediárias
        e ERROR ao esgotar todas as tentativas.
        """
        attempt = 0
        delay = self.base_delay
        while attempt < self.max_attempts:
            attempt += 1
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                exc_str = str(exc)
                is_quota = any(
                    kw in exc_str.lower()
                    for kw in ("quota", "rate", "limit", "toomany", "429", "resource exhausted")
                )
                if attempt < self.max_attempts:
                    jitter = random.uniform(0, delay * 0.25)
                    wait = min(delay + jitter, self.max_delay)
                    if is_quota:
                        self.log.warning(
                            "Limite de requisições da API atingido. "
                            "Iniciando fallback de Exponential Backoff: aguardando %.0fs "
                            "(tentativa %d/%d).",
                            wait, attempt, self.max_attempts,
                        )
                    else:
                        self.log.warning(
                            "Erro na chamada GEE (tentativa %d/%d): %s. "
                            "Aguardando %.0fs antes de nova tentativa.",
                            attempt, self.max_attempts, exc, wait,
                        )
                    time.sleep(wait)
                    delay = min(delay * 2, self.max_delay)
                else:
                    self.log.error(
                        "Falha irrecuperável na chamada GEE após %d tentativas: %s",
                        self.max_attempts, exc,
                    )
        return None

    def validate_point(
        self,
        station_uid: str,
        lat: float,
        lon: float,
        geo_version: int,
        year: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Valida se o ponto (lat, lon) é amostrável na coleção de referência.
        Retorna dicionário com:
          status: "OK" | "FAILED" | "SKIPPED"
          message: descrição do resultado
          bands: lista de bandas amostradas (None se falhou)
          geo_version: versão geográfica usada
        """
        if not self._initialized:
            return {
                "station_uid": station_uid,
                "year": year,
                "geo_version": geo_version,
                "lat": lat,
                "lon": lon,
                "status": "SKIPPED",
                "message": "GEE não inicializado — credenciais ausentes.",
                "bands": None,
            }

        ee = self._ee

        def _sample():
            point = ee.Geometry.Point([lon, lat])
            collection = (
                ee.ImageCollection(self.cfg.reference_image_collection)
                .filterBounds(point)
                .limit(1)
            )
            img = collection.first()
            result = img.reduceRegion(
                reducer=ee.Reducer.first(),
                geometry=point,
                scale=self.cfg.scale_m,
                maxPixels=1,
            ).getInfo()
            return result

        result = self._call_with_retry(_sample)

        if result is None:
            self.log.error(
                "Estação '%s' | Geo-versão %d | Ponto (%.6f, %.6f): "
                "validação GEE falhou após todas as tentativas.",
                station_uid, geo_version, lat, lon,
            )
            return {
                "station_uid": station_uid,
                "year": year,
                "geo_version": geo_version,
                "lat": lat,
                "lon": lon,
                "status": "FAILED",
                "message": "Esgotadas todas as tentativas de requisição.",
                "bands": None,
            }

        bands = [k for k, v in result.items() if v is not None]
        self.log.info(
            "Estação '%s' | Geo-versão %d | Ponto (%.6f, %.6f): "
            "amostra GEE obtida com sucesso. Bandas disponíveis: %s.",
            station_uid, geo_version, lat, lon, bands or "(nenhuma com dados)",
        )
        return {
            "station_uid": station_uid,
            "year": year,
            "geo_version": geo_version,
            "lat": lat,
            "lon": lon,
            "status": "OK",
            "message": f"Amostra obtida. Bandas: {bands}",
            "bands": bands,
        }

    # -------------------------------------------------------------------------
    # PHASE-2 HOOK — Extração de séries NDVI/LAI ao redor da estação
    # -------------------------------------------------------------------------
    def extract_ndvi_timeseries(
        self,
        station_uid: str,
        lat: float,
        lon: float,
        start_date: str,
        end_date: str,
        buffer_radius_m: Optional[float] = None,
    ) -> Optional[Dict]:
        """
        STUB — Fase 2.
        Extrai série temporal de NDVI (e outras bandas configuráveis) ao redor do ponto
        da estação, usando um buffer circular de raio `buffer_radius_m` (default:
        cfg.buffer_radius_km * 1000).

        Implementação futura:
          - ee.Geometry.Point([lon, lat]).buffer(buffer_radius_m)
          - ImageCollection filtrada por data e bounds
          - .map(mask_clouds).select(["NDVI"]).toBands()
          - reduceRegion com Reducer.mean() / Reducer.median()
          - Retorna dict {date: str, ndvi_mean: float, ...} por imagem

        Parâmetros de fase 2 disponíveis em cfg.gee:
          buffer_radius_km, roi_mode, scale_m, reference_image_collection
        """
        raise NotImplementedError(
            "extract_ndvi_timeseries() é um hook de fase 2. "
            "Implemente após validar a fase 1 (validate_point)."
        )
