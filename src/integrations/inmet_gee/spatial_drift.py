# src/integrations/inmet_gee/spatial_drift.py
# =============================================================================
# DETECÇÃO DE DERIVA ESPACIAL — Haversine, versões geográficas por estação
# =============================================================================
from __future__ import annotations

import logging
import math
from typing import Dict, List, Optional

import pandas as pd


_EARTH_RADIUS_M = 6_371_000.0


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distância geodésica haversine em metros entre dois pontos (graus decimais)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * _EARTH_RADIUS_M * math.asin(math.sqrt(a))


class SpatialDriftDetector:
    """
    Mantém estado de versões geográficas por estação entre anos e detecta
    eventos de deriva espacial (mudança real de localização vs ruído de arredondamento).

    Parâmetros:
        jitter_max_m   — deslocamento abaixo disto é considerado ruído (INFO, sem evento).
        drift_alert_m  — deslocamento acima disto é registrado como evento de deriva (WARNING).
    """

    def __init__(
        self,
        jitter_max_m: float = 50.0,
        drift_alert_m: float = 500.0,
        log: Optional[logging.Logger] = None,
    ):
        self.jitter_max_m = jitter_max_m
        self.drift_alert_m = drift_alert_m
        self.log = log or logging.getLogger(__name__)

        # Estado interno: station_uid -> (lat, lon, ano, geo_version)
        self._state: Dict[str, dict] = {}
        # Eventos de deriva registrados
        self.drift_events: List[dict] = []

    def process_year(self, station_rows: pd.DataFrame) -> None:
        """
        Processa todas as linhas de `station_rows` (resultado de aggregate_station_year)
        e detecta deriva em relação à versão geográfica anterior de cada estação.
        Registra eventos em self.drift_events.
        """
        for _, row in station_rows.iterrows():
            uid = row["station_uid"]
            lat = float(row["lat_median"])
            lon = float(row["lon_median"])
            year = int(row["ano"])

            if uid not in self._state:
                # Primeiro ano observado para esta estação
                self._state[uid] = {
                    "lat": lat, "lon": lon,
                    "ano": year, "geo_version": 1,
                }
                self.log.info(
                    "Estação '%s' registrada pela primeira vez no ano %d. "
                    "Lat: %.6f, Lon: %.6f | Versão geográfica: 1.",
                    uid, year, lat, lon,
                )
                continue

            prev = self._state[uid]
            dist_m = haversine_m(prev["lat"], prev["lon"], lat, lon)

            if dist_m <= self.jitter_max_m:
                self.log.info(
                    "Estação '%s' validada para o ano %d. "
                    "Deriva espacial: Nenhuma (deslocamento=%.1f m, dentro da tolerância de %.0f m).",
                    uid, year, dist_m, self.jitter_max_m,
                )
            else:
                new_version = prev["geo_version"] + 1
                event = {
                    "station_uid": uid,
                    "year_from": prev["ano"],
                    "year_to": year,
                    "lat_from": prev["lat"],
                    "lon_from": prev["lon"],
                    "lat_to": lat,
                    "lon_to": lon,
                    "distance_m": round(dist_m, 2),
                    "geo_version": new_version,
                }
                self.drift_events.append(event)

                if dist_m >= self.drift_alert_m:
                    self.log.warning(
                        "Deriva espacial detectada: Estação '%s' | Ano %d → %d | "
                        "Distância: %.1f m | Nova versão geográfica: %d. "
                        "Coords anteriores: (%.6f, %.6f) → Novas: (%.6f, %.6f).",
                        uid, prev["ano"], year, dist_m, new_version,
                        prev["lat"], prev["lon"], lat, lon,
                    )
                else:
                    self.log.info(
                        "Deslocamento de estação '%s' no ano %d: %.1f m "
                        "(acima do jitter de %.0f m, abaixo do limiar de alerta de %.0f m). "
                        "Registrado como versão geográfica %d.",
                        uid, year, dist_m, self.jitter_max_m, self.drift_alert_m, new_version,
                    )

                self._state[uid] = {
                    "lat": lat, "lon": lon,
                    "ano": year, "geo_version": new_version,
                }

    def get_geo_version(self, uid: str) -> int:
        """Retorna a versão geográfica atual da estação."""
        return self._state.get(uid, {}).get("geo_version", 1)

    def get_events_df(self) -> pd.DataFrame:
        if not self.drift_events:
            return pd.DataFrame(columns=[
                "station_uid", "year_from", "year_to",
                "lat_from", "lon_from", "lat_to", "lon_to",
                "distance_m", "geo_version",
            ])
        return pd.DataFrame(self.drift_events)

    def restore_state(self, state_dict: dict) -> None:
        """Restaura estado interno a partir do checkpoint (run_state.json)."""
        self._state = state_dict.get("drift_state", {})
        self.drift_events = state_dict.get("drift_events", [])

    def dump_state(self) -> dict:
        """Serializa estado interno para persistência no checkpoint."""
        return {
            "drift_state": self._state,
            "drift_events": self.drift_events,
        }
