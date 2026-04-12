# src/article/viz/variables.py
"""Registo de variáveis meteorológicas (nomes de coluna reais nos Parquets)."""
from __future__ import annotations

from typing import Dict, List, Tuple

# Alinhado a src/feature_engineering_temporal.py
COL_PRECIP = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
COL_TEMP = "TEMPERATURA DO AR - BULBO SECO, HORÁRIA (°C)"
COL_UMID = "UMIDADE RELATIVA DO AR, HORÁRIA (%)"
COL_VENTO = "VENTO, VELOCIDADE HORARIA (m/s)"
COL_PRESSAO = "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
COL_RAD = "RADIACAO GLOBAL (KJ/m²)"

LABEL_COL = "HAS_FOCO"
TS_COL = "ts_hour"
CITY_COL = "cidade_norm"

# slug amigável → (rótulo curto, nome de coluna)
METEO_REGISTRY: Dict[str, Tuple[str, str]] = {
    "precip": ("Precipitação (mm)", COL_PRECIP),
    "temp": ("Temperatura (°C)", COL_TEMP),
    "umid": ("Umidade relativa (%)", COL_UMID),
    "vento": ("Vento (m/s)", COL_VENTO),
    "pressao": ("Pressão (mB)", COL_PRESSAO),
    "rad": ("Radiação global (KJ/m²)", COL_RAD),
}


def meteo_slugs() -> List[str]:
    return list(METEO_REGISTRY.keys())


def biomass_columns_in_df(columns: List[str]) -> List[str]:
    """Colunas NDVI_/EVI_ presentes no schema."""
    return sorted(
        c
        for c in columns
        if c.startswith(("NDVI_", "EVI_"))
    )


def resolve_columns_to_load(
    selected_meteo_slugs: List[str],
    selected_biomass: List[str],
    include_has_foco: bool,
    all_columns: List[str],
) -> List[str]:
    """Lista mínima de colunas para read_parquet."""
    need = {TS_COL, CITY_COL}
    if include_has_foco and LABEL_COL in all_columns:
        need.add(LABEL_COL)
    col_set = set(all_columns)
    for slug in selected_meteo_slugs:
        if slug in METEO_REGISTRY:
            c = METEO_REGISTRY[slug][1]
            if c in col_set:
                need.add(c)
    for c in selected_biomass:
        if c in col_set:
            need.add(c)
    return sorted(need)
