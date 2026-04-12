"""
Configuração central na raiz do repositório: caminhos e constantes partilhadas
(dashboard, caminhos de Parquet do artigo).
"""
from __future__ import annotations

from pathlib import Path

# Raiz do repositório (directório que contém este ficheiro)
PROJECT_ROOT: Path = Path(__file__).resolve().parent

# Parquets anuais do pipeline do artigo (nome de ficheiro por ano)
PARQUET_TEMPLATE: str = "inmet_bdq_{year}_cerrado.parquet"

# Secção «Vários anos» no Streamlit
MAX_MULTI_YEARS: int = 5

# Coluna sintética — precipitação acumulada (derivada em memória no viz)
SYNTH_PRECIP_CUM_COL: str = "_precip_acum_mm"
