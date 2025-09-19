# src/webapp/columns_meta.py
# =============================================================================
# Dicionários de descrição de colunas (INMET & BDQueimadas)
# =============================================================================

# INMET — nomes como aparecem nos arquivos consolidados
INMET_COLS_DESC = {
    "DATA (YYYY-MM-DD)": "Data de observação (UTC).",
    "HORA (UTC)": "Hora de observação (UTC).",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)": "Acumulado horário de precipitação em milímetros.",
    "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)": "Pressão ao nível da estação (milibar).",
    "RADIACAO GLOBAL (KJ/m²)": "Radiação solar global no período (kJ/m²).",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)": "Temperatura do ar (°C) medida por bulbo seco.",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)": "Temperatura do ponto de orvalho (°C).",
    "UMIDADE RELATIVA DO AR, HORARIA (%)": "Umidade relativa do ar (%).",
    "VENTO, DIREÇÃO HORARIA (gr) (° (gr))": "Direção do vento (graus).",
    "VENTO, RAJADA MAXIMA (m/s)": "Rajada máxima de vento (m/s) no período.",
    "VENTO, VELOCIDADE HORARIA (m/s)": "Velocidade média do vento (m/s) no período.",
    "ANO": "Ano de referência do registro.",
    "CIDADE": "Localidade/estação (nome).",
    "LATITUDE": "Latitude da estação (graus decimais).",
    "LONGITUDE": "Longitude da estação (graus decimais).",
    "DATETIME": "Timestamp UTC gerado a partir de DATA e HORA.",
}

# BDQueimadas — colunas típicas da exportação TerraBrasilis
BDQ_COLS_DESC = {
    "DataHora": "Timestamp local/UTC da detecção do foco (formato da exportação).",
    "Satelite": "Satélite responsável pela detecção (ex.: AQUA_M-T).",
    "Pais": "País.",
    "Estado": "Estado (UF) no Brasil.",
    "Municipio": "Município do Brasil.",
    "Bioma": "Bioma brasileiro do ponto (ex.: Amazônia, Cerrado).",
    "DiaSemChuva": "Dias consecutivos sem chuva no local; -999 indica ausência de informação.",
    "Precipitacao": "Precipitação (mm) associada (se disponível).",
    "RiscoFogo": "Índice de risco de fogo (escala do INPE).",
    "FRP": "Fire Radiative Power (MW) estimado para o foco.",
    "Latitude": "Latitude do foco (graus decimais).",
    "Longitude": "Longitude do foco (graus decimais).",
}
