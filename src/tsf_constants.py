# src/tsf_constants.py
# Colunas e limites partilhados entre fusão temporal legada e pipeline do artigo.
# Re-exportados por src/feature_engineering_temporal para compatibilidade de imports.

COL_PRECIP = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
COL_TEMP = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
COL_UMID = "UMIDADE RELATIVA DO AR, HORARIA (%)"
COL_VENTO = "VENTO, VELOCIDADE HORARIA (m/s)"
COL_PRESSAO = "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)"
COL_RAD = "RADIACAO GLOBAL (KJ/m²)"

# Primeiras N falhas por método/ano: WARNING com mensagem completa
TSF_FAIL_DETAIL_LOG_CAP = 25
