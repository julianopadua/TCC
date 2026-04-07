# Auditoria do consolidado: Base integrada INMET + BDQueimadas

## Resumo geral

- Arquivo: `D:\Projetos\TCC\data\dataset\inmet_bdq_all_years_cerrado.csv`
- Tamanho em disco: 6.18 GB
- Linhas totais: 45.135.924
- Colunas totais: 23
- Intervalo temporal inferido: 2003 a 2024
- Linhas com pelo menos um valor faltante: 45.135.924 (100.0000%)
- Coluna alvo auditada: `HAS_FOCO`
- Classe positiva: 151.544
- Classe negativa: 44.984.380
- Proporção da classe positiva: 0.3358%

## Arquivo CSV gerado

O arquivo `dataset_integrado_column_audit.csv` contém a auditoria completa por coluna, incluindo tipo do dado, contagem de faltantes e proporção de missing.

## Colunas com maior proporção de missing

| column_name | dtype | missing_total | pct_missing | non_missing_total |
| --- | --- | --- | --- | --- |
| RISCO_FOGO | float64 | 45.029.486 | 99.7642% | 106.438 |
| FRP | float64 | 44.984.468 | 99.6644% | 151.456 |
| FOCO_ID | object | 44.984.380 | 99.6642% | 151.544 |
| Data | float64 | 26.656.594 | 59.0585% | 18.479.330 |
| Hora UTC | float64 | 26.656.594 | 59.0585% | 18.479.330 |
| RADIACAO GLOBAL (KJ/m²) | int64 | 22.088.168 | 48.9370% | 23.047.756 |
| DATA (YYYY-MM-DD) | object | 18.479.330 | 40.9415% | 26.656.594 |
| HORA (UTC) | object | 18.479.330 | 40.9415% | 26.656.594 |
| PRECIPITAÇÃO TOTAL, HORÁRIO (mm) | object | 6.728.728 | 14.9077% | 38.407.196 |
| VENTO, DIREÇÃO HORARIA (gr) (° (gr)) | int64 | 5.332.054 | 11.8133% | 39.803.870 |
| VENTO, RAJADA MAXIMA (m/s) | object | 5.127.546 | 11.3602% | 40.008.378 |
| VENTO, VELOCIDADE HORARIA (m/s) | object | 5.104.256 | 11.3086% | 40.031.668 |
| TEMPERATURA DO PONTO DE ORVALHO (°C) | object | 4.961.412 | 10.9922% | 40.174.512 |
| UMIDADE RELATIVA DO AR, HORARIA (%) | int64 | 4.893.918 | 10.8426% | 40.242.006 |
| PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB) | object | 4.376.706 | 9.6967% | 40.759.218 |

## Auditoria completa por coluna

| column_name | dtype | missing_total | pct_missing | non_missing_total |
| --- | --- | --- | --- | --- |
| RISCO_FOGO | float64 | 45.029.486 | 99.7642% | 106.438 |
| FRP | float64 | 44.984.468 | 99.6644% | 151.456 |
| FOCO_ID | object | 44.984.380 | 99.6642% | 151.544 |
| Data | float64 | 26.656.594 | 59.0585% | 18.479.330 |
| Hora UTC | float64 | 26.656.594 | 59.0585% | 18.479.330 |
| RADIACAO GLOBAL (KJ/m²) | int64 | 22.088.168 | 48.9370% | 23.047.756 |
| DATA (YYYY-MM-DD) | object | 18.479.330 | 40.9415% | 26.656.594 |
| HORA (UTC) | object | 18.479.330 | 40.9415% | 26.656.594 |
| PRECIPITAÇÃO TOTAL, HORÁRIO (mm) | object | 6.728.728 | 14.9077% | 38.407.196 |
| VENTO, DIREÇÃO HORARIA (gr) (° (gr)) | int64 | 5.332.054 | 11.8133% | 39.803.870 |
| VENTO, RAJADA MAXIMA (m/s) | object | 5.127.546 | 11.3602% | 40.008.378 |
| VENTO, VELOCIDADE HORARIA (m/s) | object | 5.104.256 | 11.3086% | 40.031.668 |
| TEMPERATURA DO PONTO DE ORVALHO (°C) | object | 4.961.412 | 10.9922% | 40.174.512 |
| UMIDADE RELATIVA DO AR, HORARIA (%) | int64 | 4.893.918 | 10.8426% | 40.242.006 |
| PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB) | object | 4.376.706 | 9.6967% | 40.759.218 |
| TEMPERATURA DO AR - BULBO SECO, HORARIA (°C) | object | 4.307.446 | 9.5433% | 40.828.478 |
| ANO | int64 | 0 | 0.0000% | 45.135.924 |
| CIDADE | object | 0 | 0.0000% | 45.135.924 |
| HAS_FOCO | int64 | 0 | 0.0000% | 45.135.924 |
| LATITUDE | object | 0 | 0.0000% | 45.135.924 |
| LONGITUDE | object | 0 | 0.0000% | 45.135.924 |
| cidade_norm | object | 0 | 0.0000% | 45.135.924 |
| ts_hour | object | 0 | 0.0000% | 45.135.924 |