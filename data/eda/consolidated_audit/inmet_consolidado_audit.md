# Auditoria do consolidado: INMET consolidado

## Resumo geral

- Arquivo: `D:\Projetos\TCC\data\consolidated\INMET\inmet_all_years_cerrado.csv`
- Tamanho em disco: 4.96 GB
- Linhas totais: 49.922.606
- Colunas totais: 15
- Intervalo temporal inferido: 2000 a 2025
- Linhas com pelo menos um valor faltante: 27.418.972 (54.9230%)
- Proporção da classe positiva: N/A neste consolidado

## Arquivo CSV gerado

O arquivo `inmet_consolidado_column_audit.csv` contém a auditoria completa por coluna, incluindo tipo do dado, contagem de faltantes e proporção de missing.

## Colunas com maior proporção de missing

| column_name | dtype | missing_total | pct_missing | non_missing_total |
| --- | --- | --- | --- | --- |
| RADIACAO GLOBAL (KJ/m²) | object | 24.409.980 | 48.8956% | 25.512.626 |
| PRECIPITAÇÃO TOTAL, HORÁRIO (mm) | object | 7.537.136 | 15.0976% | 42.385.470 |
| VENTO, DIREÇÃO HORARIA (gr) (° (gr)) | object | 5.999.908 | 12.0184% | 43.922.698 |
| VENTO, RAJADA MAXIMA (m/s) | object | 5.738.188 | 11.4942% | 44.184.418 |
| VENTO, VELOCIDADE HORARIA (m/s) | object | 5.719.640 | 11.4570% | 44.202.966 |
| TEMPERATURA DO PONTO DE ORVALHO (°C) | object | 5.533.788 | 11.0847% | 44.388.818 |
| UMIDADE RELATIVA DO AR, HORARIA (%) | int64 | 5.433.550 | 10.8839% | 44.489.056 |
| PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB) | object | 4.849.178 | 9.7134% | 45.073.428 |
| TEMPERATURA DO AR - BULBO SECO, HORARIA (°C) | object | 4.814.952 | 9.6448% | 45.107.654 |
| ANO | int64 | 0 | 0.0000% | 49.922.606 |
| CIDADE | object | 0 | 0.0000% | 49.922.606 |
| DATA (YYYY-MM-DD) | object | 0 | 0.0000% | 49.922.606 |
| HORA (UTC) | object | 0 | 0.0000% | 49.922.606 |
| LATITUDE | object | 0 | 0.0000% | 49.922.606 |
| LONGITUDE | object | 0 | 0.0000% | 49.922.606 |

## Auditoria completa por coluna

| column_name | dtype | missing_total | pct_missing | non_missing_total |
| --- | --- | --- | --- | --- |
| RADIACAO GLOBAL (KJ/m²) | object | 24.409.980 | 48.8956% | 25.512.626 |
| PRECIPITAÇÃO TOTAL, HORÁRIO (mm) | object | 7.537.136 | 15.0976% | 42.385.470 |
| VENTO, DIREÇÃO HORARIA (gr) (° (gr)) | object | 5.999.908 | 12.0184% | 43.922.698 |
| VENTO, RAJADA MAXIMA (m/s) | object | 5.738.188 | 11.4942% | 44.184.418 |
| VENTO, VELOCIDADE HORARIA (m/s) | object | 5.719.640 | 11.4570% | 44.202.966 |
| TEMPERATURA DO PONTO DE ORVALHO (°C) | object | 5.533.788 | 11.0847% | 44.388.818 |
| UMIDADE RELATIVA DO AR, HORARIA (%) | int64 | 5.433.550 | 10.8839% | 44.489.056 |
| PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB) | object | 4.849.178 | 9.7134% | 45.073.428 |
| TEMPERATURA DO AR - BULBO SECO, HORARIA (°C) | object | 4.814.952 | 9.6448% | 45.107.654 |
| ANO | int64 | 0 | 0.0000% | 49.922.606 |
| CIDADE | object | 0 | 0.0000% | 49.922.606 |
| DATA (YYYY-MM-DD) | object | 0 | 0.0000% | 49.922.606 |
| HORA (UTC) | object | 0 | 0.0000% | 49.922.606 |
| LATITUDE | object | 0 | 0.0000% | 49.922.606 |
| LONGITUDE | object | 0 | 0.0000% | 49.922.606 |