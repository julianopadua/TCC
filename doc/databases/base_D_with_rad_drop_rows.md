# Auditoria Consolidada: base_D_with_rad_drop_rows

**Arquivos Processados:** 2003 a 2024
**Total de Registros:** 16,900,984

## 0. Amostra e Estrutura dos Dados
> **Arquivo de Referência:** `inmet_bdq_2003_cerrado.parquet`

**Total de Colunas:** 21

**Lista de Colunas:**
`DATA (YYYY-MM-DD), HORA (UTC), PRECIPITAÇÃO TOTAL, HORÁRIO (mm), PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB), RADIACAO GLOBAL (KJ/m²), TEMPERATURA DO AR - BULBO SECO, HORARIA (°C), TEMPERATURA DO PONTO DE ORVALHO (°C), UMIDADE RELATIVA DO AR, HORARIA (%), VENTO, DIREÇÃO HORARIA (gr) (° (gr)), VENTO, RAJADA MAXIMA (m/s), VENTO, VELOCIDADE HORARIA (m/s), ANO, CIDADE, LATITUDE, LONGITUDE, cidade_norm, ts_hour, RISCO_FOGO, FRP, FOCO_ID, HAS_FOCO`

**Preview (5 primeiras linhas):**
| DATA (YYYY-MM-DD)   | HORA (UTC)   | PRECIPITAÇÃO TOTAL, HORÁRIO (mm)   | PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)   | RADIACAO GLOBAL (KJ/m²)   | TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)   | TEMPERATURA DO PONTO DE ORVALHO (°C)   | UMIDADE RELATIVA DO AR, HORARIA (%)   | VENTO, DIREÇÃO HORARIA (gr) (° (gr))   | VENTO, RAJADA MAXIMA (m/s)   | VENTO, VELOCIDADE HORARIA (m/s)   | ANO   | CIDADE   | LATITUDE   | LONGITUDE   | cidade_norm   | ts_hour             | RISCO_FOGO   | FRP   | FOCO_ID   | HAS_FOCO   |
|:--------------------|:-------------|:-----------------------------------|:--------------------------------------------------------|:--------------------------|:-----------------------------------------------|:---------------------------------------|:--------------------------------------|:---------------------------------------|:-----------------------------|:----------------------------------|:------|:---------|:-----------|:------------|:--------------|:--------------------|:-------------|:------|:----------|:-----------|
| 2003-01-01          | 09:00        | 0                                  | 897.5                                                   | 756                       | 19.5                                           | 18.2                                   | 92                                    | 111                                    | 4.5                          | 1.9                               | 2003  | ARAXA    | -19.6056   | -46.9494    | araxa         | 2003-01-01 09:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 09:00        | 0                                  | 897.5                                                   | 756                       | 19.5                                           | 18.2                                   | 92                                    | 111                                    | 4.5                          | 1.9                               | 2003  | ARAXA    | -19.6056   | -46.9494    | araxa         | 2003-01-01 09:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 09:00        | 0                                  | 940.4                                                   | 14                        | 21.4                                           | 19.5                                   | 89                                    | 142                                    | 3.2                          | 0.9                               | 2003  | BAURU    | -22.3581   | -49.0289    | bauru         | 2003-01-01 09:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 09:00        | 0                                  | 940.4                                                   | 14                        | 21.4                                           | 19.5                                   | 89                                    | 142                                    | 3.2                          | 0.9                               | 2003  | BAURU    | -22.3581   | -49.0289    | bauru         | 2003-01-01 09:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 09:00        | 0                                  | 886.3                                                   | 8                         | 19                                             | 18.2                                   | 95                                    | 330                                    | 3.4                          | 1.6                               | 2003  | BRASILIA | -15.7894   | -47.9258    | brasilia      | 2003-01-01 09:00:00 | nan          | <NA>  | <NA>      | 0          |

## 1. Distribuição do Target (HAS_FOCO)
| Classe | Contagem | Proporção |
| :--- | :---: | :---: |
| Sem Fogo (0) | 16,801,528 | 99.4115% |
| **Fogo (1)** | 99,456 | 0.5885% |

## 2. Qualidade Global (Variáveis Climáticas)
| Variável | Nulos (NaN) | Sentinelas (<= -999) | Total Ausente | % da Base |
| :--- | :---: | :---: | :---: | :---: |
| `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)` | 0 | 0 | 0 | 0.00% |
| `RADIACAO GLOBAL (KJ/m²)` | 0 | 0 | 0 | 0.00% |
| `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)` | 0 | 0 | 0 | 0.00% |
| `UMIDADE RELATIVA DO AR, HORARIA (%)` | 0 | 0 | 0 | 0.00% |
| `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)` | 0 | 0 | 0 | 0.00% |
| `VENTO, VELOCIDADE HORARIA (m/s)` | 0 | 0 | 0 | 0.00% |
| `VENTO, DIREÇÃO HORARIA (gr) (° (gr))` | 0 | 0 | 0 | 0.00% |
| `VENTO, RAJADA MAXIMA (m/s)` | 0 | 0 | 0 | 0.00% |

## 3. Detalhamento Temporal (Falhas Críticas)
| Ano | Linhas | Focos | Temp (Falhas) | Rad (Falhas) | Pressão (Falhas) | Vento (Falhas) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 2003 | 75,512 | 490 | 0 | 0 | 0 | 0 |
| 2004 | 66,612 | 220 | 0 | 0 | 0 | 0 |
| 2005 | 115,742 | 642 | 0 | 0 | 0 | 0 |
| 2006 | 155,492 | 1,206 | 0 | 0 | 0 | 0 |
| 2007 | 631,982 | 5,864 | 0 | 0 | 0 | 0 |
| 2008 | 1,122,416 | 7,078 | 0 | 0 | 0 | 0 |
| 2009 | 1,232,760 | 4,932 | 0 | 0 | 0 | 0 |
| 2011 | 1,205,404 | 6,482 | 0 | 0 | 0 | 0 |
| 2012 | 1,246,030 | 7,846 | 0 | 0 | 0 | 0 |
| 2013 | 1,241,506 | 6,150 | 0 | 0 | 0 | 0 |
| 2014 | 1,202,696 | 6,898 | 0 | 0 | 0 | 0 |
| 2015 | 1,213,138 | 7,306 | 0 | 0 | 0 | 0 |
| 2016 | 1,235,266 | 7,536 | 0 | 0 | 0 | 0 |
| 2017 | 1,312,656 | 7,908 | 0 | 0 | 0 | 0 |
| 2018 | 1,509,060 | 6,932 | 0 | 0 | 0 | 0 |
| 2019 | 76,720 | 266 | 0 | 0 | 0 | 0 |
| 2020 | 96,124 | 478 | 0 | 0 | 0 | 0 |
| 2021 | 56,374 | 174 | 0 | 0 | 0 | 0 |
| 2022 | 1,138,148 | 6,776 | 0 | 0 | 0 | 0 |
| 2023 | 808,488 | 5,792 | 0 | 0 | 0 | 0 |
| 2024 | 1,158,858 | 8,480 | 0 | 0 | 0 | 0 |
