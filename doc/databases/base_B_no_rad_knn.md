# Auditoria Consolidada: base_B_no_rad_knn

**Arquivos Processados:** 2003 a 2024
**Total de Registros:** 45,434,876

## 0. Amostra e Estrutura dos Dados
> **Arquivo de Referência:** `inmet_bdq_2003_cerrado.parquet`

**Total de Colunas:** 20

**Lista de Colunas:**
`DATA (YYYY-MM-DD), HORA (UTC), PRECIPITAÇÃO TOTAL, HORÁRIO (mm), PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB), TEMPERATURA DO AR - BULBO SECO, HORARIA (°C), TEMPERATURA DO PONTO DE ORVALHO (°C), UMIDADE RELATIVA DO AR, HORARIA (%), VENTO, DIREÇÃO HORARIA (gr) (° (gr)), VENTO, RAJADA MAXIMA (m/s), VENTO, VELOCIDADE HORARIA (m/s), ANO, CIDADE, LATITUDE, LONGITUDE, cidade_norm, ts_hour, RISCO_FOGO, FRP, FOCO_ID, HAS_FOCO`

**Preview (5 primeiras linhas):**
| DATA (YYYY-MM-DD)   | HORA (UTC)   | PRECIPITAÇÃO TOTAL, HORÁRIO (mm)   | PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)   | TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)   | TEMPERATURA DO PONTO DE ORVALHO (°C)   | UMIDADE RELATIVA DO AR, HORARIA (%)   | VENTO, DIREÇÃO HORARIA (gr) (° (gr))   | VENTO, RAJADA MAXIMA (m/s)   | VENTO, VELOCIDADE HORARIA (m/s)   | ANO   | CIDADE   | LATITUDE   | LONGITUDE   | cidade_norm   | ts_hour             | RISCO_FOGO   | FRP   | FOCO_ID   | HAS_FOCO   |
|:--------------------|:-------------|:-----------------------------------|:--------------------------------------------------------|:-----------------------------------------------|:---------------------------------------|:--------------------------------------|:---------------------------------------|:-----------------------------|:----------------------------------|:------|:---------|:-----------|:------------|:--------------|:--------------------|:-------------|:------|:----------|:-----------|
| 2003-01-01          | 01:00        | 0                                  | 897.3                                                   | 21.2                                           | 19.5                                   | 90                                    | 142                                    | 4.2                          | 0.8                               | 2003  | ARAXA    | -19.6056   | -46.9494    | araxa         | 2003-01-01 01:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 01:00        | 0                                  | 897.3                                                   | 21.2                                           | 19.5                                   | 90                                    | 142                                    | 4.2                          | 0.8                               | 2003  | ARAXA    | -19.6056   | -46.9494    | araxa         | 2003-01-01 01:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 01:00        | 0                                  | 941.1                                                   | 26                                             | 18.7                                   | 64                                    | 5                                      | 1.8                          | 0.4                               | 2003  | BAURU    | -22.3581   | -49.0289    | bauru         | 2003-01-01 01:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 01:00        | 0                                  | 941.1                                                   | 26                                             | 18.7                                   | 64                                    | 5                                      | 1.8                          | 0.4                               | 2003  | BAURU    | -22.3581   | -49.0289    | bauru         | 2003-01-01 01:00:00 | nan          | <NA>  | <NA>      | 0          |
| 2003-01-01          | 01:00        | 0                                  | 887.3                                                   | 19.9                                           | 18.5                                   | 92                                    | 302                                    | 2.2                          | 1.3                               | 2003  | BRASILIA | -15.7894   | -47.9258    | brasilia      | 2003-01-01 01:00:00 | nan          | <NA>  | <NA>      | 0          |

## 1. Distribuição do Target (HAS_FOCO)
| Classe | Contagem | Proporção |
| :--- | :---: | :---: |
| Sem Fogo (0) | 45,282,000 | 99.6635% |
| **Fogo (1)** | 152,876 | 0.3365% |

## 2. Qualidade Global (Variáveis Climáticas)
| Variável | Nulos (NaN) | Sentinelas (<= -999) | Total Ausente | % da Base |
| :--- | :---: | :---: | :---: | :---: |
| `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)` | 0 | 0 | 0 | 0.00% |
| `RADIACAO GLOBAL (KJ/m²)` | 45,434,876 | 0 | 45,434,876 | 100.00% |
| `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)` | 0 | 0 | 0 | 0.00% |
| `UMIDADE RELATIVA DO AR, HORARIA (%)` | 0 | 0 | 0 | 0.00% |
| `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)` | 0 | 0 | 0 | 0.00% |
| `VENTO, VELOCIDADE HORARIA (m/s)` | 0 | 0 | 0 | 0.00% |
| `VENTO, DIREÇÃO HORARIA (gr) (° (gr))` | 0 | 0 | 0 | 0.00% |
| `VENTO, RAJADA MAXIMA (m/s)` | 0 | 0 | 0 | 0.00% |

## 3. Detalhamento Temporal (Falhas Críticas)
| Ano | Linhas | Focos | Temp (Falhas) | Rad (Falhas) | Pressão (Falhas) | Vento (Falhas) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 2003 | 198,028 | 1,054 | 0 | 198,028 | 0 | 0 |
| 2004 | 198,244 | 994 | 0 | 198,244 | 0 | 0 |
| 2005 | 298,952 | 1,332 | 0 | 298,952 | 0 | 0 |
| 2006 | 302,130 | 1,236 | 0 | 302,130 | 0 | 0 |
| 2007 | 1,181,586 | 5,902 | 0 | 1,181,586 | 0 | 0 |
| 2008 | 2,000,366 | 7,222 | 0 | 2,000,366 | 0 | 0 |
| 2009 | 2,185,516 | 5,076 | 0 | 2,185,516 | 0 | 0 |
| 2011 | 2,298,104 | 6,990 | 0 | 2,298,104 | 0 | 0 |
| 2012 | 2,442,676 | 8,450 | 0 | 2,442,676 | 0 | 0 |
| 2013 | 2,505,914 | 6,926 | 0 | 2,505,914 | 0 | 0 |
| 2014 | 2,510,432 | 8,132 | 0 | 2,510,432 | 0 | 0 |
| 2015 | 2,462,726 | 8,282 | 0 | 2,462,726 | 0 | 0 |
| 2016 | 2,583,240 | 8,854 | 0 | 2,583,240 | 0 | 0 |
| 2017 | 2,734,100 | 9,090 | 0 | 2,734,100 | 0 | 0 |
| 2018 | 3,053,532 | 7,732 | 0 | 3,053,532 | 0 | 0 |
| 2019 | 3,378,380 | 11,492 | 0 | 3,378,380 | 0 | 0 |
| 2020 | 3,407,804 | 11,388 | 0 | 3,407,804 | 0 | 0 |
| 2021 | 3,433,528 | 11,288 | 0 | 3,433,528 | 0 | 0 |
| 2022 | 3,153,240 | 10,628 | 0 | 3,153,240 | 0 | 0 |
| 2023 | 1,944,498 | 8,068 | 0 | 1,944,498 | 0 | 0 |
| 2024 | 3,161,880 | 12,740 | 0 | 3,161,880 | 0 | 0 |
