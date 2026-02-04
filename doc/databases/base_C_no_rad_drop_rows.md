# Auditoria Consolidada: base_C_no_rad_drop_rows

**Arquivos Processados:** 2003 a 2024
**Total de Registros:** 30,519,868

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
| Sem Fogo (0) | 30,417,598 | 99.6649% |
| **Fogo (1)** | 102,270 | 0.3351% |

## 2. Qualidade Global (Variáveis Climáticas)
| Variável | Nulos (NaN) | Sentinelas (<= -999) | Total Ausente | % da Base |
| :--- | :---: | :---: | :---: | :---: |
| `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)` | 0 | 0 | 0 | 0.00% |
| `RADIACAO GLOBAL (KJ/m²)` | 30,519,868 | 0 | 30,519,868 | 100.00% |
| `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)` | 0 | 0 | 0 | 0.00% |
| `UMIDADE RELATIVA DO AR, HORARIA (%)` | 0 | 0 | 0 | 0.00% |
| `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)` | 0 | 0 | 0 | 0.00% |
| `VENTO, VELOCIDADE HORARIA (m/s)` | 0 | 0 | 0 | 0.00% |
| `VENTO, DIREÇÃO HORARIA (gr) (° (gr))` | 0 | 0 | 0 | 0.00% |
| `VENTO, RAJADA MAXIMA (m/s)` | 0 | 0 | 0 | 0.00% |

## 3. Detalhamento Temporal (Falhas Críticas)
| Ano | Linhas | Focos | Temp (Falhas) | Rad (Falhas) | Pressão (Falhas) | Vento (Falhas) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| 2003 | 184,476 | 986 | 0 | 184,476 | 0 | 0 |
| 2004 | 197,542 | 992 | 0 | 197,542 | 0 | 0 |
| 2005 | 284,192 | 1,182 | 0 | 284,192 | 0 | 0 |
| 2006 | 282,336 | 1,208 | 0 | 282,336 | 0 | 0 |
| 2007 | 1,154,308 | 5,868 | 0 | 1,154,308 | 0 | 0 |
| 2008 | 1,945,928 | 7,120 | 0 | 1,945,928 | 0 | 0 |
| 2009 | 2,081,104 | 4,938 | 0 | 2,081,104 | 0 | 0 |
| 2011 | 2,130,338 | 6,490 | 0 | 2,130,338 | 0 | 0 |
| 2012 | 2,278,910 | 7,850 | 0 | 2,278,910 | 0 | 0 |
| 2013 | 2,303,118 | 6,202 | 0 | 2,303,118 | 0 | 0 |
| 2014 | 2,247,042 | 7,190 | 0 | 2,247,042 | 0 | 0 |
| 2015 | 2,243,896 | 7,452 | 0 | 2,243,896 | 0 | 0 |
| 2016 | 2,279,136 | 7,616 | 0 | 2,279,136 | 0 | 0 |
| 2017 | 2,411,398 | 7,940 | 0 | 2,411,398 | 0 | 0 |
| 2018 | 2,744,998 | 7,044 | 0 | 2,744,998 | 0 | 0 |
| 2019 | 195,248 | 266 | 0 | 195,248 | 0 | 0 |
| 2020 | 234,672 | 478 | 0 | 234,672 | 0 | 0 |
| 2021 | 130,478 | 174 | 0 | 130,478 | 0 | 0 |
| 2022 | 1,911,110 | 6,854 | 0 | 1,911,110 | 0 | 0 |
| 2023 | 1,309,384 | 5,852 | 0 | 1,309,384 | 0 | 0 |
| 2024 | 1,970,254 | 8,568 | 0 | 1,970,254 | 0 | 0 |
