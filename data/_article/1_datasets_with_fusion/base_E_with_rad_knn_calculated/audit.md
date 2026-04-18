# Auditoria — `base_E_with_rad_knn_calculated`

**Gerado em:** 2026-04-18 16:59 UTC
**Caminho:** `D:\Projetos\TCC\data\_article\1_datasets_with_fusion\base_E_with_rad_knn_calculated`

Este ficheiro resume consistência de **schema**, **colunas** e **contagens de linhas** 
entre todos os parquets por subpasta de método (`ewma_lags`, `minirocket`, `champion`, …).

---

## Resumo executivo

- **Estado:** **AVISOS** — ver tipos relevantes ou coords abaixo.

### Cobertura de anos (por método)

| Método | Anos (n) |
|--------|-----------|
| champion | 22 |
| ewma_lags | 22 |
| minirocket | 22 |
| sarimax_exog | 1 |

- Anos **alinhados** entre `ewma_lags`, `minirocket` e `champion` (22 anos).
- `sarimax_exog`: apenas **1** ano(s) de parquet (cobertura parcial vs 22 nos outros métodos — normal se o pipeline não gerou todos os anos).

---

## `ewma_lags/`

### Avisos leves (precisão float)
- 24 coluna(s) com `float` vs `double` entre ficheiros (comum entre anos). Leitura em pandas unifica sem perda de desenho experimental.

### Avisos relevantes
- tipo divergente para 'RISCO_FOGO': double em 11 ficheiro(s); string em 11 ficheiro(s)

- **Ficheiros:** 22 | **Colunas (ref.):** 73 | **tsf_*:** 34

| Ano | Ficheiro | Linhas | Colunas | tsf_* |
|-----|----------|--------|---------|-------|
| 2003 | `inmet_bdq_2003_cerrado.parquet` | 396,056 | 73 | 34 |
| 2004 | `inmet_bdq_2004_cerrado.parquet` | 396,488 | 73 | 34 |
| 2005 | `inmet_bdq_2005_cerrado.parquet` | 597,904 | 73 | 34 |
| 2006 | `inmet_bdq_2006_cerrado.parquet` | 604,260 | 73 | 34 |
| 2007 | `inmet_bdq_2007_cerrado.parquet` | 2,363,172 | 73 | 34 |
| 2008 | `inmet_bdq_2008_cerrado.parquet` | 4,000,732 | 73 | 34 |
| 2009 | `inmet_bdq_2009_cerrado.parquet` | 4,371,032 | 73 | 34 |
| 2010 | `inmet_bdq_2010_cerrado.parquet` | 4,423,228 | 73 | 34 |
| 2011 | `inmet_bdq_2011_cerrado.parquet` | 4,596,208 | 73 | 34 |
| 2012 | `inmet_bdq_2012_cerrado.parquet` | 4,885,352 | 73 | 34 |
| 2013 | `inmet_bdq_2013_cerrado.parquet` | 5,011,828 | 73 | 34 |
| 2014 | `inmet_bdq_2014_cerrado.parquet` | 5,020,864 | 73 | 34 |
| 2015 | `inmet_bdq_2015_cerrado.parquet` | 4,925,452 | 73 | 34 |
| 2016 | `inmet_bdq_2016_cerrado.parquet` | 5,166,480 | 73 | 34 |
| 2017 | `inmet_bdq_2017_cerrado.parquet` | 5,468,200 | 73 | 34 |
| 2018 | `inmet_bdq_2018_cerrado.parquet` | 6,107,064 | 73 | 34 |
| 2019 | `inmet_bdq_2019_cerrado.parquet` | 6,791,696 | 73 | 34 |
| 2020 | `inmet_bdq_2020_cerrado.parquet` | 6,885,872 | 73 | 34 |
| 2021 | `inmet_bdq_2021_cerrado.parquet` | 6,937,128 | 73 | 34 |
| 2022 | `inmet_bdq_2022_cerrado.parquet` | 6,376,552 | 73 | 34 |
| 2023 | `inmet_bdq_2023_cerrado.parquet` | 3,888,996 | 73 | 34 |
| 2024 | `inmet_bdq_2024_cerrado.parquet` | 6,394,024 | 73 | 34 |

### Colunas (referência: primeiro ficheiro ordenado)

- `DATA (YYYY-MM-DD)`, `HORA (UTC)`, `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)`, `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)`, `RADIACAO GLOBAL (KJ/m²)`, `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)`, `TEMPERATURA DO PONTO DE ORVALHO (°C)`, `UMIDADE RELATIVA DO AR, HORARIA (%)`
- `VENTO, DIREÇÃO HORARIA (gr) (° (gr))`, `VENTO, RAJADA MAXIMA (m/s)`, `VENTO, VELOCIDADE HORARIA (m/s)`, `ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`, `cidade_norm`
- `ts_hour`, `RISCO_FOGO`, `FRP`, `FOCO_ID`, `HAS_FOCO`, `precip_ewma`, `dias_sem_chuva`, `risco_temp_max`
- `risco_umid_critica`, `risco_umid_alerta`, `fator_propagacao`, `lat_foco`, `lon_foco`, `lat_station`, `lon_station`, `geo_version`
- `coord_source_foco`, `foco_coords_from_bdq`, `gee_site_key`, `NDVI_buffer`, `EVI_buffer`, `NDVI_point`, `EVI_point`, `tsf_ewma_precip_a01`
- `tsf_ewma_precip_a03`, `tsf_ewma_precip_a08`, `tsf_lag_precip_1h`, `tsf_lag_precip_24h`, `tsf_lag_precip_168h`, `tsf_ewma_temp_a01`, `tsf_ewma_temp_a03`, `tsf_ewma_temp_a08`
- `tsf_lag_temp_1h`, `tsf_lag_temp_24h`, `tsf_lag_temp_168h`, `tsf_ewma_umid_a01`, `tsf_ewma_umid_a03`, `tsf_ewma_umid_a08`, `tsf_lag_umid_1h`, `tsf_lag_umid_24h`
- `tsf_lag_umid_168h`, `tsf_ewma_rad_a01`, `tsf_ewma_rad_a03`, `tsf_ewma_rad_a08`, `tsf_lag_rad_1h`, `tsf_lag_rad_24h`, `tsf_lag_rad_168h`, `tsf_ewma_ndvi_buffer_a01`
- `tsf_ewma_ndvi_buffer_a03`, `tsf_ewma_ndvi_buffer_a08`, `tsf_lag_ndvi_buffer_168h`, `tsf_lag_ndvi_buffer_336h`, `tsf_ewma_evi_buffer_a01`, `tsf_ewma_evi_buffer_a03`, `tsf_ewma_evi_buffer_a08`, `tsf_lag_evi_buffer_168h`
- `tsf_lag_evi_buffer_336h`

---

## `sarimax_exog/`

- **Ficheiros:** 1 | **Colunas (ref.):** 41 | **tsf_*:** 2

| Ano | Ficheiro | Linhas | Colunas | tsf_* |
|-----|----------|--------|---------|-------|
| 2020 | `inmet_bdq_2020_cerrado.parquet` | 6,885,872 | 41 | 2 |

### Colunas (referência: primeiro ficheiro ordenado)

- `DATA (YYYY-MM-DD)`, `HORA (UTC)`, `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)`, `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)`, `RADIACAO GLOBAL (KJ/m²)`, `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)`, `TEMPERATURA DO PONTO DE ORVALHO (°C)`, `UMIDADE RELATIVA DO AR, HORARIA (%)`
- `VENTO, DIREÇÃO HORARIA (gr) (° (gr))`, `VENTO, RAJADA MAXIMA (m/s)`, `VENTO, VELOCIDADE HORARIA (m/s)`, `ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`, `cidade_norm`
- `ts_hour`, `RISCO_FOGO`, `FRP`, `FOCO_ID`, `HAS_FOCO`, `precip_ewma`, `dias_sem_chuva`, `risco_temp_max`
- `risco_umid_critica`, `risco_umid_alerta`, `fator_propagacao`, `lat_foco`, `lon_foco`, `lat_station`, `lon_station`, `geo_version`
- `coord_source_foco`, `foco_coords_from_bdq`, `gee_site_key`, `NDVI_buffer`, `EVI_buffer`, `NDVI_point`, `EVI_point`, `tsf_sarimax_exog_umid_pred`
- `tsf_sarimax_exog_umid_resid`

---

## `minirocket/`

### Avisos leves (precisão float)
- 12 coluna(s) com `float` vs `double` entre ficheiros (comum entre anos). Leitura em pandas unifica sem perda de desenho experimental.

### Avisos relevantes
- tipo divergente para 'RISCO_FOGO': double em 11 ficheiro(s); string em 11 ficheiro(s)

- **Ficheiros:** 22 | **Colunas (ref.):** 207 | **tsf_*:** 168

| Ano | Ficheiro | Linhas | Colunas | tsf_* |
|-----|----------|--------|---------|-------|
| 2003 | `inmet_bdq_2003_cerrado.parquet` | 396,056 | 207 | 168 |
| 2004 | `inmet_bdq_2004_cerrado.parquet` | 396,488 | 207 | 168 |
| 2005 | `inmet_bdq_2005_cerrado.parquet` | 597,904 | 207 | 168 |
| 2006 | `inmet_bdq_2006_cerrado.parquet` | 604,260 | 207 | 168 |
| 2007 | `inmet_bdq_2007_cerrado.parquet` | 2,363,172 | 207 | 168 |
| 2008 | `inmet_bdq_2008_cerrado.parquet` | 4,000,732 | 207 | 168 |
| 2009 | `inmet_bdq_2009_cerrado.parquet` | 4,371,032 | 207 | 168 |
| 2010 | `inmet_bdq_2010_cerrado.parquet` | 4,423,228 | 207 | 168 |
| 2011 | `inmet_bdq_2011_cerrado.parquet` | 4,596,208 | 207 | 168 |
| 2012 | `inmet_bdq_2012_cerrado.parquet` | 4,885,352 | 207 | 168 |
| 2013 | `inmet_bdq_2013_cerrado.parquet` | 5,011,828 | 207 | 168 |
| 2014 | `inmet_bdq_2014_cerrado.parquet` | 5,020,864 | 207 | 168 |
| 2015 | `inmet_bdq_2015_cerrado.parquet` | 4,925,452 | 207 | 168 |
| 2016 | `inmet_bdq_2016_cerrado.parquet` | 5,166,480 | 207 | 168 |
| 2017 | `inmet_bdq_2017_cerrado.parquet` | 5,468,200 | 207 | 168 |
| 2018 | `inmet_bdq_2018_cerrado.parquet` | 6,107,064 | 207 | 168 |
| 2019 | `inmet_bdq_2019_cerrado.parquet` | 6,791,696 | 207 | 168 |
| 2020 | `inmet_bdq_2020_cerrado.parquet` | 6,885,872 | 207 | 168 |
| 2021 | `inmet_bdq_2021_cerrado.parquet` | 6,937,128 | 207 | 168 |
| 2022 | `inmet_bdq_2022_cerrado.parquet` | 6,376,552 | 207 | 168 |
| 2023 | `inmet_bdq_2023_cerrado.parquet` | 3,888,996 | 207 | 168 |
| 2024 | `inmet_bdq_2024_cerrado.parquet` | 6,394,024 | 207 | 168 |

### Colunas (referência: primeiro ficheiro ordenado)

- `DATA (YYYY-MM-DD)`, `HORA (UTC)`, `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)`, `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)`, `RADIACAO GLOBAL (KJ/m²)`, `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)`, `TEMPERATURA DO PONTO DE ORVALHO (°C)`, `UMIDADE RELATIVA DO AR, HORARIA (%)`
- `VENTO, DIREÇÃO HORARIA (gr) (° (gr))`, `VENTO, RAJADA MAXIMA (m/s)`, `VENTO, VELOCIDADE HORARIA (m/s)`, `ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`, `cidade_norm`
- `ts_hour`, `RISCO_FOGO`, `FRP`, `FOCO_ID`, `HAS_FOCO`, `precip_ewma`, `dias_sem_chuva`, `risco_temp_max`
- `risco_umid_critica`, `risco_umid_alerta`, `fator_propagacao`, `lat_foco`, `lon_foco`, `lat_station`, `lon_station`, `geo_version`
- `coord_source_foco`, `foco_coords_from_bdq`, `gee_site_key`, `NDVI_buffer`, `EVI_buffer`, `NDVI_point`, `EVI_point`, `tsf_minirocket_f000`
- `tsf_minirocket_f001`, `tsf_minirocket_f002`, `tsf_minirocket_f003`, `tsf_minirocket_f004`, `tsf_minirocket_f005`, `tsf_minirocket_f006`, `tsf_minirocket_f007`, `tsf_minirocket_f008`
- `tsf_minirocket_f009`, `tsf_minirocket_f010`, `tsf_minirocket_f011`, `tsf_minirocket_f012`, `tsf_minirocket_f013`, `tsf_minirocket_f014`, `tsf_minirocket_f015`, `tsf_minirocket_f016`
- `tsf_minirocket_f017`, `tsf_minirocket_f018`, `tsf_minirocket_f019`, `tsf_minirocket_f020`, `tsf_minirocket_f021`, `tsf_minirocket_f022`, `tsf_minirocket_f023`, `tsf_minirocket_f024`
- `tsf_minirocket_f025`, `tsf_minirocket_f026`, `tsf_minirocket_f027`, `tsf_minirocket_f028`, `tsf_minirocket_f029`, `tsf_minirocket_f030`, `tsf_minirocket_f031`, `tsf_minirocket_f032`
- `tsf_minirocket_f033`, `tsf_minirocket_f034`, `tsf_minirocket_f035`, `tsf_minirocket_f036`, `tsf_minirocket_f037`, `tsf_minirocket_f038`, `tsf_minirocket_f039`, `tsf_minirocket_f040`
- `tsf_minirocket_f041`, `tsf_minirocket_f042`, `tsf_minirocket_f043`, `tsf_minirocket_f044`, `tsf_minirocket_f045`, `tsf_minirocket_f046`, `tsf_minirocket_f047`, `tsf_minirocket_f048`
- `tsf_minirocket_f049`, `tsf_minirocket_f050`, `tsf_minirocket_f051`, `tsf_minirocket_f052`, `tsf_minirocket_f053`, `tsf_minirocket_f054`, `tsf_minirocket_f055`, `tsf_minirocket_f056`
- `tsf_minirocket_f057`, `tsf_minirocket_f058`, `tsf_minirocket_f059`, `tsf_minirocket_f060`, `tsf_minirocket_f061`, `tsf_minirocket_f062`, `tsf_minirocket_f063`, `tsf_minirocket_f064`
- `tsf_minirocket_f065`, `tsf_minirocket_f066`, `tsf_minirocket_f067`, `tsf_minirocket_f068`, `tsf_minirocket_f069`, `tsf_minirocket_f070`, `tsf_minirocket_f071`, `tsf_minirocket_f072`
- `tsf_minirocket_f073`, `tsf_minirocket_f074`, `tsf_minirocket_f075`, `tsf_minirocket_f076`, `tsf_minirocket_f077`, `tsf_minirocket_f078`, `tsf_minirocket_f079`, `tsf_minirocket_f080`
- `tsf_minirocket_f081`, `tsf_minirocket_f082`, `tsf_minirocket_f083`, `tsf_minirocket_f084`, `tsf_minirocket_f085`, `tsf_minirocket_f086`, `tsf_minirocket_f087`, `tsf_minirocket_f088`
- `tsf_minirocket_f089`, `tsf_minirocket_f090`, `tsf_minirocket_f091`, `tsf_minirocket_f092`, `tsf_minirocket_f093`, `tsf_minirocket_f094`, `tsf_minirocket_f095`, `tsf_minirocket_f096`
- `tsf_minirocket_f097`, `tsf_minirocket_f098`, `tsf_minirocket_f099`, `tsf_minirocket_f100`, `tsf_minirocket_f101`, `tsf_minirocket_f102`, `tsf_minirocket_f103`, `tsf_minirocket_f104`
- `tsf_minirocket_f105`, `tsf_minirocket_f106`, `tsf_minirocket_f107`, `tsf_minirocket_f108`, `tsf_minirocket_f109`, `tsf_minirocket_f110`, `tsf_minirocket_f111`, `tsf_minirocket_f112`
- `tsf_minirocket_f113`, `tsf_minirocket_f114`, `tsf_minirocket_f115`, `tsf_minirocket_f116`, `tsf_minirocket_f117`, `tsf_minirocket_f118`, `tsf_minirocket_f119`, `tsf_minirocket_f120`
- `tsf_minirocket_f121`, `tsf_minirocket_f122`, `tsf_minirocket_f123`, `tsf_minirocket_f124`, `tsf_minirocket_f125`, `tsf_minirocket_f126`, `tsf_minirocket_f127`, `tsf_minirocket_f128`
- `tsf_minirocket_f129`, `tsf_minirocket_f130`, `tsf_minirocket_f131`, `tsf_minirocket_f132`, `tsf_minirocket_f133`, `tsf_minirocket_f134`, `tsf_minirocket_f135`, `tsf_minirocket_f136`
- `tsf_minirocket_f137`, `tsf_minirocket_f138`, `tsf_minirocket_f139`, `tsf_minirocket_f140`, `tsf_minirocket_f141`, `tsf_minirocket_f142`, `tsf_minirocket_f143`, `tsf_minirocket_f144`
- `tsf_minirocket_f145`, `tsf_minirocket_f146`, `tsf_minirocket_f147`, `tsf_minirocket_f148`, `tsf_minirocket_f149`, `tsf_minirocket_f150`, `tsf_minirocket_f151`, `tsf_minirocket_f152`
- `tsf_minirocket_f153`, `tsf_minirocket_f154`, `tsf_minirocket_f155`, `tsf_minirocket_f156`, `tsf_minirocket_f157`, `tsf_minirocket_f158`, `tsf_minirocket_f159`, `tsf_minirocket_f160`
- `tsf_minirocket_f161`, `tsf_minirocket_f162`, `tsf_minirocket_f163`, `tsf_minirocket_f164`, `tsf_minirocket_f165`, `tsf_minirocket_f166`, `tsf_minirocket_f167`

---

## `champion/`

### Avisos leves (precisão float)
- 25 coluna(s) com `float` vs `double` entre ficheiros (comum entre anos). Leitura em pandas unifica sem perda de desenho experimental.

### Avisos relevantes
- tipo divergente para 'RISCO_FOGO': double em 11 ficheiro(s); string em 11 ficheiro(s)

- **Ficheiros:** 22 | **Colunas (ref.):** 89 | **tsf_*:** 50

| Ano | Ficheiro | Linhas | Colunas | tsf_* |
|-----|----------|--------|---------|-------|
| 2003 | `inmet_bdq_2003_cerrado.parquet` | 6,336,896 | 89 | 50 |
| 2004 | `inmet_bdq_2004_cerrado.parquet` | 6,343,808 | 89 | 50 |
| 2005 | `inmet_bdq_2005_cerrado.parquet` | 9,566,464 | 89 | 50 |
| 2006 | `inmet_bdq_2006_cerrado.parquet` | 9,668,160 | 89 | 50 |
| 2007 | `inmet_bdq_2007_cerrado.parquet` | 2,363,172 | 89 | 50 |
| 2008 | `inmet_bdq_2008_cerrado.parquet` | 4,000,732 | 89 | 50 |
| 2009 | `inmet_bdq_2009_cerrado.parquet` | 4,371,032 | 89 | 50 |
| 2010 | `inmet_bdq_2010_cerrado.parquet` | 4,423,228 | 89 | 50 |
| 2011 | `inmet_bdq_2011_cerrado.parquet` | 4,596,208 | 89 | 50 |
| 2012 | `inmet_bdq_2012_cerrado.parquet` | 4,885,352 | 89 | 50 |
| 2013 | `inmet_bdq_2013_cerrado.parquet` | 5,011,828 | 89 | 50 |
| 2014 | `inmet_bdq_2014_cerrado.parquet` | 5,020,864 | 89 | 50 |
| 2015 | `inmet_bdq_2015_cerrado.parquet` | 4,925,452 | 89 | 50 |
| 2016 | `inmet_bdq_2016_cerrado.parquet` | 5,166,480 | 89 | 50 |
| 2017 | `inmet_bdq_2017_cerrado.parquet` | 5,468,200 | 89 | 50 |
| 2018 | `inmet_bdq_2018_cerrado.parquet` | 6,107,064 | 89 | 50 |
| 2019 | `inmet_bdq_2019_cerrado.parquet` | 6,791,696 | 89 | 50 |
| 2020 | `inmet_bdq_2020_cerrado.parquet` | 6,885,872 | 89 | 50 |
| 2021 | `inmet_bdq_2021_cerrado.parquet` | 6,937,128 | 89 | 50 |
| 2022 | `inmet_bdq_2022_cerrado.parquet` | 6,376,552 | 89 | 50 |
| 2023 | `inmet_bdq_2023_cerrado.parquet` | 3,888,996 | 89 | 50 |
| 2024 | `inmet_bdq_2024_cerrado.parquet` | 6,394,024 | 89 | 50 |

### Colunas (referência: primeiro ficheiro ordenado)

- `DATA (YYYY-MM-DD)`, `HORA (UTC)`, `PRECIPITAÇÃO TOTAL, HORÁRIO (mm)`, `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)`, `RADIACAO GLOBAL (KJ/m²)`, `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)`, `TEMPERATURA DO PONTO DE ORVALHO (°C)`, `UMIDADE RELATIVA DO AR, HORARIA (%)`
- `VENTO, DIREÇÃO HORARIA (gr) (° (gr))`, `VENTO, RAJADA MAXIMA (m/s)`, `VENTO, VELOCIDADE HORARIA (m/s)`, `ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`, `cidade_norm`
- `ts_hour`, `RISCO_FOGO`, `FRP`, `FOCO_ID`, `HAS_FOCO`, `precip_ewma`, `dias_sem_chuva`, `risco_temp_max`
- `risco_umid_critica`, `risco_umid_alerta`, `fator_propagacao`, `lat_foco`, `lon_foco`, `lat_station`, `lon_station`, `geo_version`
- `coord_source_foco`, `foco_coords_from_bdq`, `gee_site_key`, `NDVI_buffer`, `EVI_buffer`, `NDVI_point`, `EVI_point`, `tsf_minirocket_f098`
- `tsf_minirocket_f116`, `tsf_minirocket_f156`, `tsf_minirocket_f114`, `tsf_minirocket_f110`, `tsf_minirocket_f033`, `tsf_minirocket_f109`, `tsf_minirocket_f106`, `tsf_minirocket_f131`
- `tsf_minirocket_f148`, `tsf_minirocket_f092`, `tsf_minirocket_f088`, `tsf_minirocket_f132`, `tsf_minirocket_f012`, `tsf_minirocket_f122`, `tsf_minirocket_f147`, `tsf_minirocket_f155`
- `tsf_minirocket_f117`, `tsf_minirocket_f090`, `tsf_minirocket_f160`, `tsf_minirocket_f020`, `tsf_minirocket_f134`, `tsf_minirocket_f141`, `tsf_minirocket_f095`, `tsf_minirocket_f111`
- `tsf_minirocket_f149`, `tsf_minirocket_f067`, `tsf_minirocket_f094`, `tsf_minirocket_f135`, `tsf_minirocket_f140`, `tsf_minirocket_f105`, `tsf_minirocket_f091`, `tsf_minirocket_f157`
- `tsf_minirocket_f003`, `tsf_minirocket_f126`, `tsf_minirocket_f064`, `tsf_minirocket_f124`, `tsf_lag_rad_1h`, `tsf_ewma_rad_a08`, `tsf_ewma_temp_a08`, `tsf_ewma_umid_a08`
- `tsf_lag_rad_24h`, `tsf_lag_umid_24h`, `tsf_lag_rad_168h`, `tsf_lag_temp_168h`, `tsf_lag_umid_168h`, `tsf_ewma_rad_a01`, `tsf_ewma_umid_a01`, `tsf_ewma_evi_buffer_a08`
- `tsf_ewma_precip_a01`

---

## Comparação com `0_datasets_with_coords` (mesmo cenário)

Por ano, **`num_rows`** de cada método deve coincidir com o parquet de **coords** (mesma base de observações horárias).

**Interpretação:** se `champion` tem **mais** linhas que `coords`, costuma ser efeito de chaves duplicadas no merge (produto cartesiano). O `article_orchestrator` deduplica `(cidade_norm, ts_hour)` no `feat_df` antes do join — regenere champion com `--overwrite` se estes deltas forem inesperados.

Divergências registadas:

- [champion] 2003: rows=6336896 vs coords=396056 (delta 5940840)
- [champion] 2004: rows=6343808 vs coords=396488 (delta 5947320)
- [champion] 2005: rows=9566464 vs coords=597904 (delta 8968560)
- [champion] 2006: rows=9668160 vs coords=604260 (delta 9063900)

## Como regenerar

```bash
python -m src.article.audit_fusion_dataset --scenario base_E_with_rad_knn_calculated
```
