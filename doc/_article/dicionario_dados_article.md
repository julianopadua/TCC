# Dicionario de Dados — Bases `_article` com Coordenadas e Biomassa GEE

**Ultima atualizacao:** 2026-04-16  
**Caminho raiz:** `data/_article/0_datasets_with_coords/`  
**Formato dos arquivos:** Apache Parquet (`inmet_bdq_{ano}_cerrado.parquet`)

---

## 1. Visao geral

Os datasets do artigo sao derivados das bases de modelagem `*_calculated` (cenarios D, E e F) processadas por dois pipelines sequenciais:

1. **Etapa 0 — Enriquecimento espacial** (`src/article/enrich_coords.py`): adiciona coordenadas de foco (BDQueimadas) e de estacao (INMET) a cada linha horaria.
2. **Etapa 1 — Extracao GEE de biomassa** (`src/article/gee_biomass.py`): acopla series semanais de NDVI e EVI (MODIS MOD13Q1) via `merge_asof` e `ffill`, propagando para os tres cenarios.

### Cenarios disponiveis

| Chave config | Pasta em `0_datasets_with_coords/` | Descricao |
|---|---|---|
| `D` | `base_D_with_rad_drop_rows_calculated` | Radiacao inclusa; linhas com NaN em radiacao removidas; features INPE calculadas |
| `E` | `base_E_with_rad_knn_calculated` | Radiacao inclusa; NaN imputados por KNN; features INPE calculadas (cenario canonico GEE) |
| `F` | `base_F_full_original_calculated` | Base completa original com NaN preservados; features INPE calculadas |

### Granularidade e volume

- **Granularidade temporal:** horaria (1 registro = 1 hora UTC por estacao por foco ativo ou ausencia de foco)
- **Granularidade espacial:** cidade normalizada (`cidade_norm`) x ponto de foco (`gee_site_key`)
- **Periodo:** 2003 a 2024 (22 anos)
- **Volume exemplar:** ano 2020, cenario E — 6.885.872 linhas x 39 colunas

---

## 2. Dicionario de colunas

As 39 colunas estao organizadas em 6 grupos funcionais.

### 2.1 Identificacao temporal

| Coluna | Tipo | NaN | Descricao |
|---|---|---|---|
| `DATA (YYYY-MM-DD)` | string | 0% | Data no formato ISO (ex.: `2020-01-01`) |
| `HORA (UTC)` | string | 0% | Hora em UTC (ex.: `0100 UTC`, `1300 UTC`) |
| `ts_hour` | string | 0% | Timestamp unificado `YYYY-MM-DD HH:MM:SS` combinando data e hora. Chave temporal principal para merges e ordenacao |
| `ANO` | int64 | 0% | Ano civil do registro |

### 2.2 Identificacao espacial e coordenadas

| Coluna | Tipo | NaN | Descricao |
|---|---|---|---|
| `CIDADE` | string | 0% | Nome da cidade como registrado pelo INMET (caixa alta, acentos) |
| `cidade_norm` | string | 0% | Nome normalizado (minusculo, sem acentos). Chave de join principal entre pipelines |
| `LATITUDE` | float32 | 0% | Latitude da estacao INMET no registro original |
| `LONGITUDE` | float32 | 0% | Longitude da estacao INMET no registro original |
| `lat_station` | float64 | 0% | Latitude mediana da estacao no ano (de `station_year_locations.csv`). Fallback: `LATITUDE` da linha |
| `lon_station` | float64 | 0% | Longitude mediana da estacao no ano. Fallback: `LONGITUDE` da linha |
| `geo_version` | int64 | 0% | Versao da geolocalizacao da estacao (muda quando a estacao e realocada fisicamente) |
| `lat_foco` | float64 | 0% | Latitude do foco de calor (BDQueimadas) quando `HAS_FOCO=1` e merge ok; coords da estacao caso contrario |
| `lon_foco` | float64 | 0% | Longitude do foco de calor. Mesma logica de fallback que `lat_foco` |
| `coord_source_foco` | object | 0% | Origem da coordenada do foco: `BDQ` (lat/lon reais do foco), `STATION_FALLBACK_POSITIVE` (positivo sem match no BDQ), `STATION_IMPUTED` (HAS_FOCO=0) |
| `foco_coords_from_bdq` | bool | 0% | `True` se lat/lon do foco vieram diretamente do BDQueimadas |
| `gee_site_key` | string | 0% | Chave estavel por foco para join com GEE: `foco_{FOCO_ID}` se disponivel; senao `{lat_5dec}_{lon_5dec}` |
| `FOCO_ID` | object | ~0% | ID do foco no BDQueimadas. `<NA>` para linhas sem foco (HAS_FOCO=0) |

### 2.3 Variaveis meteorologicas (INMET)

Registros horarios das estacoes automaticas do INMET, bioma Cerrado. Nas bases `*_calculated`, lacunas podem ter sido tratadas (E = KNN; D = drop rows com NaN em radiacao; F = preserva NaN).

| Coluna | Tipo | NaN (E) | Unidade | Descricao |
|---|---|---|---|---|
| `PRECIPITACAO TOTAL, HORARIO (mm)` | float32 | 0% | mm | Precipitacao acumulada na hora. Variavel central para risco de fogo (serie z classica) |
| `PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)` | float32 | 0% | mB | Pressao atmosferica ao nivel da estacao |
| `RADIACAO GLOBAL (KJ/m2)` | float32 | 0% | KJ/m2 | Radiacao solar global horaria. Ausente nas bases A/B/C; presente em D/E/F |
| `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)` | float32 | 0% | °C | Temperatura do ar |
| `TEMPERATURA DO PONTO DE ORVALHO (°C)` | float32 | 0% | °C | Temperatura do ponto de orvalho (indicador indireto de umidade) |
| `UMIDADE RELATIVA DO AR, HORARIA (%)` | float32 | 0% | % | Umidade relativa. Abaixo de 15% = critico para incendios |
| `VENTO, DIRECAO HORARIA (gr) (° (gr))` | float32 | 0% | graus | Direcao do vento |
| `VENTO, RAJADA MAXIMA (m/s)` | float32 | 0% | m/s | Rajada maxima de vento na hora |
| `VENTO, VELOCIDADE HORARIA (m/s)` | float32 | 0% | m/s | Velocidade media do vento |

**Nota sobre NaN:** no cenario E (KNN), as colunas meteorologicas tem 0% NaN gracias a imputacao KNN. No cenario F, pode haver NaN variavel (especialmente radiacao e precipitacao). No cenario D, linhas com NaN em radiacao foram removidas, o que reduz o volume total.

### 2.4 Features derivadas (physics-informed, INPE)

Calculadas em `src/feature_engineering_physics.py` usando a base E como referencia e propagadas para D e F via merge em `(cidade_norm, ts_hour)`. Baseadas no modelo de risco de fogo do INPE (documento RiscoFogo_Sucinto_v11_2019.pdf).

| Coluna | Tipo | Descricao |
|---|---|---|
| `precip_ewma` | float64 | Media movel exponencial da precipitacao (alpha=0.5, sem ajuste). Aproxima a "precipitacao ponderada" (PSE) do INPE — captura o efeito de memoria da chuva recente |
| `dias_sem_chuva` | float32 | Contador de horas consecutivas sem chuva (limiar: < 1mm) dividido por 24. Representa dias fracionarios de seca. Mantem estado entre anos para evitar reset em 01/Jan |
| `risco_temp_max` | int64 | Flag binaria: 1 se temperatura > 30°C (limiar critico INPE Tabela 2.2) |
| `risco_umid_critica` | int64 | Flag binaria: 1 se umidade relativa < 15% (risco extremo de incendio) |
| `risco_umid_alerta` | int64 | Flag binaria: 1 se 15% <= umidade < 30% (alerta de risco moderado) |
| `fator_propagacao` | float32 | Indice composto: `(velocidade_vento * temperatura) / (umidade + 1)`. Modela a interacao vento-calor-secura que acelera propagacao de incendios |

### 2.5 Biomassa / Vegetacao (GEE — MODIS MOD13Q1)

Indices de vegetacao extraidos do Google Earth Engine, colecao `MODIS/061/MOD13Q1` (compositos de 16 dias, resolucao 250m). Alinhados de serie semanal para granularidade horaria via `merge_asof(direction='backward')` + `ffill` por grupo `(cidade_norm, gee_site_key)`.

| Coluna | Tipo | NaN (E, 2020) | Descricao |
|---|---|---|---|
| `NDVI_buffer` | float64 | 0% | Normalized Difference Vegetation Index — media num buffer circular de 50km ao redor do foco/estacao. Escala 0–1 (ja multiplicado por 0.0001). Valores tipicos no Cerrado: 0.3 (seca) a 0.8 (chuvoso). Reflete saude/verdor da vegetacao na regiao ampla |
| `EVI_buffer` | float64 | 0% | Enhanced Vegetation Index — media no buffer de 50km. Escala 0–1. Menos sensivel a ruido atmosferico que NDVI; diferencia melhor areas com alta densidade de biomassa |
| `NDVI_point` | float64 | 0% | NDVI amostrado no ponto exato do foco (ou estacao quando HAS_FOCO=0). Reflete a vegetacao local |
| `EVI_point` | float64 | 0% | EVI amostrado no ponto exato |

**Notas sobre alinhamento temporal:**
- Os compositos MOD13Q1 tem resolucao temporal de 16 dias. Cada linha horaria recebe o valor do composito mais recente anterior ou igual a sua data (`merge_asof backward`).
- Apos o merge, aplica-se `ffill` dentro de cada grupo `(cidade_norm, gee_site_key)` para preencher eventuais gaps iniciais do ano (antes do primeiro composito disponivel).
- A extracao e feita no cenario canonico (E) e propagada para D e F via merge na chave `(cidade_norm, gee_site_key, ts_hour)`.

### 2.6 Variavel-alvo e metadados de fogo

| Coluna | Tipo | NaN | Descricao |
|---|---|---|---|
| `HAS_FOCO` | int64 | 0% | **Variavel-alvo binaria.** 1 = foco de calor detectado naquela hora/estacao (BDQueimadas, satelite Aqua Tarde); 0 = ausencia de deteccao. Desbalanceamento tipico: ~0.3% positivos |
| `RISCO_FOGO` | string | ~99.7% | Indice de risco de fogo reportado pelo BDQueimadas. Disponivel apenas para linhas com foco ativo. Maioria NA |
| `FRP` | string | ~99.7% | Fire Radiative Power — potencia radiativa do foco (MW). Apenas para linhas com deteccao. Maioria NA |

---

## 3. Relacoes entre variaveis e uso em modelagem

### 3.1 Chaves de join

| Contexto de join | Colunas-chave |
|---|---|
| Merge entre cenarios D/E/F | `(cidade_norm, ts_hour)` |
| Merge com BDQueimadas (focos) | `FOCO_ID` |
| Merge GEE biomassa semanal → horario | `(cidade_norm, gee_site_key, ts_hour)` |
| Merge features physics → bases | `(cidade_norm, ts_hour)` |

### 3.2 Candidatas a series primarias para fusao temporal

| Variavel | Slug | Justificativa |
|---|---|---|
| `PRECIPITACAO TOTAL, HORARIO (mm)` | `precip` | Serie z classica ja usada no pipeline de fusao temporal existente. Forte correlacao negativa com seca e fogo |
| `NDVI_buffer` | `ndvi_buffer` | Serie com sazonalidade clara (seca/chuvosa) e correlacao negativa esperada com `HAS_FOCO` |
| `NDVI_point` | `ndvi_point` | Biomassa local no ponto do foco; mais ruidosa que buffer mas potencialmente mais discriminativa |
| `EVI_buffer` | `evi_buffer` | Complementar ao NDVI; melhor saturacao em areas de alta biomassa |

### 3.3 Candidatas a variaveis exogenas (ARIMAX/SARIMAX)

| Variavel | Slug | Papel exogeno |
|---|---|---|
| `TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)` | `temp` | Driver rapido de secagem da vegetacao |
| `UMIDADE RELATIVA DO AR, HORARIA (%)` | `umid` | Variavel complementar a temperatura; umidade critica (<15%) e limiar de risco |
| `RADIACAO GLOBAL (KJ/m2)` | `rad` | Energia solar que intensifica evapotranspiracao e ressecamento |
| `VENTO, VELOCIDADE HORARIA (m/s)` | `vento` | Fator de propagacao; interage com secura para acelerar fogo |
| `NDVI_buffer` / `EVI_buffer` | — | Podem servir como exogenas de precipitacao (biomassa condiciona capacidade de retencao hidrica) |

### 3.4 Candidatas a canais multivariados (MiniROCKET/TSKMeans)

Janelas deslizantes com multiplos canais sao o insumo de MiniROCKET e TSKMeans. Os canais mais relevantes sao:

- **Meteorologicos:** precipitacao, temperatura, umidade, radiacao (4 canais, ja suportados)
- **Biomassa:** NDVI_buffer, NDVI_point, EVI_buffer, EVI_point (4 canais adicionais)
- **Physics-informed:** precip_ewma, dias_sem_chuva, fator_propagacao (3 canais adicionais)

A inclusao de canais de biomassa permite que o embedding capture regimes compostos (ex.: "seca prolongada + vegetacao ainda verde" vs "seca prolongada + vegetacao seca"), enriquecendo a representacao para o classificador final.

---

## 4. Quirks e limitacoes

1. **ffill da biomassa:** como MOD13Q1 tem resolucao de 16 dias, o valor de biomassa e constante dentro de cada composito. Isso cria "degraus" na serie horaria, nao uma curva continua. Modelos ARIMA podem interpretar esses degraus como estrutura a modelar.

2. **Duplicacao de linhas por foco:** quando ha multiplos focos simultaneos na mesma estacao/hora, a base pode ter linhas repetidas com `gee_site_key` diferentes. A agregacao por `(cidade_norm, ts_hour)` usada no pipeline temporal reduz isso via media.

3. **NaN por cenario:** a cobertura de NaN varia entre D, E e F. O cenario E (KNN) e o mais limpo; F pode ter series interrompidas que inviabilizam ARIMA/SARIMA em certas cidades/anos.

4. **RISCO_FOGO e FRP:** colunas com ~99.7% NA. Uteis apenas para analise exploratoria de linhas com foco ativo, nao como features de modelagem.

5. **Desbalanceamento de `HAS_FOCO`:** tipicamente ~0.3% positivos. PR-AUC e a metrica principal de avaliacao, nao acuracia.

---

*Documento gerado a partir da inspecao do parquet `inmet_bdq_2020_cerrado.parquet` (cenario E) e analise dos scripts `enrich_coords.py`, `gee_biomass.py` e `feature_engineering_physics.py`.*
