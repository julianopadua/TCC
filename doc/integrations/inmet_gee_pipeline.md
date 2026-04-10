# Pipeline INMET-GEE — Documentação Técnica

**Módulo:** `src/integrations/inmet_gee/`
**Branch:** `feature/inmet-gee-station-pipeline`
**Versão do esquema de checkpoint:** 1
**Contexto:** TCC — Previsão de Risco de Incêndios no Cerrado

---

## Objetivo científico

Este módulo extrai e valida os metadados geográficos de todas as estações INMET presentes nas bases tabulares do projeto (variantes `*_calculated`), detecta **deriva espacial** (mudanças de coordenadas de uma mesma estação ao longo dos anos), valida cada ponto no **Google Earth Engine** (GEE) e exporta **séries temporais comparativas** entre a base E (dados com imputação KNN/radiação) e a base F (dados originais enriquecidos com features físicas).

Os artefatos produzidos alimentam etapas futuras de extração de NDVI/biomassa, interpolação espacial (IDW/Krigagem) e análise de distância geodésica até focos de calor (BDQueimadas).

---

## Pré-requisitos

### Python
Todos os pacotes já estão em `requirements.txt`. Dependência GEE:
```
earthengine-api
```

### Conta e credenciais do Google Earth Engine

1. Crie uma conta em [earthengine.google.com](https://earthengine.google.com) associada a um projeto Google Cloud habilitado para a API Earth Engine.
2. Autentique na máquina de trabalho:
   ```
   earthengine authenticate
   ```
   Isso grava as credenciais em `~/.config/earthengine/credentials`.
3. Exporte o ID do projeto como variável de ambiente (recomendado em vez de gravar no `config.yaml`):
   ```
   set GEE_PROJECT=meu-projeto-cloud
   ```
   Se preferir configuração estática, preencha `inmet_gee_pipeline.gee.project_id` no `config.yaml`.

### Conta de serviço (JSON do Google Cloud)

Recomendado para servidores e para evitar depender só do `earthengine authenticate`:

1. No Google Cloud Console, crie uma conta de serviço, baixe a chave JSON e coloque-a em `credentials/` (pasta ignorada pelo Git).
2. No `config.yaml`, defina `inmet_gee_pipeline.gee.service_account_key_path` com o caminho relativo à raiz do projeto (ex.: `./credentials/service_account_gee.json`) **ou** defina a variável de ambiente `GEE_SERVICE_ACCOUNT_JSON` com o caminho absoluto.
3. O campo `project_id` no YAML ou `GEE_PROJECT` deve ser o **mesmo projeto** onde a API Earth Engine está habilitada. Se `project_id` estiver vazio, o pipeline usa o `project_id` lido do próprio JSON.
4. No IAM do projeto, conceda à conta de serviço permissões adequadas (em erros **403**, o Console costuma pedir `roles/serviceusage.serviceUsageConsumer` ou equivalente). Registre a conta no Earth Engine conforme a política do seu laboratório/projeto.

O código usa `ee.ServiceAccountCredentials(key_file=...)` e `ee.Initialize(credentials=..., project=...)`.

### Sem credenciais GEE

O pipeline funciona sem GEE usando a flag `--skip-gee`. Nesse caso, apenas metadados de estações, deriva espacial e séries temporais são gerados.

---

## Execução

### Comando completo (metadados + séries + gráficos)
```
python -m src.integrations.inmet_gee.run_pipeline
```

### Apenas metadados + deriva + GEE (sem séries nem gráficos)

Útil quando Parquets e plots já foram gerados e você só quer validar estações no Earth Engine (ou refazer CSVs de metadados):

```
python -m src.integrations.inmet_gee.run_pipeline --only-metadata-gee
```

Equivalente a `--skip-timeseries --skip-plots` sem pular o GEE.

### Apenas séries temporais E vs F (sem GEE)
```
python -m src.integrations.inmet_gee.run_pipeline --skip-gee
```

### Apenas séries e gráficos (pular metadados de estações)
```
python -m src.integrations.inmet_gee.run_pipeline --only-timeseries
```

### Gerar gráficos a partir de Parquets já existentes
```
python -m src.integrations.inmet_gee.run_pipeline --only-plots
```

### Reprocessar um ano específico
```
python -m src.integrations.inmet_gee.run_pipeline --force-year 2019
```

### Reprocessar todos os anos com falha anterior
```
python -m src.integrations.inmet_gee.run_pipeline --retry-failed
```

### Limitar aos anos 2018–2020
```
python -m src.integrations.inmet_gee.run_pipeline --years 2018 2019 2020
```

---

## Estrutura de saída

```
data/integrations/inmet_gee/
├── checkpoints/
│   ├── run_state.json          # estado do pipeline de metadados
│   ├── run_state.bak           # backup automático (escrita atômica)
│   ├── timeseries_state.json   # estado do pipeline de séries
│   └── timeseries_state.bak
├── outputs/
│   ├── csv/
│   │   ├── station_year_locations.csv   # metadados por estação/ano
│   │   ├── spatial_drift_events.csv     # eventos de deriva espacial
│   │   └── gee_point_validation.csv     # resultados de validação GEE
│   ├── timeseries/
│   │   └── yearly/
│   │       ├── ts_compare_2003.parquet  # série comparativa E vs F
│   │       ├── ts_compare_2003_sample.csv
│   │       └── ...
│   └── plots/
│       ├── compare_precip_mm_brasilia_2003-2024.png
│       ├── compare_precip_cumsum_brasilia_2003-2024.png
│       ├── global_precip_mm_2003-2024_daily_mean.png
│       └── index.json                   # metadados de todas as figuras
└── cache/                               # reservado para fase 2 (NDVI)
```

---

## Schema dos CSVs

### `station_year_locations.csv`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `station_uid` | str | ID lógico da estação (padrão: `cidade_norm`; ver Limitações) |
| `ano` | int | Ano do registro |
| `lat_median` | float | Mediana das latitudes no ano |
| `lon_median` | float | Mediana das longitudes no ano |
| `n_obs` | int | Número de observações horárias no ano |
| `n_distinct_coord_pairs` | int | Pares lat/lon distintos além do jitter configurado |
| `ambiguous_intra_year_coords` | bool | True se houver mais de um par de coordenadas no ano |
| `cidade_norm` | str | Nome da cidade normalizado |
| `CIDADE` | str | Nome original da cidade |
| `geo_version` | int | Versão geográfica atual (1 = sem deriva desde o primeiro ano) |

**Chave primária lógica:** `(station_uid, ano)`

### `spatial_drift_events.csv`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `station_uid` | str | ID da estação |
| `year_from` | int | Último ano na versão geográfica anterior |
| `year_to` | int | Ano em que a deriva foi detectada |
| `lat_from` | float | Latitude anterior |
| `lon_from` | float | Longitude anterior |
| `lat_to` | float | Nova latitude |
| `lon_to` | float | Nova longitude |
| `distance_m` | float | Distância haversine em metros |
| `geo_version` | int | Nova versão geográfica após a deriva |

### `gee_point_validation.csv`

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `station_uid` | str | ID da estação |
| `year` | int | Ano da validação (pode ser None se per_geo_version) |
| `geo_version` | int | Versão geográfica validada |
| `lat` | float | Latitude usada |
| `lon` | float | Longitude usada |
| `status` | str | `OK` \| `FAILED` \| `SKIPPED` |
| `message` | str | Descrição do resultado |
| `bands` | str | Bandas disponíveis separadas por `\|` |
| `validated_at` | str | Timestamp ISO 8601 UTC |

### `ts_compare_{YYYY}.parquet`

Colunas principais: `ts_hour`, `cidade_norm`, `ano`, `{variável}__E`, `{variável}__F` para cada variável configurada. Colunas adicionais: `precip_cumsum_E`, `precip_cumsum_F` quando `timeseries.cumsum_precip=true`.

---

## Configuração relevante (`config.yaml`)

```yaml
inmet_gee_pipeline:
  station_source_scenario: "base_E_calculated"
  station_id_columns: ["cidade_norm"]
  coordinate_jitter_max_m: 50
  drift_alert_min_m: 500
  retry_max_attempts: 3
  retry_base_delay_s: 5
  gee:
    project_id: ""           # ou GEE_PROJECT; vazio com SA usa project_id do JSON
    service_account_key_path: "./credentials/service_account_gee.json"
    roi_mode: "point"        # fase 1
    buffer_radius_km: 5.0    # fase 2
    validation_strategy: "per_geo_version"
  timeseries:
    enabled: true
    scenarios:
      E: "base_E_calculated"
      F: "base_F_calculated"
    auto_sample_n: 5
    cumsum_precip: true
    downsample_for_global_plot: "daily_mean"
    export_format: "parquet"
  station_influence_radius_km: 50.0   # fase 3: análise de focos
```

---

## Sistema de checkpoints

O pipeline mantém dois arquivos de estado em `checkpoints/`:

- `run_state.json` — controla o loop de metadados (anos concluídos, falhas, estado de deriva, chaves GEE).
- `timeseries_state.json` — controla o loop de séries temporais (anos de parquet já exportados).

Em caso de falha (timeout, queda de rede, interrupção manual), basta reexecutar o mesmo comando. O pipeline retomará a partir do último ano não concluído sem reprocessar o passado.

Os arquivos são escritos **atomicamente** (`write → .tmp → os.replace`) e têm backup automático (`.bak`) antes de cada atualização, tornando o estado recuperável mesmo em falhas de I/O.

---

## Limitações conhecidas

1. **Homônimos de cidade:** o `station_uid` padrão é `cidade_norm`. Duas estações em cidades com o mesmo nome normalizado serão tratadas como uma única. Mitigação futura: configurar `station_id_columns: ["CD_ESTACAO"]` quando o código INMET estiver disponível nos CSVs brutos, ou adicionar coordenadas ao UID via `enrich_uid_with_coords()` em `stations.py`.

2. **Séries temporais — cobertura E∩F:** o join entre E e F é `inner` em `(cidade_norm, ts_hour)`. Anos ou cidades com cobertura assimétrica entre as bases geram menos linhas na série comparativa. O log registra contagens de linhas descartadas com WARNING.

3. **GEE — cota:** com `validation_strategy: "per_geo_version"`, cada versão geográfica única de uma estação é validada apenas uma vez, reduzindo o número de requisições. O backoff exponencial (com jitter) é aplicado automaticamente em erros de quota.

---

## Próximos passos (fase 2 e 3)

| Etapa | Descrição | Hook disponível |
|-------|-----------|-----------------|
| Fase 2 | Extração de NDVI/LAI com buffer ao redor da estação | `GeeSampler.extract_ndvi_timeseries()` |
| Fase 2 | Parâmetro `buffer_radius_km` na config | `gee.buffer_radius_km` |
| Fase 3 | Distância geodésica entre estações e focos de calor | `station_influence_radius_km` |
| Fase 3 | IDW/Krigagem com `ROI_SIZE` parametrizado | `spatial_interpolation.roi_size_km` |
