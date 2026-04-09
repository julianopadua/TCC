# `src/feature_engineering_temporal.py`

## Função

**Fusão temporal** para o artigo: a partir de parquets em pastas `*_calculated` (padrão D e F),
gera colunas `tsf_*` com famílias `ewma_lags`, `arima`, `sarima`, `prophet`, `minirocket`, `tskmeans`.

### Layouts de saída

| Layout | Flag | Destino |
|--------|------|---------|
| `split` (padrão) | `--output-layout split` | Uma pasta por método: `data/temporal_fusion/{base_folder}/{método}/inmet_bdq_{year}_cerrado.parquet`. Cenários `tf_D_*` / `tf_F_*` em `config.yaml`. |
| `merged` (legado) | `--output-layout merged` | Todos os métodos num único parquet: `data/modeling/{base_folder}_tsfusion/`. Cenários `base_D_calculated_tsfusion` em `config.yaml`. |

### Métricas de Camada A (Layer A)

Registradas via `LayerATracker` e salvas em `data/eda/temporal_fusion/`:

| Arquivo | Conteúdo |
|---------|----------|
| `layer_a_summary.csv` | MAE/MSE/R² por método × ano (todos os anos) |
| `layer_a_detail.csv` | Registro por cidade × método × ano |
| `layer_a_summary_train.csv` | Idem, apenas anos de **treino** (`is_train=True`) |
| `method_ranking_train.csv` | Uma linha por método, ordenada por MAE crescente (treino) — entrada para `build_champion_temporal_bases.py` |

O ranking usa apenas anos de treino para evitar vazamento de informação temporal na seleção de métodos.

## CLI principal

```
python src/feature_engineering_temporal.py \
  [--output-layout split|merged] \
  [--methods ewma_lags arima sarima prophet minirocket tskmeans] \
  [--scenarios base_D_calculated base_F_calculated] \
  [--years 2020 2021] \
  [--refit-hours 168] \
  [--window-hours 720] \
  [--sarima-m 24] \
  [--arima-order 2 1 2] \
  [--minirocket-window 24] \
  [--minirocket-kernels 84] \
  [--tskmeans-k 8] \
  [--tskmeans-period 168] \
  [--test-years 2] \
  [--overwrite]
```

**Smoke test recomendado (rápido):**
```
python src/feature_engineering_temporal.py --years 2020 --methods ewma_lags
```

## Configuração

- `config.yaml / paths.data.temporal_fusion` → raiz das pastas por método.
- `config.yaml / temporal_fusion_paths` → mapeamento `scenario_folder → subcaminho` usado por `resolve_parquet_dir` em `utils.py`.
- Cenários `tf_D_*` / `tf_F_*` em `config.yaml / modeling_scenarios` → aparecem automaticamente no menu do `train_runner`.

## Dependências opcionais

Sem o pacote instalado, o método correspondente é **ignorado com aviso** no log:

| Pacote | Métodos |
|--------|---------|
| `statsmodels` | `arima`, `sarima` |
| `prophet` | `prophet` |
| `aeon` | `minirocket` |
| `tslearn` | `tskmeans` |

## Logging

- Console + arquivo: `logs/log_YYYYMMDD/temporal_HHMMSS.log` (via `per_run_file=True`).
- Uso de memória (RSS + disponível + % usado) logado no início/fim de cada método pesado e no início/fim do `run()`.
- Erros por cidade/bloco logados em nível `DEBUG` (primeiros 5 por método/ano para não poluir).

## Referência no código

Balduino & Valente, "Implementation of IoT Data Fusion Architectures for Precipitation Forecasting", Preprints 2025.
