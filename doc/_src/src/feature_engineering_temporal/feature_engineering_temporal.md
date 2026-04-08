# `src/feature_engineering_temporal.py`

## Função

**Fusão temporal** para o artigo: a partir de parquets em pastas `*_calculated` (padrão D e F), gera colunas `tsf_*` com famílias `ewma_lags`, `arima`, `sarima`, `prophet`, `minirocket`, `tskmeans`. Escreve pastas com sufixo `_tsfusion` e métricas de **Camada A** (MAE/MSE/R² da série contínua, precipitação como série principal).

## CLI principal

`python src/feature_engineering_temporal.py [--methods ...] [--scenarios base_D_calculated base_F_calculated] [--years ...] [--refit-hours 168] [--window-hours 720] [--test-years 2] [--overwrite]`

## Configuração

Chaves `base_*_calculated_tsfusion` em `config.yaml` apontam para as pastas de saída usadas pelo `train_runner`.

## Referência no código

Balduino & Valente (Preprints 2025), citada no cabeçalho do módulo.
