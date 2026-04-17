# Próximos passos (fusão temporal legado + artigo)

## Legado `data/temporal_fusion/` (D/E/F calculated)

Fusão temporal via `feature_engineering_temporal.py` suporta apenas **`ewma_lags`** e **`sarimax_exog`**.

```bash
python src/feature_engineering_temporal.py --years 2020 --methods ewma_lags --scenarios base_D_calculated
python src/feature_engineering_temporal.py --output-layout split --methods ewma_lags sarimax_exog
python src/feature_engineering_temporal.py --methods sarimax_exog --arimax-endog temp --scenarios base_D_calculated --years 2018 2019
```

Camada A (métricas) e `method_ranking_train.csv`: gerados em `data/eda/temporal_fusion/` ao executar o script acima.

## Pipeline do artigo (`src/article/`)

Orquestrador unificado (fusão em `data/_article/`, Camada A, champion):

```bash
python src/article/article_orchestrator.py
```

Ranking e features selecionadas: `data/eda/temporal_fusion/method_ranking_article.csv`, `selected_features_article.json`.
