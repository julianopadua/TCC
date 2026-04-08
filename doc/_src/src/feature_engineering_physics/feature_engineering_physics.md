# `src/feature_engineering_physics.py`

## Função

Engenharia de features **inspirada em documentação INPE** (risco de fogo): usa a base **E** (KNN, sem buracos nas features) como referência temporal por cidade, mantém **estado de memória entre anos** (ex.: dias sem chuva não zeram em 1º de janeiro) e propaga colunas derivadas para cenários A–F.

## Saídas

Novos diretórios `data/modeling/<cenário>_calculated/` com parquets anuais enriquecidos (ex.: `precip_ewma`, `dias_sem_chuva`, indicadores de risco).

## Posição no pipeline

Entre `modeling_build_datasets.py` e `feature_engineering_temporal.py` (que consome variantes `*_calculated`).
