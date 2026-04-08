# `src/train_runner.py`

## Função

Orquestrador **interativo** de experimentos: escolhe uma ou mais bases (`modeling_scenarios` do `config.yaml`), um ou mais modelos (`dummy`, `logistic`, `xgboost`, opcionalmente `naive_bayes`, `svm`, `random_forest`) e combinações de variação (base vs GridSearch, SMOTE, peso de classe).

## Dados

Carrega parquets ano a ano da pasta do cenário; aplica `TemporalSplitter` (`ml/core.py`), downsample de negativos com preservação de positivos, e treina conforme o plano montado no menu.

## Features especiais

Se o nome da pasta do cenário contém `tsfusion`, estende automaticamente a lista de features com todas as colunas `tsf_*` detectadas no primeiro parquet.

## Execução

`python src/train_runner.py` (menus no terminal).

## Artefatos

Métricas JSON e modelos `.joblib` por run (pastas sob o diretório de resultados configurado no fluxo).
