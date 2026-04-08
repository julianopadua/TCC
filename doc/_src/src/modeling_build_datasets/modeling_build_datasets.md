# `src/modeling_build_datasets.py`

## Função

Lê `data/dataset/inmet_bdq_*_cerrado.csv`, harmoniza radiação global, trata sentinelas `-999`/`-9999` como NaN, coage features numéricas e grava **seis cenários** A–F em `data/modeling/<pasta>/inmet_bdq_{ANO}_cerrado.parquet` (um parquet por ano).

## Cenários

- **F:** original com radiação, sem imputação, sem drop de linhas.  
- **A:** sem radiação.  
- **B:** sem radiação + KNN nas features.  
- **C:** sem radiação + drop de linhas incompletas.  
- **D:** com radiação + drop de linhas incompletas.  
- **E:** com radiação + KNN.

## CLI

`python src/modeling_build_datasets.py [--years ...] [--overwrite-existing] [--n-neighbors N] ...`

## Próxima etapa no pipeline

`feature_engineering_physics.py` lê esses parquets e gera pastas `*_calculated`; em seguida `feature_engineering_temporal.py` pode gerar `*_tsfusion`.
