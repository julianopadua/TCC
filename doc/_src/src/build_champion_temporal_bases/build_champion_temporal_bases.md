# `src/build_champion_temporal_bases.py`

## Função

Constrói duas bases "campeãs" (`tf_D_champion` e `tf_F_champion`) mesclando as colunas `tsf_*`
dos métodos de fusão temporal melhor ranqueados segundo `method_ranking_train.csv`
(Camada A — apenas anos de treino, sem vazamento temporal).

## Pré-requisito

Executar `feature_engineering_temporal.py --output-layout split` primeiro, de forma que existam
as pastas `data/temporal_fusion/{base_D,base_F}/{ewma_lags,arima,...}/` e o ranking
em `data/eda/temporal_fusion/method_ranking_train.csv`.

## Saída

| Pasta | Cenário no config |
|-------|------------------|
| `data/temporal_fusion/base_D_with_rad_drop_rows_calculated_champion_tsfusion/` | `tf_D_champion` |
| `data/temporal_fusion/base_F_full_original_calculated_champion_tsfusion/` | `tf_F_champion` |

Cada arquivo `inmet_bdq_{year}_cerrado.parquet` contém todas as colunas originais da base
`*_calculated` mais as colunas `tsf_*` dos métodos selecionados mescladas por `(cidade_norm, ts_hour)`.

## CLI

```
python src/build_champion_temporal_bases.py [--top-k N] [--methods m1 m2] [--bases D F] [--overwrite]
```

| Flag | Descrição | Padrão |
|------|-----------|--------|
| `--top-k N` | Selecionar os N melhores do ranking (MAE crescente) | todos |
| `--methods m1 m2` | Lista explícita de métodos (ignora ranking) | — |
| `--bases D F` | Bases a processar | D F |
| `--overwrite` | Sobrescrever parquets existentes | desligado |

**Exemplo — top-2 do ranking:**
```
python src/build_champion_temporal_bases.py --top-k 2
```

**Exemplo — métodos explícitos:**
```
python src/build_champion_temporal_bases.py --methods arima prophet --bases D F
```

## Logging

- Console + arquivo: `logs/log_YYYYMMDD/champion_HHMMSS.log`.
- Memória (RSS + disponível) logada antes e depois de cada ano processado.
- Colunas com nome conflitante entre métodos são ignoradas e reportadas como `[WARN]`.
