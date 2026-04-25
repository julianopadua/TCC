# Audits do pipeline de dados

Este diretorio centraliza as auditorias automatizadas do pipeline.
Cada execucao produz uma pasta `{TIMESTAMP}_{stage}` com tres arquivos.

## Estrutura

```
data/_article/_audits/
├── README.md                       (este arquivo)
├── LATEST.md                       indice do relatorio mais recente por estagio
└── YYYYMMDD_HHMMSS_{stage}/        um diretorio por execucao
    ├── summary.md                  humano-legivel, tabela por arquivo + status
    ├── per_file.csv                tabela long (uma linha por parquet auditado)
    └── raw.json                    dump completo estruturado (JSON)
```

## Estagios

| Stage         | Audita                                                          |
| ------------- | --------------------------------------------------------------- |
| `modeling`    | `data/modeling/base_*/` (fontes pre-features)                   |
| `calculated`  | `data/modeling/base_*_calculated/` (com physics features)       |
| `coords`      | `data/_article/0_datasets_with_coords/` (pos GEE + coords)      |
| `fusion`      | `data/_article/1_datasets_with_fusion/{base}/{method}/`         |

## Status

Cada arquivo auditado recebe um `status`:

| Status        | Quando                                                          |
| ------------- | --------------------------------------------------------------- |
| `OK`          | `dup_ratio < 1.01x` (sem duplicacao)                            |
| `SOFT_DUP`    | `1.01x <= dup_ratio < 1.10x` (multi-foco legitimo, aceitavel)   |
| `DUPLICATED`  | `dup_ratio >= 1.10x` (bug: rodar `make dedupe` + regenerar)     |
| `NO_KEYS`     | Parquet sem colunas `(cidade_norm, ts_hour)`                    |
| `EMPTY`       | 0 linhas                                                         |
| `ERROR`       | Falha na leitura                                                 |

## Uso

```bash
# Audita todos os estagios
make audit-pipeline

# Audita apenas um
make audit-stage STAGE=coords
make audit-stage STAGE=fusion

# Abre o indice do ultimo audit
make audit-pipeline-latest
```

## Validacao por run de treino

Cada run do `train_runner` tambem gera um `data_validation_{ts}.md`
na pasta do run (junto com `metrics_*.json`), contendo:
- Status OK/FAIL geral
- Audit per-file dos parquets que entraram no treino
- Stats do split carregado (train/test rows, pos_rate, anos)
- Acao recomendada se houver anomalia
