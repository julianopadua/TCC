# Auditoria de dados faltantes - 2003

Este diretorio contem a auditoria de valores faltantes nas colunas de feature do arquivo consolidado `inmet_bdq_2003_cerrado.csv`.

## Resumo geral

- Linhas totais: 198028
- Linhas com foco (HAS_FOCO == 1): 1054
- Proporcao de focos: 0.0053

## Arquivo de resultados

O arquivo `missing_by_column.csv` traz, para cada coluna de feature, a contagem e a proporcao de valores faltantes.

Colunas do CSV:

- `year`: ano de referencia dos dados.
- `col`: nome da coluna de feature na base original.
- `rows_total`: numero total de linhas no arquivo do ano.
- `focos_total`: numero total de linhas com HAS_FOCO == 1.
- `missing_total`: numero de linhas em que nao ha valor valido para essa coluna.
- `missing_focus`: numero de linhas com foco (HAS_FOCO == 1) em que nao ha valor valido para essa coluna.
- `missing_nonfocus`: numero de linhas sem foco em que nao ha valor valido para essa coluna.
- `pct_missing_total`: proporcao de linhas com valor faltante na coluna.
- `pct_missing_focus`: proporcao de linhas com foco que estao com valor faltante na coluna.
- `pct_missing_nonfocus`: proporcao de linhas sem foco que estao com valor faltante na coluna.

## Top colunas com mais faltantes

As 5 colunas com maior `pct_missing_total` neste ano sao:

| col | missing_total | pct_missing_total |
| --- | ------------- | ----------------- |
| RADIACAO GLOBAL (KJ/m²) | 116286 | 0.5872 |
| VENTO, RAJADA MAXIMA (m/s) | 13122 | 0.0663 |
| VENTO, DIREÇÃO HORARIA (gr) (° (gr)) | 12962 | 0.0655 |
| VENTO, VELOCIDADE HORARIA (m/s) | 12958 | 0.0654 |
| PRECIPITAÇÃO TOTAL, HORÁRIO (mm) | 1746 | 0.0088 |