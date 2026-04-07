# Auditoria de dados faltantes - 2008

Este diretorio contem a auditoria de valores faltantes nas colunas de feature do arquivo consolidado `inmet_bdq_2008_cerrado.csv`.

## Resumo geral

- Linhas totais: 2000366
- Linhas com foco (HAS_FOCO == 1): 7222
- Proporcao de focos: 0.0036

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
| RADIACAO GLOBAL (KJ/m²) | 848332 | 0.4241 |
| VENTO, DIREÇÃO HORARIA (gr) (° (gr)) | 31478 | 0.0157 |
| PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB) | 14552 | 0.0073 |
| VENTO, RAJADA MAXIMA (m/s) | 11904 | 0.0060 |
| VENTO, VELOCIDADE HORARIA (m/s) | 10660 | 0.0053 |