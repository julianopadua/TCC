# `src/bdqueimadas_scraper.py`

## Função

Baixa da página COIDS (`Brasil_sat_ref`) os arquivos anuais `focos_br_ref_YYYY.zip`, grava em `data/raw/ID_BDQUEIMADAS` (ou pasta configurada) e extrai para `data/processed/...`.

## CLI típica

`python src/bdqueimadas_scraper.py --years 2019 2020` (opções: `--folder`, `--overwrite`, `--no-extract`).

## Dependências

`utils` (`get_requests_session`, `get_logger`, `get_path`, etc.), `requests`, parsing HTML da página de links.

## Saídas

Zips brutos + CSV `focos_br_ref_YYYY.csv` por ano após extração; usados por `bdqueimadas_consolidated.py`.
