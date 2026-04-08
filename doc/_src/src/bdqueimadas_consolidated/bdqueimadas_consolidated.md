# `src/bdqueimadas_consolidated.py`

## Função

Consolida exportações **manuais** do portal BDQueimadas (`exportador_*_ref_YYYY.csv` em `data/raw/BDQUEIMADAS`) com a camada **processada** `focos_br_ref_YYYY.csv` (pós-scraper), via chave sintética hora cheia + país/UF/município normalizados.

## Saídas

CSV em `data/consolidated/BDQUEIMADAS/` (caminho vem de `paths.data.external` no `config.yaml`): `bdq_targets_{ano}_{bioma}.csv` ou intervalos / `all_years`.

## CLI

`--years`, `--biome`, `--overwrite`, `--validation`, `--output-filename`, `--encoding`.

## Relação com o README antigo

Substitui a menção histórica a `consolidated_bdqueimadas.py` (nome antigo).
