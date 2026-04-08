# `src/audit_consolidated_sources.py`

## Função

Auditoria das **fontes consolidadas** (INMET e BDQueimadas em `data/consolidated/`): cobertura temporal, contagem de arquivos, consistência de nomes e checagens de qualidade usadas para validar o pipeline antes do `build_dataset`.

## CLI

`python src/audit_consolidated_sources.py` — ver `main()` para subcomandos e flags.

## Saídas

Relatórios/logs e, conforme implementação, CSVs ou markdown em `data/eda/` ou `doc/`-adjacente.
