# Cenário `base_E_with_rad_knn_calculated`

Subpastas típicas: `ewma_lags/`, `minirocket/`, `sarimax_exog/`, `champion/` (parquets `inmet_bdq_*_cerrado.parquet`).

## Auditoria de consistência

O ficheiro **`audit.md`** (na mesma pasta) descreve, por método e por ano:

- alinhamento de **colunas** entre ficheiros;
- contagem de **`tsf_*`**;
- comparação de **`num_rows`** com `0_datasets_with_coords/base_E_with_rad_knn_calculated/`;
- avisos de tipo (ex.: float vs double entre anos).

Regenerar após alterar parquets:

```bash
python -m src.article.audit_fusion_dataset --scenario base_E_with_rad_knn_calculated
```

Outros cenários: substitua o nome da pasta do cenário em `--scenario`.
