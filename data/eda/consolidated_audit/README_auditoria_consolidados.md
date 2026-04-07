# Auditoria dos arquivos consolidados

Este diretório reúne a auditoria dos três arquivos consolidados: BDQueimadas, INMET e base integrada INMET + BDQueimadas.

## Visão geral

| Fonte | Linhas | Colunas | Tamanho | Linhas com missing | % linhas com missing | Classe positiva | % classe positiva | Markdown | CSV |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| BDQueimadas consolidado | 1.349.101 | 8 | 135.53 MB | 667.218 | 49.4565% | N/A | N/A | bdqueimadas_consolidado_audit.md | bdqueimadas_consolidado_column_audit.csv |
| INMET consolidado | 49.922.606 | 15 | 4.96 GB | 27.418.972 | 54.9230% | N/A | N/A | inmet_consolidado_audit.md | inmet_consolidado_column_audit.csv |
| Base integrada INMET + BDQueimadas | 45.135.924 | 23 | 6.18 GB | 45.135.924 | 100.0000% | 151.544 | 0.3358% | dataset_integrado_audit.md | dataset_integrado_column_audit.csv |

Observação: a proporção da classe positiva só é calculada quando a base contém explicitamente uma coluna alvo binária, como `HAS_FOCO`.