# `src/modeling/results_consolidator.py`

## Função

Consolida métricas e metadados de múltiplos runs de treino (JSONs, pastas por modelo/cenário) em tabelas únicas para análise comparativa e para alimentar o visualizador.

## CLI

`python src/modeling/results_consolidator.py` ou via `run_results_consolidator.py`.

## Rótulos

Mapeia nomes de pasta (`base_A_no_rad_knn_calculated` etc.) para rótulos legíveis em português, incluindo flag de “variáveis derivadas”.
