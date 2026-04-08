# Espelho de documentação (`doc/_src`)

Esta árvore **espelha a pasta `src/`**: cada módulo Python relevante possui um `.md` com visão técnica (responsabilidade, entradas/saídas, CLI, ligações com o restante do projeto).

## Índice por arquivo

| Código | Documentação |
|--------|----------------|
| `src/utils.py` | [src/utils/utils.md](./src/utils/utils.md) |
| `src/bdqueimadas_scraper.py` | [src/bdqueimadas_scraper/bdqueimadas_scraper.md](./src/bdqueimadas_scraper/bdqueimadas_scraper.md) |
| `src/bdqueimadas_consolidated.py` | [src/bdqueimadas_consolidated/bdqueimadas_consolidated.md](./src/bdqueimadas_consolidated/bdqueimadas_consolidated.md) |
| `src/bdq_build_biome_dict.py` | [src/bdq_build_biome_dict/bdq_build_biome_dict.md](./src/bdq_build_biome_dict/bdq_build_biome_dict.md) |
| `src/inmet_scraper.py` | [src/inmet_scraper/inmet_scraper.md](./src/inmet_scraper/inmet_scraper.md) |
| `src/inmet_consolidated.py` | [src/inmet_consolidated/inmet_consolidated.md](./src/inmet_consolidated/inmet_consolidated.md) |
| `src/build_dataset.py` | [src/build_dataset/build_dataset.md](./src/build_dataset/build_dataset.md) |
| `src/dataset_missing_audit.py` | [src/dataset_missing_audit/dataset_missing_audit.md](./src/dataset_missing_audit/dataset_missing_audit.md) |
| `src/modeling_build_datasets.py` | [src/modeling_build_datasets/modeling_build_datasets.md](./src/modeling_build_datasets/modeling_build_datasets.md) |
| `src/feature_engineering_physics.py` | [src/feature_engineering_physics/feature_engineering_physics.md](./src/feature_engineering_physics/feature_engineering_physics.md) |
| `src/feature_engineering_temporal.py` | [src/feature_engineering_temporal/feature_engineering_temporal.md](./src/feature_engineering_temporal/feature_engineering_temporal.md) |
| `src/train_runner.py` | [src/train_runner/train_runner.md](./src/train_runner/train_runner.md) |
| `src/audit_city_coverage.py` | [src/audit_city_coverage/audit_city_coverage.md](./src/audit_city_coverage/audit_city_coverage.md) |
| `src/audit_consolidated_sources.py` | [src/audit_consolidated_sources/audit_consolidated_sources.md](./src/audit_consolidated_sources/audit_consolidated_sources.md) |
| `src/audit_databases.py` | [src/audit_databases/audit_databases.md](./src/audit_databases/audit_databases.md) |
| `src/explore_risco_fogo.py` | [src/explore_risco_fogo/explore_risco_fogo.md](./src/explore_risco_fogo/explore_risco_fogo.md) |
| `src/merge_risco_validation.py` | [src/merge_risco_validation/merge_risco_validation.md](./src/merge_risco_validation/merge_risco_validation.md) |
| `src/plot_confusion.py` | [src/plot_confusion/plot_confusion.md](./src/plot_confusion/plot_confusion.md) |
| `src/modeling/results_consolidator.py` | [src/modeling/results_consolidator/results_consolidator.md](./src/modeling/results_consolidator/results_consolidator.md) |
| `src/modeling/results_visualizer.py` | [src/modeling/results_visualizer/results_visualizer.md](./src/modeling/results_visualizer/results_visualizer.md) |
| `src/run_results_consolidator.py` | [src/run_results_consolidator/run_results_consolidator.md](./src/run_results_consolidator/run_results_consolidator.md) |
| `src/run_results_visualization.py` | [src/run_results_visualization/run_results_visualization.md](./src/run_results_visualization/run_results_visualization.md) |
| `src/ml/core.py` | [src/ml/core/core.md](./src/ml/core/core.md) |
| `src/models/dummy.py` | [src/models/dummy/dummy.md](./src/models/dummy/dummy.md) |
| `src/models/logistic.py` | [src/models/logistic/logistic.md](./src/models/logistic/logistic.md) |
| `src/models/xgboost_model.py` | [src/models/xgboost_model/xgboost_model.md](./src/models/xgboost_model/xgboost_model.md) |
| `src/models/naive_bayes.py` | [src/models/naive_bayes/naive_bayes.md](./src/models/naive_bayes/naive_bayes.md) |
| `src/models/svm_linear.py` | [src/models/svm_linear/svm_linear.md](./src/models/svm_linear/svm_linear.md) |
| `src/models/random_forest.py` | [src/models/random_forest/random_forest.md](./src/models/random_forest/random_forest.md) |

## Manutenção

Ao criar um script novo em `src/`, adicione o par correspondente em `doc/_src/` e atualize este índice e a seção de documentação do `README.md` na raiz.
