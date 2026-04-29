# Model evaluation report

- Generated at `2026-04-28T22:38:19`
- Runs: `3`
- Note: `model_performance_comparison.png` is temporal holdout comparison; it is not 5-fold CV uncertainty.

## Metrics

| source | model | variation | scenario | ts | model | PR AUC(eval) | ROC AUC(eval) | PR AUC(saved) | ROC AUC(saved) |
|---|---|---|---|---|---|---:|---:|---:|---:|
| article | XGBoost | base | tf_F_champion | 20260425_040414 | no |  |  | 0.324215 | 0.952359 |
| article | XGBoost | gridsearch_smote | tf_F_champion | 20260425_042235 | no |  |  | 0.261401 | 0.945338 |
| article | XGBoost | gridsearch_weight | tf_F_champion | 20260425_043707 | no |  |  | 0.208726 | 0.947465 |


## CSV artifacts

- `metrics_comparison`: `metrics_comparison.csv`
