# Comparative Analysis of ML Models for Predicting Innovation Outcomes (Applied Sciences, 2025)

## Ficha técnica

Autores: Marko Martinović; Kristian Dokić; Dalibor Pudić
Tipo: Estudo aplicado com **comparação sistemática** de classificadores
Escopo: dados CIS2014 (Croácia), modelos avaliados (**LR, RF, SVM, ANN/MLP, XGBoost, LightGBM, CatBoost**); otimização de hiperparâmetros com **Bayesian optimization (Hyperopt/TPE)**; avaliação com **k-fold CV (10×), 5×2-CV**, repetições, e **testes estatísticos corrigidos** (t-tests corrigidos, Wilcoxon, McNemar, Friedman). Principais achados: **ensembles baseados em árvores (boosting)** dominam em acurácia/precision/F1/ROC-AUC; **SVM se destaca em recall**; **LR é o mais eficiente computacionalmente**. 

---

## Trechos centrais (curtos, literais)

> “Tree-based boosting algorithms consistently outperformed other models in accuracy, precision, F1-score, and ROC-AUC, while the kernel-based approach excelled in recall.” 

> “The choice of an appropriate cross-validation protocol and accounting for overlapping data splits are crucial to reduce bias and ensure reliable comparisons.” 

> “Logistic regression proved to be the most computationally efficient model despite its weaker predictive power.” 

---

## Por que importa para a sua seção “Comparação de Modelos”

1. **Racional do desenho comparativo**
   O paper mostra que conclusões mudam com o **protocolo de validação**; usar **10-fold (repetido)** e/ou **5×2-CV** com **testes corrigidos** evita falsas vitórias entre modelos. Isso fundamenta você reportar **médias + variância** e aplicar **t-tests corrigidos / Wilcoxon / Friedman** nas suas bases (com e sem imputação). 

2. **Ensembles como referência forte**
   Para dados tabulares, **XGBoost/LightGBM/CatBoost** tendem a liderar em **F1/ROC-AUC**; use-os como **linhas de base de alta performance** na sua comparação. 

3. **Métrica-alvo e perfil do modelo**
   O estudo evidencia **trade-offs por métrica**: **SVM** pode ganhar em **recall** (sensibilidade), enquanto **LR** entrega **baixo custo**. Isso justifica você **apresentar ranking por métrica** (F1, ROC-AUC; opcionalmente Accuracy/Precision/Recall) e discutir **custos de treino/inferência**. 

4. **Hiperparâmetros com Bayesian/TPE**
   Adoção de **Hyperopt/TPE** para buscar regiões promissoras dá eficiência sem CV aninhada exaustiva; reproduzível com **seeds** e **espaços condicionais** por modelo. Base para padronizar seu **tuning**. 

---

## Tabela síntese (conceito → uso imediato no TCC)

| Conceito do artigo                  | Risco se ignorado                        | Ação concreta na sua comparação                                                                    |
| ----------------------------------- | ---------------------------------------- | -------------------------------------------------------------------------------------------------- |
| **CV e testes corrigidos importam** | “Vitórias” espúrias entre modelos        | Usar **10-fold repetido (≥10x)** + **5×2-CV** e **t-tests corrigidos**, **Wilcoxon**, **Friedman** |
| **Boosting domina em F1/ROC-AUC**   | Subestimar o estado-da-arte tabular      | Incluir **XGB/LGBM/CB** como **baselines principais**                                              |
| **SVM com melhor recall**           | Perder sensibilidade a eventos positivos | Reportar **recall** e **threshold tuning** quando recall é crítico                                 |
| **LR eficiente**                    | Comparação só por acurácia               | Reportar **tempo de treino/inferência** e **custo computacional**                                  |
| **Tuning bayesiano**                | Grid lento/ineficiente                   | **Hyperopt/TPE** com espaços condicionais e seed fixo                                              |



---

## Onde citar no TCC

Seção **Metodologia — Comparação de Modelos** (subseções: protocolo de validação, testes estatísticos, tuning e métricas) e **Resultados — Discussão por métrica e custo**. Use como referência para justificar **boosting como baseline forte**, **SVM para recall**, **LR para eficiência**, e o **arranjo de CV/testes corrigidos** no seu desenho experimental. 
