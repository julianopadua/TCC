# Supervised Machine Learning — A Review of Classification (Kotsiantis, Informatica 2007)

## Ficha técnica

Autores: S. B. Kotsiantis
Tipo: Revisão de técnicas de classificação supervisionada
Escopo: pipeline de ML (pré-processamento, seleção de atributos, validação), árvores de decisão, regras, perceptrons/ANNs/RBF, k-NN, Naive Bayes e Redes Bayesianas, SVM, comparação e seleção de classificadores. 

---

## Trechos centrais (literais, curtos)

> “Supervised machine learning is the search for algorithms that reason from externally supplied instances to produce general hypotheses… The goal… is to build a concise model… then used to assign class labels to testing instances.” 

> “The process of applying supervised ML to a real-world problem is described in Figure 1 [data preprocessing → algorithm selection → training → evaluation → tuning].” 

> “Feature subset selection… removing irrelevant and redundant features… reduces dimensionality and enables algorithms to operate faster and more effectively.” 

> “A common method for comparing supervised ML algorithms is to perform statistical comparisons… cross-validation… paired t-tests… with caveats about Type I/II errors and partition sensitivity.” 

> “Decision trees (e.g., C4.5) are comprehensible, fast, and support pruning to avoid overfitting.” 

> “Perceptron-based and multilayer networks learn non-linear boundaries but may be slower and less interpretable.” 

> “Instance-based learning (k-NN) is lazy, sensitive to distance choice and k, with higher classification-time cost.” 

> “Naive Bayes/BNs provide probabilistic models; NB often competitive despite independence assumptions.” 

> “SVMs maximize margin; kernels enable non-linear separation; training can be slow but generalization is strong.” 

---

## Por que importa para o seu TCC (usar na seção “Machine Learning”)

1. Estrutura do pipeline
   Base para descrever seu fluxo: **pré-processamento (missing/outliers, seleção/construção de atributos)** → **escolha de algoritmo** → **validação adequada** → **comparação estatística**. Justifica falar de **auditorias de missing** e **seleção de features climáticas/espaciais** antes de modelar. 

2. Validação e comparação
   Endossa **k-fold / repetida** e alerta para **erros Tipo I/II** e sensibilidade à partição—útil ao reportar testes entre RF, XGB, SVM, etc., e ao discutir significância (p-valores/intervalos) das diferenças de desempenho. 

3. Escolha de modelos (guideline rápido)

* **Árvores/Boosting**: interpretáveis (importâncias), bons com mistas/não-linearidades; prever risco de overfitting e usar **poda/regularização**. 
* **SVM**: margem máxima, robusto em alta dimensionalidade; custo de treino maior e **seleção de kernel** via CV. 
* **k-NN**: baseline simples; requer metrificação/escala, custa em predição; bom para checagem rápida. 
* **Naive Bayes/BN**: baseline probabilístico rápido; NB competitivo mesmo com dependências; BN exige discretização/estrutura. 
* **ANN/RBF**: capturam não-linearidades complexas, porém **menos interpretáveis** e mais lentos para treinar. 

4. Link com suas decisões

* **Pré-processamento forte** (tratamento de faltantes e sentinelas, seleção/engenharia de variáveis climáticas e espaciais) melhora eficiência e acurácia. 
* **Comparação multimodelo** é esperada em uma revisão como esta; reporte **métricas sensíveis a desbalanceamento** (ex. PR-AUC) e inclua avaliação estatística da diferença entre modelos. 

---

## Tabela síntese (conceito → uso imediato no TCC)

| Conceito da revisão                                   | Risco se ignorado       | Ação concreta                                                             |
| ----------------------------------------------------- | ----------------------- | ------------------------------------------------------------------------- |
| Pipeline com pré-processamento e seleção de atributos | Modelo lento/ruidoso    | Padronizar tratamento de missing/outliers; seleção/construção de features |
| Validação e testes estatísticos                       | Comparações enganosas   | k-fold estratificado/repetido; relatar variância; teste pareado           |
| Árvores/Regras                                        | Overfitting             | Poda/regularização; interpretar importâncias                              |
| SVM com kernel                                        | Escolha ruim de kernel  | Grid/CV para C, γ, kernel; normalização                                   |
| k-NN                                                  | Alto custo em predição  | Usar como baseline; escalar dados; escolher k via CV                      |
| Naive Bayes/BN                                        | Suposições não checadas | Discretização/checar dependências; usar como baseline rápido              |

---

## Onde citar no TCC

Seção **Fundamentação — Machine Learning (Classificação Supervisionada)** e **Metodologia — Pipeline e Comparação de Modelos**: usar como referência-guia para design do pipeline, escolha de algoritmos e protocolo de comparação/validação. 
