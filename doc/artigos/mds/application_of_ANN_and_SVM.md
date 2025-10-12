# Li20-Guangxi — Application of the Artificial Neural Network and Support Vector Machines in Forest Fire Prediction in the Guangxi Autonomous Region, China (2020)

Autores: Yudong Li; Zhongke Feng; Shilin Chen; Ziyu Zhao; Fengge Wang. Local/Periódico/Conferência: Discrete Dynamics in Nature and Society. Link/DOI: 10.1155/2020/5612650.

## Ficha técnica

Objeto/Região/Período: Guangxi (China), 2010–2018.
Tarefa/Alvo: previsão de ocorrência (binária) de incêndios florestais.
Variáveis: meteorologia (temperaturas, umidade, precipitação, pressão, vento, insolação), terreno (altitude, declividade, exposição), vegetação (NDVI), infraestrutura (rodovias, ferrovias, água, assentamentos), socioeconômicas (população, PIB), calendário (feriados).
Modelos: BPNN (rede neural de retropropagação); SVM (RBF).
Dados & Split: ~26.355 amostras (pontos de fogo e pontos aleatórios 1:1); treino 70%, teste 30%; SVM com busca em grade e 10-fold CV.
Métricas: acurácia, precisão, recall, F1, MSE; ROC e AUC.
Pré-processamento: normalização Min–Max; VIF para colinearidade (eliminação de 7 variáveis térmicas); seleção de atributos via Relief (8 variáveis finais: insolação, UR média, precipitação 20–20, vento médio, vento máx., distância a área residencial, latitude, feriados); codificação one-hot de categorias.
Código/Reprodutibilidade: não informado (Matlab 2019; LIBSVM).

## Trechos literais do artigo

> “The results showed that the prediction accuracy of the BP neural network and SVM is 92.16% and 89.89%.” (Li et al., 2020).
> “the AUC value of the SVM model was 0.95, which was lower than the BP neural network model.” (Li et al., 2020).
> “the sunshine time weight coefficient is the largest and has the greatest correlation with the target category.” (Li et al., 2020).
> “the eight feature subsets […] entered into the neural network model and support vector machine model establishment.” (Li et al., 2020).

## Leitura analítica e crítica

Metodologia: Delineamento supervisionado binário com amostragem 1:1 (fogo vs. não-fogo) espacial e temporalmente aleatória. VIF remove colinearidades fortes; Relief reduz de 26 para 8 variáveis, priorizando fatores meteorológicos. BPNN com topologia 8:10:2 definida por tentativa-e-erro; SVM com kernel RBF e otimização por grid search (C e γ) e 10-fold CV. Há risco de vazamento espaço-temporal, pois o split aleatório (70/30) não bloqueia dependências entre pontos próximos no espaço/tempo. A definição de pontos negativos aleatórios pode inflar métricas ao não refletir o verdadeiro desbalanceamento (incêndio raro).

Resultados: BPNN supera SVM em acurácia (92,16% vs. 89,89%) e F1/recall; ambos apresentam AUC alto (>0,95 reportado para SVM; BPNN indicado como superior). Importâncias por Relief destacam insolação, umidade, precipitação e vento como dominantes; variáveis antrópicas (distância a assentamentos, feriados) também entram no subconjunto ótimo, sugerindo papel de ignições humanas. O número elevado de vetores de suporte (5.166) sugere complexidade do SVM e possível sobreajuste.

Limitações: (i) Split aleatório propenso a superestimar generalização; (ii) Amostragem 1:1 distorce prevalência e pode favorecer métricas de acurácia; (iii) Ausência de validação por blocos espaço-temporais e de curvas precisão‑recall para dados raros; (iv) Critério de threshold do Relief é empírico; (v) Falta de reprodutibilidade (código/dados não abertos). Métricas de erro (MSE) são pouco informativas para classificação; ausência de PR‑AUC e análise de detecção precoce.

Qualidade: Texto claro sobre fontes de dados e engenharia básica; justificam Relief e VIF. Transparência parcial (detalhes de tuning do MLP limitados). Estatística de incerteza ausente (sem IC/DP em múltiplas rodadas). Reprodutibilidade fraca.

## Relação com o TCC

Relevância: alta.
Por que importa: O estudo compara ANN e SVM com variáveis climáticas, destaca meteorologia como principal bloco de features e relata AUC/recall elevados. Serve como baseline metodológico e como alerta para vieses de split e amostragem. Traz lista de variáveis climáticas compatíveis com INMET e prática comum de construir negativos aleatórios, que devemos substituir por janelas e validação espaço‑temporal.

## Tabela resumida

| Item                | Conteúdo                                                                                                          |
| ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Variáveis           | Insolação, UR, precipitação, vento, pressão, temperatura, NDVI, relevo, infraestrutura, socioeconômicas, feriados |
| Modelos             | BPNN (8:10:2), SVM (RBF)                                                                                          |
| Validação           | Split aleatório 70/30; SVM com 10-fold CV para ajuste de hiperparâmetros                                          |
| Métricas principais | Acurácia, Precisão, Recall, F1, AUC                                                                               |
| Melhor desempenho   | BPNN ≈ 92,16% (teste); AUC reportada superior à do SVM                                                            |
| Limitações          | Risco de vazamento espaço-temporal; amostragem 1:1; ausência de PR‑AUC; reprodutibilidade limitada                |

## Itens acionáveis para o TCC

1. Implementar validação por blocos espaço‑temporais (ex.: anos 2005–2020 treino, 2021–2025 teste; blocagem espacial por célula 0,05°) para evitar vazamento.
2. Substituir amostragem 1:1 por prevalência real com técnicas de desbalanceamento (class weights, focal loss, SMOTE‑Tomek) e usar PR‑AUC/F1 como métricas‑chave.
3. Replicar conjunto de variáveis climáticas (insolação, UR, precipitação, vento) e incorporar contexto antrópico; aplicar SHAP para ordenação de importâncias e análise de dependência parcial.

---
