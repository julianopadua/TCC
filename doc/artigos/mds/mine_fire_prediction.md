# WANG2019 - Mine Fire Prediction Based on WEKA Data Mining (2019)

Autores: Xin Wang; Jian Hao; Jun Chen; Weijia Cheng. Local/Periódico/Conferência: IOP Conf. Ser.: Earth and Environmental Science 384 (012164), 2019. Link/DOI: [https://doi.org/10.1088/1755-1315/384/1/012164](https://doi.org/10.1088/1755-1315/384/1/012164).

## Ficha técnica

Objeto/Região/Período: Linhua Coal Mine, Guizhou, China; frente 20917. 
Tarefa/Alvo: Classificação do grau de perigo de incêndio em mina (none, weak, medium, strong). 
Variáveis: posição do ponto de medição (m), temperatura, teor de gás residual (m³/t), velocidade do vento (m³/h), O₂ (%), CO (ppm). 
Modelos: SVM (SMO), BP neural network (Multilayer Perceptron), J48 decision tree (C4.5). 
Dados & Split: 30 amostras; primeiras 22 para treino e últimas 8 para teste; também mencionam 10-fold.  
Métricas: Detailed Accuracy by Class, ROC (por classe), matriz de confusão; acurácia geral reportada para cada algoritmo.  
Pré-processamento: normalização, discretização, redução via rough set (mantiveram todos os atributos). 
Código/Reprodutibilidade: Weka; edição de dados no UltraEdit; sem repositório de código. 

## Trechos literais do artigo

> “using Weka… selecting SVM…, BP neural network and J48 decision tree to obtain the accuracy of the samples” (Wang et al., 2019). 
> “In this paper, 30 samples of mine fire… dangerous degree is divided into four categories: none, weak, medium and strong” (Wang et al., 2019). 
> “The correct examples of SVM, BP neural network and J48 decision tree are 7, 7 and 5 respectively… 87.5% = 87.5% > 62.5%” (Wang et al., 2019). 
> “the accuracy of the three algorithms is 87.5wt%, 87.5% and 62.5%, respectively.” (Wang et al., 2019). 

## Leitura analítica e crítica

Metodologia: O estudo compara SVM, MLP (BP) e J48 em um conjunto extremamente pequeno (30 amostras), com classes ordinalizadas em quatro níveis. O pré-processamento inclui normalização, discretização e tentativa de redução por rough set, mas os autores mantêm todas as variáveis, o que sugere não haver redundâncias claras. A validação é ambígua: mencionam 10-fold e, simultaneamente, um hold-out com 22/8 amostras; a descrição não deixa inequívoco se o 10-fold foi aplicado apenas no treino ou no conjunto completo, o que afeta a validade das métricas. Não há controle explícito de vazamento temporal nem espacial, ainda que a tarefa seja essencialmente tabular e local à mina. Tuning de hiperparâmetros não é descrito.

Resultados: Reportam acurácias de 87,5% para SVM e BP, e 62,5% para J48, alinhadas à matriz de confusão com 7, 7 e 5 acertos em 8 amostras, respectivamente. Em “Detailed Accuracy by Class”, apresentam ROC por classe, mas sem intervalo de confiança. Os erros médios listados para “node error rate” trazem uma incongruência interpretativa: a tabela sugere menores erros para BP, enquanto a discussão conclui que SVM e J48 seriam “melhores” que BP nesse aspecto, sem reconciliação estatística clara. 

Limitações: Tamanho amostral diminuto e potencial sobreajuste; ausência de análise de desbalanceamento entre categorias; falta de protocolo de validação reprodutível; nenhuma análise de sensibilidade ou ablação por grupo de variáveis. As features são operacionais de mina (gás residual, CO, O₂), não generalizáveis a incêndios florestais. Não há estimativas de incerteza, repetição com seeds distintos, nem separação por blocos tempo-espaciais.

Qualidade: Clareza básica do fluxo com Weka, porém transparência metodológica limitada. Não disponibilizam código nem dados. Métricas focadas em acurácia e ROC por classe, sem PR-AUC ou F1 para lidar com possíveis desbalanceamentos.

## Relação com o TCC 

Relevância: baixa.
Por que importa (se relevante): O artigo mostra um pipeline comparativo simples em Weka entre SVM, MLP e árvore, útil apenas como referência de estrutura de relatório de comparação.
Se baixa relevância: O estudo trata de incêndios em minas com variáveis específicas do subsolo e apenas 30 amostras, sem validação espaço-temporal, o que não se transfere para previsão diária de queimadas com dados climáticos do INMET/INPE.

## Tabela resumida

| Item                | Conteúdo                                                                                                   |
| ------------------- | ---------------------------------------------------------------------------------------------------------- |
| Variáveis           | Posição do ponto, temperatura, gás residual, vento, O₂, CO.                                                |
| Modelos             | SVM (SMO), BP/MLP, J48/C4.5.                                                                               |
| Validação           | Mencionam 10-fold e split 22/8; descrição ambígua.                                                         |
| Métricas principais | Acurácia, matriz de confusão, ROC por classe.                                                              |
| Melhor desempenho   | SVM e BP com 87,5% (7/8).                                                                                  |
| Limitações          | Amostra muito pequena; variáveis não climáticas; sem PR-AUC; validação pouco clara; sem reprodutibilidade. |

## Itens acionáveis para o TCC

1. Usar validação por blocos espaço-temporais e repetição estratificada para evitar vazamento e reduzir variância das estimativas.
2. Priorizar PR-AUC, F1 e curvas precisão-recall para desbalanceamento forte típico de focos diários; relatar incertezas com ICs via bootstrap.
3. Planejar ablações por grupos de variáveis climáticas e sazonalidade; incluir SHAP para modelos de árvore e análise de estabilidade das importâncias.

---
