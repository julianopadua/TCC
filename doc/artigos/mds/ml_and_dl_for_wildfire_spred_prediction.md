# Andrianarivony2024-Review – Machine Learning and Deep Learning for Wildfire Spread Prediction: A Review (2024)

## Ficha técnica

Autores: Henintsoa S. Andrianarivony; Moulay A. Akhloufi.
Periódico: Fire (MDPI), 7(12):482. 

Escopo: revisão sistemática de modelos de previsão de propagação de incêndios florestais (wildfire spread) baseados em técnicas de machine learning (ML) e deep learning (DL), com comparação a modelos clássicos físico/empíricos e discussão de métricas, bases de dados e desafios em aberto. 

---

## Objeto da revisão e posição em relação aos modelos clássicos

O artigo parte da constatação de que modelos clássicos de propagação, como FARSITE e Prometheus, baseados em equações físicas e regras empíricas (e muitas vezes implementados via autômatos celulares), exigem preparação de dados extensa, são computacionalmente custosos e têm dificuldade em representar a complexidade espaço temporal do fogo em tempo quase real. 

Nesse contexto, ML e DL são apresentados como alternativas data-driven capazes de incorporar múltiplas fontes de dados (meteorologia, topografia, combustível, uso do solo, imagens de satélite) e aprender padrões não lineares sem especificar explicitamente todas as interações físico-químicas. A revisão organiza o campo em famílias de modelos (ML tabular, CNN, ConvLSTM/CRN, transformers, RL, GNN) e discute em quais cenários cada família se mostra mais adequada. 

---

## Principais famílias de modelos e implicações teóricas

1. **Modelos de ML “clássico” (tabular)**
   São discutidos algoritmos como Support Vector Machines/Regression, Gaussian Process Regression, árvores de decisão, ensembles tipo Random Forest/Gradient Boosting e redes rasas. Esses modelos operam tipicamente sobre dados tabulares (variáveis meteorológicas, índices de seca, variáveis de combustível, dados de campo) e têm como alvo taxa de propagação, área queimada ou indicadores de comportamento do fogo. A revisão ressalta que tais modelos funcionam bem com conjuntos de dados relativamente pequenos, são mais interpretáveis e exigem menor custo computacional, mas capturam de forma limitada a estrutura espacial explícita da frente de fogo. 

2. **Redes convolucionais (CNN) para mapas de propagação**
   CNNs (U-Net, FireCast, multi-kernel CNN, redes hierárquicas como WFNet) são usadas para prever máscaras de área queimada ou campos contínuos de suscetibilidade / probabilidade de queima em grade, a partir de imagens de satélite (NDVI, LST), dados topográficos (DEM, declividade) e variáveis meteorológicas. Em geral, esses modelos superam simuladores físicos em acurácia para tarefas específicas e conseguem operar em resoluções espaciais finas com custo computacional muito menor, o que sustenta teoricamente o uso de modelos de árvore e CNNs em contextos onde o objetivo é mapear risco/espaço, não apenas descrever a física do fogo. 

3. **Redes convolucionais recorrentes (ConvLSTM, CRN) e modelos de séries temporais**
   ConvLSTM e arquiteturas híbridas CNN+LSTM são apresentadas como centrais para capturar simultaneamente a estrutura espacial (imagens/máscaras) e a dinâmica temporal (sequências diárias ou subdiárias de propagação). Esses modelos aprendem um processo autoregressivo de evolução do fogo (usar o estado previsto em (t) para prever (t+1)), obtendo bons resultados em janelas de horas a dias à frente. Do ponto de vista teórico, isso reforça a ideia de que incêndios devem ser tratados como processos espaço temporais com memória de curto e médio prazo, e não como eventos independentes. 

4. **Transformers, RL e GNNs como fronteira do estado da arte**
   A revisão destaca o uso inicial de transformers (Swin U-Net, MA-Net, AutoST-Net) para lidar com dependências de longo alcance em espaço e tempo, reforçando o papel de atenção para combinar múltiplas camadas de variáveis (fogo anterior, clima, vegetação, uso do solo). Modelos de reinforcement learning (MCTS, A3C) aparecem mais ligados a simulação e tomada de decisão (planejamento de combate), e GNNs são propostos para representar a paisagem como grafo irregular, explorando vizinhanças adaptativas e rotas explícitas de propagação. Teoricamente, essas abordagens consolidam a visão do fogo como processo dinâmico em rede, com interações locais e globais. 

---

## Métricas, bases de dados e papel das séries espaço temporais

O artigo sistematiza métricas usadas para avaliação:

* tarefas de regressão (MAE, RMSE, MAPE) para área queimada e taxa de propagação;
* tarefas de classificação (accuracy, precision, recall, F1) para presença/ausência de fogo em grade;
* métricas espaciais (IoU, Sorensen–Dice) para sobreposição entre máscara prevista e área queimada observada. 

Do ponto de vista de dados, a revisão lista e caracteriza conjuntos amplamente usados, como Next Day Wildfire Spread, FEDS, WildfireSpreadTS, WildfireDB, Mesogeos, dados VIIRS/MODIS/Himawari, além de produtos de área queimada e bases nacionais (por exemplo, Canadian Fire Spread dataset). Todos combinam, em maior ou menor grau, (i) histórico de fogo (máscaras, perímetros, data de queima); (ii) variáveis meteorológicas; (iii) topografia; (iv) uso e cobertura da terra; e, em alguns casos, (v) proxies de pressão antrópica. Isso fundamenta teoricamente a necessidade de integrar múltiplas camadas ambientais e climáticas em qualquer modelo de ocorrência/propagação. 

---

## Desafios, lacunas e como a revisão fundamenta o TCC

A síntese crítica do artigo destaca cinco eixos de limitação: (i) dificuldade de comparação entre modelos devido a datasets e métricas heterogêneos; (ii) baixa explicabilidade dos modelos mais complexos, com necessidade de XAI (Grad-CAM, SHAP) para ganhar confiança operacional; (iii) carência de modelos realmente leves e de tempo real para uso em campo; (iv) problemas de generalização espacial, com modelos treinados em uma região e desempenho reduzido em outras; e (v) limitações e viés de conjuntos de dados (regiões pouco amostradas, resolução desigual, dependência de dados simulados). 

Para a fundamentação teórica do TCC, essa revisão cumpre três papéis principais:

1. **Justifica o foco em ML/DL para incêndios**: demonstra que a comunidade internacional converge para ML/DL como alternativa mais flexível e acurada que modelos puramente físicos para previsão de comportamento do fogo, sobretudo em problemas que envolvem múltiplas variáveis climáticas e ambientais.
2. **Delimita o espaço de modelos**: oferece um “mapa” das famílias de algoritmos relevantes (ML tabular, ensembles, CNN, ConvLSTM, transformers, GNN, RL) e de como eles dialogam com diferentes tipos de dado e formulações de alvo, permitindo justificar a escolha de modelos supervisionados específicos no contexto de ocorrência de focos.
3. **Evidencia uma lacuna temática e geográfica**: apesar de revisar extensivamente trabalhos em América do Norte, Europa e Ásia, o artigo não traz aplicações focadas no Cerrado brasileiro nem no uso sistemático conjunto de BDQueimadas e dados meteorológicos nacionais para modelos supervisionados de ocorrência. Essa ausência reforça a relevância de um estudo direcionado ao Cerrado, com recorte metodológico próprio (modelos supervisionados para probabilidade de ocorrência) ancorado nas mesmas premissas teóricas que sustentam os modelos de propagação revisados.
