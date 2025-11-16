# Alejo25-ImputationReview - Missing data imputation of climate time series: A review (2025)

Autores: Lizette Elena Alejo-Sanchez; Aldo Márquez-Grajales; Fernando Salas-Martínez; Anilu Franco-Arcega; Virgilio López-Morales; Otilio Arturo Acevedo-Sandoval; César Abelardo González-Ramírez; Ramiro Villegas-Vega.
Local/Periódico/Conferência: MethodsX (Elsevier).
Link/DOI: 10.1016/j.mex.2025.103455. 

---

## Ficha técnica

Objeto/Região/Período: revisão sistemática de estudos de imputação de dados faltantes em séries temporais climáticas (2015 em diante), com foco em exemplos de Ásia, Europa, América e Oceania.
Tarefa/Alvo: síntese crítica de técnicas de imputação para séries de temperatura, precipitação, umidade, vento, radiação solar e outras variáveis climáticas.
Variáveis: principalmente temperatura do ar, precipitação e umidade relativa; em menor frequência, vento, radiação solar, evapotranspiração, pressão atmosférica, fluxos de calor e carbono. 
Métodos: classificação em três blocos principais:

1. Métodos convencionais (estatísticos) como média, mediana, regressão simples e múltipla, interpolação espacial e temporal (IDW, Kriging), PCA/BPCA, MICE, VAR etc.
2. Métodos de aprendizado de máquina: missForest, KNN, ANN, Random Forest, SVM, SOM, Gradient Boosting, XGBoost, modelos híbridos com triangulação e PCA.
3. Métodos de aprendizado profundo: RNN, LSTM, BiLSTM, GAN, GAIN, autoencoders, transformers, arquiteturas híbridas espaço-temporais. 
   Dados & Estudos: 2015–2024, com estudos baseados majoritariamente em redes de monitoramento meteorológico de superfície (93 por cento) e minoritariamente em satélites (7 por cento). 
   Métricas: RMSE, MAE, NRMSE, NSE, coeficiente de correlação, KGE, índices de similaridade, testes de Kolmogorov-Smirnov; alguns estudos analisam também complexidade computacional.
   Questões centrais:

* Onde se concentra a produção científica sobre imputação climática.
* Quais variáveis são mais estudadas.
* Quais métodos funcionam melhor em diferentes cenários de missing (curtos vs longos, univariado vs multivariado, redes densas vs esparsas).

---

## Trechos literais do artigo

> “Missing data in climate time series is a significant problem.” 

> “This review presents techniques for imputing missing data on climate.” 

---

## Leitura analítica e crítica

Metodologia: O artigo faz uma revisão estruturada de literatura usando várias bases (Dimensions, Google Scholar, Scopus, ResearchRabbit), com palavras-chave envolvendo climate, time series, missing data, imputation. Restringe o período a 2015 em diante, apenas em inglês e com foco em artigos indexados, capítulos e anais de conferência. Após limpeza de duplicatas, os autores constroem um banco de estudos e extraem, para cada um, região de estudo, variáveis climáticas, método de imputação, fonte de dados e principais resultados. 

Panorama: Asia e Europa concentram a maior parte dos trabalhos; Malásia, China e Itália se destacam. No Ocidente, Brasil aparece como um dos países que mais produzem sobre imputação climática, ao lado da Austrália. Mostram também que temperatura e precipitação dominam as aplicações, enquanto radiação solar, vento e outras variáveis mais “caras” aparecem menos vezes. A imensa maioria das séries vem de redes de monitoramento de superfície; uso de satélite é minoritário e, quando aparece, muitas vezes trata nuvens como “dados faltantes” em produtos como LST. 

Convencionais: A seção de métodos convencionais é bem rica. Os autores percorrem desde imputação por média e mediana até regressões simples/múltiplas, transformadas de Fourier, interpolação IDW e Kriging, PCA/BPCA, NIPALS, MICE, SSA e variações. De forma geral, argumentam que:

* Interpolações simples funcionam bem em dados suaves e frequentes (horários, subdiários) e em lacunas curtas, mas degradam com buracos longos ou comportamento não linear.
* Métodos baseados em regressão e PCA exploram correlações espaciais e entre variáveis, mas exigem alta correlação e redes relativamente densas; quando isso falha, o ganho desaparece.
* Geostatística (Kriging, cokriging) é poderosa, mas computacionalmente cara e dependente de modelagem cuidadosa de semivariogramas.
* Imputações por média/mediana são baratas e populares, mas distorcem variância e assimetria, achatando extremos e alterando a estrutura de dependência da série. 

Eles destacam também trabalhos brasileiros focados em radiação solar e precipitação, usando MICE/PMM, midastouch e Kalman em séries horárias e diárias. Em radiação, resultados bons aparecem em séries de alta resolução e com forte dependência temporal; quando se agrega demais ou há longos vazios, a qualidade cai. 

Aprendizado de máquina: No bloco de machine learning, o artigo mostra que missForest virou uma espécie de “baseline forte” para imputação de séries climáticas multivariadas: funciona bem em mistura de variáveis contínuas e categóricas, lida com não linearidades e produz bons erros em vários estudos de precipitação, temperatura e umidade relativa. O custo é alta complexidade computacional, crescendo com o número de amostras e variáveis. KNN, por sua vez, aparece em vários trabalhos, frequentemente como método de comparação: é simples, razoavelmente bom com correlações locais, mas sofre quando a dimensão aumenta ou quando o conjunto é muito grande. Random Forest, combinado com MICE ou PCA, é citado como alternativa robusta em cenários multivariados e com grandes blocos de missing. 

Redes neurais aparecem como bloco à parte dentro de ML: MLP, redes feed-forward, SOMs, híbridos com triangulação espacial ou Kriging. A mensagem central é que, sozinhas, elas nem sempre ganham dos métodos tabulares bem calibrados, mas quando combinadas com pré-imputações estatísticas ou uso explícito da estrutura espacial, tendem a produzir os melhores resultados. O custo é alto: treinamento caro, hiperparâmetros sensíveis, necessidade de grande volume de dados limpos para não sobreajustar. 

Aprendizado profundo: Na parte de deep learning, o artigo revisa GNNs, LSTM/BiLSTM, redes recorrentes com memória linear (LIME-RNN), GANs, GAIN, transformers e modelos híbridos espaço-temporais aplicados a temperatura, precipitação, LST, umidade do solo e outras séries. O resumo crítico é que essas técnicas conseguem lidar melhor com padrões complexos, múltiplas escalas temporais e grandes blocos de missing, especialmente quando há forte sazonalidade ou dinâmica não linear. GANs e variações (GAIN, XGBoost-DE em contexto de radiação solar) são apontadas como particularmente promissoras para reconstruir segmentos longos, mas exigem tuning pesado e podem ser instáveis. Alguns estudos mostram, inclusive, que métodos mais “clássicos” de matriz incompleta ou MICE ainda ganham em alguns datasets, o que reforça o argumento de que não existe “melhor método universal”. 

Conclusão geral: Os autores enfatizam que a escolha do método deve levar em conta:

1. tipo de variável (temperatura, chuva, radiação etc),
2. resolução temporal (horária, diária, mensal),
3. padrão de missing (isolado, blocos longos, aleatório, sistemático),
4. estrutura espacial (rede densa ou esparsa) e
5. recursos computacionais disponíveis.
   Métodos simples podem ser adequados para análises exploratórias ou para lacunas curtas, mas reconstruções agressivas com ML ou deep learning introduzem uma camada extra de incerteza que precisa ser explicitada. 

---

## Relação com o TCC

Relevância: máxima para a parte de tratamento de dados faltantes e para justificar as diferentes bases de modelagem (com e sem imputação) no seu TCC de queimadas no Cerrado.

Por que importa para o TCC:

1. O artigo confirma que séries climáticas reais, inclusive de redes oficiais, têm missing sistemático e complexo (falhas de estação, infraestrutura, armazenamento), exatamente a situação de INMET e BDQueimadas. Isso legitima dedicar um capítulo inteiro só à auditoria de missing e à definição de semântica de sentinelas.

2. A crítica aos métodos simples de média/mediana apoia a sua decisão de **não** “inventar” dados indiscriminadamente. Ter bases onde você apenas harmoniza variáveis, converte sentinelas em NaN e depois ou remove linhas problemáticas ou deixa o modelo lidar com missing é coerente com a literatura, que alerta para distorções na variância e na distribuição quando se exagera na substituição por médias.

3. Ao mesmo tempo, o artigo mostra que há espaço para técnicas mais sofisticadas de imputação quando a variável é importante e a taxa de missing é alta. É exatamente o caso da radiação global no seu dataset: variável fisicamente relevante, mas com buracos enormes. Isso sustenta teoricamente a sua ideia de criar cenários específicos em que a radiação é imputada (por exemplo com KNN) e comparar com cenários em que ela é simplesmente removida ou em que as linhas faltantes são descartadas.

4. A discussão sobre complexidade computacional e custo de calibração ajuda a justificar por que você não parte direto para missForest, GANs ou transformers para imputar radiação global. Para um TCC, é razoável optar por soluções intermediárias e transparentes (como KNNImputer) e usar o artigo para argumentar que métodos mais avançados ficam como linha de pesquisa futura.

5. A ênfase do artigo na dependência do contexto (tipo de variável, resolução temporal, padrão de missing) dialoga diretamente com a sua auditoria anual, onde você mede proporções de missing por coluna e por classe de foco. Você pode citar essa revisão ao explicar porque escolheu radiação como variável candidata a imputação “pesada” e, em contrapartida, decidiu não imputar variáveis com buracos extremos ou pouca correlação comprovada.

---

## Tabela resumida

| Item               | Conteúdo                                                                                                                                                |
| ------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Objetivo           | Revisar e classificar métodos de imputação para séries climáticas, incluindo estatísticos, machine learning e deep learning                             |
| Escopo             | Estudos 2015–2024 sobre temperatura, precipitação, umidade, vento, radiação, etc., com foco em redes de monitoramento de superfície                     |
| Métodos            | Média/mediana, regressões, interpolação, PCA/BPCA, MICE, SSA, Kriging; missForest, KNN, RF, ANN, SOM, GB, XGBoost; GAN, GNN, LSTM, transformers         |
| Principais achados | Não há método único melhor; desempenho depende de variável, resolução, padrão de missing e densidade da rede; missForest e GANs são destaques           |
| Sobre radiação     | Estudos de radiação solar usam MICE, midastouch, Kalman e métodos híbridos; boas performances aparecem em séries horárias com rede densa                |
| Limitações         | Poucos trabalhos em alguns países; pouco uso de PR-AUC; custo computacional alto em ML/deep learning; desempenho sensível à calibração dos métodos      |
| Contribuição       | Fornece mapa conceitual para escolher estratégias de imputação e evidencia trade-offs entre precisão, complexidade e preservação da estrutura dos dados |

---

## Itens acionáveis para o TCC

1. Na seção de metodologia de dados faltantes, citar explicitamente esta revisão ao explicar por que você criou **múltiplos cenários de base**:

   * bases que apenas harmonizam e limpam sentinelas, sem imputação agressiva;
   * bases que descartam linhas com missing em features;
   * bases que aplicam KNNImputer, em especial nas versões com radiação global.

2. Usar o artigo para fortalecer a argumentação de que **radiação global merece tratamento diferenciado**:

   * é uma variável menos observada e mais difícil de imputar;
   * os trabalhos revisados usam métodos relativamente sofisticados e, mesmo assim, reconhecem limitações em blocos longos de missing.
     Isso ajuda a justificar por que você tem cenários “sem radiação” versus cenários com radiação imputada e por que compará-los é parte central da contribuição do TCC.

3. Incluir, na discussão de resultados, uma pequena subseção de “Posicionamento em relação ao estado da arte em imputação climática”, conectando:

   * os métodos que você escolheu (sem imputação, KNN, remoção de linhas)
   * com as categorias desta revisão (convencional, ML, deep learning),
     deixando claro que o seu objetivo não é propor o melhor imputador do mundo, mas **avaliar o impacto de decisões simples e reprodutíveis de imputação vs não imputação** sobre o desempenho de modelos de previsão de queimadas.
