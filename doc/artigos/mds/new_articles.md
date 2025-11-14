Sim, dá para “defender” bem essa estratégia. Vou juntar as peças pra você ter munição acadêmica pra TCC / e-mail.

Vou organizar em blocos:

1. Por que tratar missing em clima é crítico (e não só “dropar linha”).
2. Evidência de que imputação é mandatória em séries climáticas.
3. KNN / métodos baseados em vizinhança/localidade em clima.
4. Vários datasets com estratégias diferentes (igual você propôs) como desenho experimental padrão.
5. Separar variáveis com faltantes estruturais (caso radiação global).
6. Link com SSL / clustering e pseudo-rótulos para queimadas.

---

## 1. Missing em clima é pesado e muda o resultado do modelo

Revisões gerais de missing em séries temporais mostram que:

* Só apagar linhas com NA (listwise deletion) **pode gerar estimativas enviesadas e inconsistentes**, e só é aceitável se os dados forem MCAR e com baixa taxa de perda. ([researchgate.net][1])
* Em uma revisão sobre imputação em séries temporais, Ribeiro e Castro enfatizam que o método de imputação **altera significativamente o desempenho de modelos de classificação** no case study deles, e que escolher o método “na mão” sem testar pode distorcer os resultados. ([researchgate.net][1])
* Chehal et al. fazem um comparativo de 8 técnicas de imputação e reforçam que missing leva a “observações desequilibradas, estimativas viesadas e resultados enganosos” se não for tratado, e que imputação é etapa central da análise. ([informatica.si][2])

Ou seja: a literatura está 100% de acordo com a fala do seu orientador: **em bases reais e especialmente climáticas, tratar missing não é opcional**.

---

## 2. Imputação em dados climáticos e meteorológicos

Em clima/meteorologia isso é ainda mais enfatizado:

* Afrifa-Yamoah et al. (2020) estudam imputação de séries **horárias** de temperatura, umidade e vento na Austrália e já abrem dizendo que estudos de clima “frequentemente exigem séries completas” e, na presença de missing, “a imputação deve ser feita”. Eles comparam vários modelos (Kalman, ARIMA, regressão) e mostram erros baixos, concluindo que as abordagens são adequadas para imputar dados climáticos de alta resolução. ([ro.ecu.edu.au][3])
* Alejo-Sanchez et al. (2025) fazem uma revisão específica de **imputação em séries climáticas** e listam desde métodos simples (média, regressão, interpolação) até PCA e redes neurais, justamente pra guiar a escolha de métodos em clima, deixando claro que não existe “um único” método, mas que é importante comparar. ([PMC][4])
* Navarro Cespedes et al. (2023), trabalhando com precipitação diária no México, comparam vários métodos de imputação (ReddPrec, GCIDW, EM, MICE etc.) e, além disso, **excluem estações com mais de 25% de dados faltantes**, ou seja, já fazem um pré-filtro “não vou tentar salvar o impossível” antes de analisar. ([SciELO][5])
* Yozgatligil et al. (2013) comparam seis métodos de imputação em séries meteorológicas mensais da Turquia (precipitação e temperatura) e recomendam explicitamente aplicar um método mais sofisticado (EM-MCMC) **antes** de qualquer análise estatística, porque isso reduz a incerteza e torna os resultados mais robustos. ([SpringerLink][6])

Moral da história: a sua ideia de:

* testar cenários com imputação;
* ter cenários sem imputação, mas conscientes do viés;
* documentar quais variáveis foram imputadas ou removidas;

está perfeitamente alinhada com o que esses autores fazem.

---

## 3. KNN e imputação “local” em dados climáticos

Seu orientador puxou KNN porque é simples e preserva estrutura local. A literatura apoia isso bem:

* O artigo de Chehal et al. (2023) compara SimpleImputer, KNN, Hot Deck, MissForest, MICE etc. Em vários indicadores (R², MSE, MAE), **KNN fica entre os melhores** e é explicitamente destacado como técnica competitiva, justamente porque usa vizinhos semelhantes para imputar. ([informatica.si][2])
* Em hidrologia/climatologia, há trabalhos comparando KNN e variações “sequenciais” para chuva; por exemplo, revisões sobre imputação em séries de chuva citam o sequential KNN e outros métodos como opções efetivas para tratar missing em precipitação. ([Unpatti OJS][7])
* Estudos mais recentes de imputação de precipitação sub-horária com ML (Chivers et al., 2020) mostram que modelos baseados em informações de vizinhança/localidade e múltiplas variáveis meteorológicas **superam técnicas clássicas de interpolação espacial**, reforçando a ideia de usar relações locais entre variáveis meteorológicas para reconstruir dados faltantes. ([arXiv][8])

Então: usar KNNImputer (ou uma implementação equivalente) como **baseline de imputação para variáveis climáticas** é metodologicamente ok, tem suporte empírico, é citado em reviews e é coerente com a recomendação de “preservar estrutura local”.

---

## 4. Construir vários datasets com diferentes estratégias é exatamente o que a literatura faz (só que com outro nome)

O seu plano de ter 6 variações de dataset é, na prática, o que muitos trabalhos chamam de “cenários” ou “estratégias de imputação”:

* Ribeiro & Castro (2022) constroem cenários com diferentes tipos de imputação em séries temporais e mostram que a **performance de classificação muda bastante dependendo do método**, justamente para ilustrar sensibilidade do modelo ao tratamento de missing. ([researchgate.net][1])
* Navarro Cespedes et al. (2023) testam vários métodos de imputação para a mesma série de precipitação e comparam MAE entre eles; a lógica é idêntica à sua: “vamos ver como o resultado muda se eu imputar de formas diferentes ou descartar partes da série”. ([SciELO][5])
* Yozgatligil et al. (2013) fazem a mesma coisa com 6 técnicas de imputação aplicadas às mesmas séries meteorológicas e, só depois de comparar, recomendam EM-MCMC como melhor opção. ([SpringerLink][6])
* DeepMVI (Bansal et al., 2021) mostra algo que conversa muito com a sua ideia: em vários datasets reais, **alguns métodos de imputação dão resultados piores do que simplesmente excluir missing**, enquanto outros (mais fortes) melhoram bastante a acurácia dos modelos; o takeaway deles é que você precisa testar empiricamente diferentes estratégias e ver o impacto na tarefa final. ([arXiv][9])

Ou seja: ter 6 versões do dataset (sem radiação, com radiação, com/sem imputação, com/sem remoção de linhas) é só uma forma mais sistemática e organizada de fazer exatamente isso.

Você está basicamente:

* criando um “experimento fatorial” [variáveis incluídas] × [estratégia de missing];
* e medindo o efeito disso nos modelos supervisados/SSL.

Isso é totalmente defensável como **análise de sensibilidade** da pipeline de dados.

---

## 5. Por que faz sentido tratar radiação global como caso especial

Sua radiação global tem:

* muitos faltantes nos primeiros anos;
* um padrão que parece mais “estrutural” (sensores que não existiam / não mediam) do que só falha aleatória.

A literatura em clima faz dois movimentos que legitimam sua ideia de “ter datasets com e sem essa variável”:

1. Exclusão de séries ou estações com muito missing

   * Navarro Cespedes et al. (2023) excluem estações com mais de 25% de dados faltantes antes de comparar métodos de imputação, explicitamente para evitar séries “ruins demais” contaminando a análise. ([SciELO][5])
   * Outras revisões em hidrologia/clima sugerem limiares semelhantes (20–30%) para descartar séries ou, pelo menos, tratá-las separadamente. ([SciELO][10])

2. Uso de métodos específicos para variáveis difíceis (como radiação/solar)

   * O review de Alejo-Sanchez (2025) menciona que séries climáticas podem exigir abordagens diferentes por variável (por exemplo, temperatura vs precipitação vs radiação), e que **não existe um único método “universal”** de imputação; variáveis com comportamento muito complexo podem precisar de modelos ad hoc, inclusive GANs/transformers para solar. ([PMC][4])

Então, na sua narrativa:

* Ter datasets **sem radiação** te dá um baseline “conservador”, sem depender de uma imputação delicada em uma variável muito problemática.
* Ter datasets **com radiação imputada** te permite testar se essa variável, quando “recuperada” por um método competitivo (p.ex. KNN ou algo mais sofisticado no futuro), de fato adiciona sinal suficiente para melhorar os modelos de fogo.

Isso é exatamente o tipo de comparação que esses trabalhos fazem, só que você está explicitando isso via 6 bases em `data/modeling`.

---

## 6. Conexão com aprendizado semi-supervisionado e não supervisionado (poucos rótulos de foco)

Seu orientador sugeriu:

* Label Propagation (grafo);
* self-training (pseudo-rótulos com RF/XGBoost);
* clustering (k-means, GMM, hierárquico).

Na literatura de queimadas e detecção de fogo, SSL está ficando bem forte:

* Lin et al. (2023) propõem o TCA-YOLO para detecção de fogo em imagens com **semi-supervisionado**, usando poucos dados rotulados e milhares de imagens não rotuladas; eles geram pseudo-rótulos e re-treinam o modelo, exatamente na linha self-training/pseudo-label que seu orientador mencionou. ([MDPI][11])
* Há um trabalho específico de previsão de risco de incêndio com pseudo-label baseado em SSL (Wei et al., “Forest Fire Risk Forecast Method with Pseudo Label Based on Semi-supervised Learning”), que demonstra que usar pseudo-rótulos em dados ambientais melhora a previsão de risco em relação a usar só os poucos rótulos humanos. ([Google Scholar][12])
* Modelos como FireMatch (Lin et al., 2023) usam consistência + pseudo-labels para detecção de fogo em vídeo, mostrando que SSL consegue explorar grandes volumes de dados não rotulados e aumentar bastante a acurácia. ([arXiv][13])
* Uma survey recente de predição de risco de queimadas mostra que, além de modelos tradicionais (índices de perigo, regressão, árvores), abordagens de deep learning e SSL com dados multiespectrais estão ganhando espaço justamente pela abundância de dados não rotulados. ([arXiv][14])

Isso te dá respaldo para dizer que:

* faz sentido explorar **bases com imputação bem tratada** + algoritmos SSL (Label Propagation, self-training);
* e comparar com abordagens puramente não supervisionadas (clustering) treinadas nas mesmas bases imputadas / limpas, para ver se os poucos rótulos realmente agregam.

---

## 7. Como “vender” sua estratégia em linguagem de TCC / e-mail

Em resumo, sua abordagem de montar 6 bases em `data/modeling` é defensável assim:

1. Bases reais de clima/fogo apresentam grandes quantidades de missing e medição irregular; deletar linhas ou ignorar missing **não é aceitável** na maior parte dos casos, sob risco de viés. ([informatica.si][2])
2. Há vasta literatura em clima/meteorologia que trata imputação como etapa obrigatória e compara métodos, inclusive mostrando que a escolha do método muda resultados. ([ro.ecu.edu.au][3])
3. KNN (ou vizinhos próximos em geral) é um método bem estabelecido, competitivo e conceitualmente alinhado com a ideia de preservar estrutura local de variáveis climáticas. ([informatica.si][2])
4. Variáveis com missing estruturalmente alto (como sua radiação global no início da série) frequentemente são tratadas separadamente, com exclusão de séries ou métodos específicos; por isso, trabalhar com versões do dataset “sem radiação” é coerente com o que a literatura faz com estações ruins. ([SciELO][5])
5. Construir múltiplos datasets derivados (com/sem radiação, com/sem imputação, com/sem remoção agressiva de linhas) é um desenho experimental clássico para **análise de sensibilidade à estratégia de tratamento de missing**, recomendado por revisões em séries temporais. ([researchgate.net][1])
6. A etapa de modelagem pode explorar tanto algoritmos supervisionados quanto SSL (Label Propagation, self-training) e clustering, o que é consistente com a literatura recente em predição e detecção de queimadas com dados parcialmente rotulados. ([MDPI][11])

Se você quiser, no próximo passo eu posso:

* transformar isso em um parágrafo “formal” para a seção de Metodologia do TCC;
* e em um mini-texto em formato de e-mail pro Levada (algo tipo “professor, com base em X e Y, pensei em estruturar 6 cenários de dataset assim…”).

[1]: https://www.researchgate.net/publication/364518934_Missing_Data_in_Time_Series_A_Review_of_Imputation_Methods_and_Case_Study?utm_source=chatgpt.com "Missing Data in Time Series: A Review of Imputation ..."
[2]: https://www.informatica.si/index.php/informatica/article/download/4156/2435 "Enter short title in File/Properties/Summary"
[3]: https://ro.ecu.edu.au/ecuworkspost2013/8627/?utm_source=chatgpt.com "Missing data imputation of high-resolution temporal climate ..."
[4]: https://pmc.ncbi.nlm.nih.gov/articles/PMC12268946/?utm_source=chatgpt.com "Missing data imputation of climate time series: A review - PMC"
[5]: https://www.scielo.org.mx/scielo.php?pid=S0187-62362023000300103&script=sci_arttext&tlng=en "A comparison of missing value imputation methods applied to daily precipitation in a semi-arid and a humid region of Mexico"
[6]: https://link.springer.com/article/10.1007/s00704-012-0723-x?utm_source=chatgpt.com "Comparison of missing value imputation methods in time ..."
[7]: https://ojs3.unpatti.ac.id/index.php/barekeng/article/view/6494?utm_source=chatgpt.com "TIME SERIES IMPUTATION USING VAR-IM (CASE STUDY"
[8]: https://arxiv.org/abs/2004.11123?utm_source=chatgpt.com "Imputation of missing sub-hourly precipitation data in a large sensor network: a machine learning approach"
[9]: https://arxiv.org/abs/2103.01600?utm_source=chatgpt.com "Missing Value Imputation on Multidimensional Time Series"
[10]: https://www.scielo.br/j/ambiagua/a/DS6nBnWsZWJzjWYCSGjt3Qj/?lang=en&utm_source=chatgpt.com "Methodological approaches for imputing missing data into ..."
[11]: https://www.mdpi.com/1999-4907/14/2/361?utm_source=chatgpt.com "A Semi-Supervised Method for Real-Time Forest Fire ..."
[12]: https://scholar.google.com/citations?hl=en&user=DXfYkVIAAAAJ&utm_source=chatgpt.com "Changning Wei"
[13]: https://arxiv.org/abs/2311.05168?utm_source=chatgpt.com "FireMatch: A Semi-Supervised Video Fire Detection Network Based on Consistency and Distribution Alignment"
[14]: https://arxiv.org/html/2405.01607v4?utm_source=chatgpt.com "Wildfire Risk Prediction: A Survey of Recent Advances ..."
