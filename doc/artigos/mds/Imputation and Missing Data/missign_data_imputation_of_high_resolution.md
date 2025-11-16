# Afrifa20-MissingClimate - Missing data imputation of high-resolution temporal climate time series data (2020)

Autores: Ebenezer Afrifa-Yamoah; Ute A. Mueller; S. M. Taylor; A. J. Fisher.
Periódico: Meteorological Applications (Royal Meteorological Society).
Link/DOI: 10.1002/met.1873. 

---

## Ficha técnica

Objeto/Região/Período:
Quatro estações costeiras da Austrália Ocidental (Esperance, Perth, Exmouth, Broome); séries horárias de 12 meses (01/03/2011 a 29/02/2012).

Tarefa/Alvo:
Imputar valores faltantes em séries climáticas de alta resolução (temperatura, umidade relativa e velocidade do vento), avaliando diferentes classes de modelos.

Variáveis:
Alvo de imputação: temperatura do ar, umidade relativa, velocidade do vento.
Preditoras: precipitação, direção do vento (transformada em seno/cosseno), rajadas de vento, pressão ao nível do mar, além das próprias séries alvo em defasagens temporais. 

Modelos:

1. ARIMA univariado com representação em espaço de estados e suavização de Kalman.
2. Modelo estrutural de séries temporais (componentes de nível, tendência e sazonalidade) também com filtro/smoother de Kalman.
3. Regressão linear múltipla (incluindo sen/cos da direção do vento) com erros robustos a heterocedasticidade e autocorrelação (Newey-West). 

Dados e esquema de validação:

1. Para cada local, os autores identificam o maior intervalo completamente observado (sem falhas).
2. Nesses sub-conjuntos completos, criam lacunas artificiais, removendo 10% dos pontos, em blocos consecutivos de vários comprimentos, imitando padrões reais de falhas de sensores.
3. Usam validação cruzada 5-fold: cinco padrões distintos de lacunas; em cada um imputam e comparam com os valores verdadeiros. 

Métricas:
MAE, RMSE, SMAPE e correlação de Pearson entre valores observados e imputados.

Pré-processamento:
Transformação trigonométrica da direção do vento; formulação de ARIMA e modelos estruturais em espaço de estados; uso de filtros de Kalman para previsão e suavização; escolha de modelos com base em critérios de informação, assumindo mecanismo de missing MAR (missing at random). 

Código/Reprodutibilidade:
Implementação em R 3.5.1, usando o pacote imputeTS (função na.kalman com opções StructTS e auto.arima) para os métodos de Kalman, e regressão linear com erros robustos. Dados horários fornecidos pelo Bureau of Meteorology; não há repositório de código dedicado. 

---

## Trechos literais do artigo

> “Climate studies require complete time series data which, in the presence of missing data, means that imputation must be undertaken.” 

> “Climatic data are generally characterized by properties such as autocorrelation between time lags, seasonality, periodic trends, cycles and the homogeneity effect over geographical areas.” 

> “Most imputation methods assume MCAR and MAR because their missing data mechanisms are said to be ignorable.” 

> “The multiple linear regression model was generally the best model based on the pooled performance indicators, followed by the ARIMA with Kalman smoothing.” 

> “The methods studied have demonstrated suitability in imputing missing data in hourly temperature, humidity and wind speed data.” 

---

## Leitura analítica e crítica

Metodologia:
O artigo mira exatamente o problema de imputação em séries climáticas horárias de alta resolução, cenário muito próximo do uso que fazemos dos dados do INMET. Em vez de trabalhar em dados diários/mensais agregados, eles preservam a escala horária e assumem um mecanismo de missing MAR. Com isso, constroem lacunas artificiais de 10% em sub-séries completas, o que permite comparar diretamente valores imputados com os valores verdadeiros via validação cruzada 5-fold. A discussão conceitual de mecanismos MCAR, MAR e MNAR é cuidadosa: os autores enfatizam que a maior parte dos métodos supõe MCAR/MAR (mecanismos “ignoráveis”) e que entender a causa física das falhas é crucial antes de imputar. 

Nos modelos univariados, ARIMA e a formulação estrutural em espaço de estados exploram a autocorrelação, sazonalidade diária e possíveis tendências das séries climáticas. Para cada variável alvo (por exemplo, temperatura), o modelo tenta reconstruir o valor faltante seguindo o padrão temporal passado, suavizado via filtro/smoother de Kalman. Já a regressão múltipla explora a correlação cruzada entre variáveis (por exemplo, temperatura e umidade, ou velocidade do vento e pressão), incorporando direção do vento via seno/cosseno para respeitar a natureza circular da variável. Esse design é muito próximo do que faríamos ao tentar imputar radiação global a partir de outras variáveis meteorológicas correlacionadas. 

Resultados:
No agregado das cinco dobras e quatro localidades, a regressão múltipla tende a apresentar os menores MAE/SMAPE, especialmente em estações onde temperatura e umidade são fortemente correlacionadas. Em regiões tropicais com regime climático diferente (Broome), onde as correlações são mais fracas, a vantagem da regressão diminui e modelos ARIMA com Kalman passam a competir mais de perto. De qualquer forma, os erros médios são pequenos: para temperatura, MAE em torno de 0,25 °C; para umidade, cerca de 1,3 pontos percentuais; para vento, cerca de 0,56 km/h. Os autores reforçam essa conclusão mostrando que as distribuições dos valores imputados praticamente coincidem com as distribuições originais, e que a correlação entre imputado e observado geralmente excede 0,95. 

Limitações:
Eles próprios destacam vários limites que importam para o nosso TCC. Primeiro, trabalham com sub-séries de apenas um ano em cada local, com estrutura temporal relativamente simples; séries mais longas e complexas poderiam favorecer ainda mais modelos estruturais. Segundo, consideram 10% de missing, com lacunas variadas, mas não exploram cenários de missing extremo nem mecanismos claramente não ignoráveis (como falhas sistemáticas em determinados horários ou condições meteorológicas). Terceiro, a boa performance da regressão depende fortemente da disponibilidade de preditores sem falhas; quando as mesmas variáveis sofrem missing, essa abordagem deixa de ser viável. Por fim, a suposição de MAR pode não valer para lacunas causadas por manutenções programadas, cortes de energia ou saturação de sensores, casos em que a probabilidade de faltar dado depende do próprio processo climático. 

Qualidade geral:
O trabalho é sólido como estudo metodológico de imputação em clima de alta resolução: discute teoricamente mecanismos de missing, descreve bem os modelos em espaço de estados, aplica uma validação cruzada clara e inspeciona não apenas métricas numéricas, mas também a preservação da distribuição dos dados. Ao mesmo tempo, é honesto em não vender imputação como panaceia: explicita o papel da estrutura local dos dados (clima regional, correlações entre variáveis) e sugere que a escolha de método deve ser guiada por essas características e pelas causas físicas do missing.

---

## Relação com o TCC

Relevância: altíssima, porque dialoga diretamente com três decisões centrais do nosso TCC:

1. Trabalhar em escala horária com dados do INMET (temperatura, umidade, vento, pressão, radiação global quando disponível).
2. Assumir explicitamente uma semântica de missing (NaN + sentinelas) e discutir se vamos imputar ou não determinadas variáveis.
3. Justificar a existência de cenários de modelagem com e sem imputação, especialmente para radiação global.

Como fundamenta a opção por bases sem imputação:
O artigo mostra que qualquer imputação é, na prática, a aplicação de um modelo estatístico adicional em cima dos dados de entrada. Esse modelo depende de hipóteses sobre o mecanismo de missing (MCAR/MAR), da estrutura temporal e das correlações entre variáveis. Em contextos onde:

* o percentual de missing é muito alto,
* a causa do missing pode ser não ignorável (por exemplo, sensores de radiação que falham justamente em dias muito nublados),
* ou queremos avaliar o desempenho dos modelos de fogo apenas sobre observações que realmente existiram,

faz sentido manter uma base “sem imputação”, em que missing é tratado com exclusão de linhas ou de colunas, em vez de preencher tudo com valores artificiais. A discussão de Afrifa-Yamoah et al. reforça a ideia de que a imputação altera a distribuição e a prevalência de certas condições e, portanto, deve ser vista como mais uma escolha de modelagem a ser avaliada, não como padrão obrigatório. 

Como fundamenta a opção por imputar radiação global em cenários específicos:
Por outro lado, o trabalho também mostra que, para variáveis climáticas de alta correlação e comportamento suavizado (como temperatura, umidade e vento), modelos bem especificados conseguem imputar valores com erros muito pequenos e preservar a distribuição original. Isso é um argumento forte a favor de um cenário em que forcemos a presença de radiação global via imputação, já que:

* radiação é física e teoricamente relevante para o risco de fogo;
* em muitas estações ela é fortemente correlacionada com outras variáveis (temperatura, cobertura de nuvens via proxies, etc.);
* podemos usar modelos regressivos e/ou temporais (ainda que simplificados em relação ao artigo) para reconstruir a série de forma plausível.

Nesse sentido, o artigo dá a base teórica para dizer: “quando a variável é crucial para o mecanismo físico da queimada e há estrutura temporal e correlação suficientes, imputar cuidadosamente pode ser preferível a simplesmente descartá-la”. O nosso desenho de múltiplas bases de modelagem (sem radiação, com radiação imputada, com remoção de linhas, com KNN) conversa exatamente com essa visão de imputação como escolha metodológica que produz cenários alternativos a serem comparados. 

---

## Tabela resumida

| Item                | Conteúdo                                                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| Variáveis alvo      | Temperatura, umidade relativa, velocidade do vento (horárias)                                                                        |
| Preditoras          | Precipitação, direção do vento (sen/cos), rajada, pressão ao nível do mar, além de defasagens das próprias séries                    |
| Modelos             | ARIMA em espaço de estados com Kalman; modelo estrutural com Kalman; regressão linear múltipla com erros robustos                    |
| Validação           | Sub-séries completas por local; 10% de dados removidos em blocos; validação cruzada 5-fold com comparação imputado vs real           |
| Métricas principais | MAE, RMSE, SMAPE, correlação entre imputado e observado                                                                              |
| Achados centrais    | Regressão múltipla geralmente melhor; ARIMA próximo; erros médios baixos; imputações preservam bem a distribuição dos dados          |
| Limitações          | Séries de apenas 1 ano, 10% de missing; suposição MAR; regressão depende de preditores completos; não há cenários de missing extremo |

---

## Itens acionáveis para o TCC

1. Na seção de metodologia de dados faltantes, citar explicitamente Afrifa-Yamoah et al. como referência para:
   a) a discussão de MCAR/MAR/MNAR em clima de alta resolução;
   b) a ideia de que modelos de imputação são escolhas de modelagem adicionais que precisam ser avaliadas, não pressupostas.

2. Justificar a existência de **cenários sem imputação** (bases C e D, com remoção de linhas) dizendo que, à luz do artigo, queremos comparar modelos treinados apenas em observações reais com modelos treinados em bases imputadas, isolando o efeito da imputação na performance.

3. Justificar a criação de **cenários com imputação de radiação global** (por exemplo, bases E e B com KNN ou futuros métodos mais sofisticados) argumentando que, assim como temperatura e umidade no estudo, a radiação é fisicamente central e pode ser reconstruída com erros pequenos quando há forte estrutura temporal e correlações adequadas.

4. Discutir explicitamente que, diferentemente do artigo (10% de missing), algumas variáveis nossas têm missing muito mais severo (como radiação global), o que torna ainda mais importante reportar resultados com e sem imputação, e tratar a imputação de radiação como um experimento metodológico e não como “verdade de referência”.

5. Incluir nas conclusões ou trabalhos futuros a possibilidade de substituir/complementar o KNNImputer por abordagens inspiradas neste artigo (modelos em espaço de estados ou regressões temporais) para imputar variáveis climáticas importantes em novas versões do pipeline.
