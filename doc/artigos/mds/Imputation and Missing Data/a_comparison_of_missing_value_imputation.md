# Navarro23-MexPrecip - A comparison of missing value imputation methods applied to daily precipitation in a semi-arid and a humid region of Mexico (2023)

Autores: Juan Manuel Navarro Céspedes et al.
Local/Periódico/Conferência: Atmósfera, vol. 37, 2023.
Link/DOI: 10.20937/ATM.53095. 

## Ficha técnica

Objeto/Região/Período:
Duas regiões climáticas do México: bacia do Alto Rio Laja (semiárida, estado de Guanajuato) e estado de Tabasco (região úmida). Séries de precipitação diária: 1993–2017 (semiárida) e 1980–2012 (úmida). 

Tarefa/Alvo:
Imputar valores faltantes em séries de precipitação diária e comparar métodos quanto ao erro médio absoluto (MAE) e à preservação de características estatísticas e de homogeneidade das séries. 

Variável climática:
Precipitação diária em estações pluviométricas, com análise de duas regiões com regimes e orografia distintos, avaliando efeito da altitude, regime de precipitação e porcentagem de faltantes sobre o MAE. 

Métodos de imputação comparados:
Família de métodos de ponderação espacial e por correlação (NR, NRWC, IDW, CCW, MCCW, CIDW, NRIDW, NRIDC, HIDW, GNRIDW, GCIDW), além de métodos mais gerais: MICE (PMM), ReddPrec, EM, regressão linear (RG). 

Dados e desenho experimental:
Exclusão de estações com mais de 25 por cento de dados faltantes; uso de estações alvo e auxiliares na região semiárida, mas apenas estações alvo em Tabasco. Avaliação dos métodos via remoção artificial de valores e comparação estimado versus observado com MAE. 

Métricas principais:
MAE como métrica central (preferida a RMSE por menor sensibilidade a outliers de precipitação); comparação de médias, máximos, mínimos e desvios padrão imputados versus observados; análise de correlação de Spearman entre MAE, desvio padrão, porcentagem de faltantes etc. 

Homogeneidade:
Avaliação da homogeneidade das séries por SNHT, Buishand range test e teste de Pettitt, classificando estações em confiáveis, moderadamente confiáveis ou suspeitas conforme quantos testes rejeitam a hipótese nula de homogeneidade. 

## Trechos literais do artigo

> “Climatological data with unreliable or missing values is an important area of research, and multiple methods are available to fill in missing data.” 

> “The climate variable used for the analysis was daily precipitation.” 

> “We considered two different climatic and orographic regions to evaluate the effects of elevation, precipitation regime, and percentage of missing data.” 

> “In the semi-arid region, ReddPrec and GCIDW were the best-performing methods with average MAE values of 1.63 and 1.46 mm/day, respectively.” 

> “In the humid region, GCIDW was optimal in about 59% of stations, EM in about 24%, and ReddPrec in about 17%.” 

> “EM and RG methods imputed negative values, which do not correspond to the physics of precipitation phenomena.” 

> “This research makes a valuable contribution to identifying the most appropriate methods to impute daily precipitation in different climatic regions of Mexico.” 

## Leitura analítica e crítica

Metodologia:
O artigo foca em uma comparação sistemática de métodos de imputação de dados faltantes especificamente para precipitação diária, em dois contextos climáticos contrastantes (semiárido e úmido). Primeiro, os autores selecionam séries com no máximo 25 por cento de dados faltantes, removendo estações mais problemáticas antes mesmo de pensar em imputação. Em seguida, definem um conjunto de métodos explicitamente desenhados para chuva, baseados em combinações de distância, correlação e altitude (como GCIDW e GNRIDW), além de métodos mais gerais de imputação multivariada (MICE), regressão e EM. O desenho experimental consiste em mascarar artificialmente observações em períodos onde não há faltantes, imputá-las com cada método e comparar os valores estimados com os observados via MAE e estatísticas descritivas (média, máximo, desvio padrão). Essa abordagem permite avaliar o “custo” de cada método em termos de erro e distorção estatística, isolando o efeito da técnica do efeito da estrutura de faltantes real. 

Resultados:
Na região semiárida (CARL), os métodos de ponderação espacial/correlação se destacam: ReddPrec é ótimo em 9 de 23 estações e GCIDW em 8, com MAE médio em torno de 1,5 a 1,6 mm/dia. Métodos mais simples como NR também conseguem ser ótimos em alguns casos, enquanto EM, RG e outros têm desempenho claramente inferior, especialmente porque EM e RG chegam a gerar valores negativos de precipitação, fisicamente impossíveis, que precisaram ser truncados para zero em análises posteriores. A análise de Spearman mostra que quanto maior a porcentagem de faltantes e o desvio padrão da precipitação, maior o MAE e maior a divergência entre métodos, o que reforça a ideia de que imputar em séries muito “ruidosas” ou muito incompletas é intrinsecamente mais arriscado. 

Na região úmida (Tabasco), com regime pluviométrico mais intenso e poucas estações auxiliares, a hierarquia muda: GCIDW é o método ótimo em cerca de 59 por cento das estações, EM em 24 por cento e ReddPrec em 17 por cento. O desempenho médio de GCIDW ainda é o melhor (MAE ≈ 6 mm/dia), mas chama atenção o fato de ReddPrec piorar bastante nesse contexto, com MAE médio próximo de 9,8 mm/dia, e de EM, mesmo com problemas de valores negativos, competir em vários casos. Ou seja, a eficácia da imputação depende fortemente do regime de precipitação, da densidade da rede e da disponibilidade de estações auxiliares. 

Limitações:
Os autores se concentram apenas em precipitação e em duas regiões mexicanas, de modo que a extrapolação direta para outras variáveis climáticas (por exemplo, temperatura, radiação solar) é conceitualmente plausível, mas não testada empiricamente. A métrica central é o MAE; RMSE é discutido, mas rejeitado por alta sensibilidade a outliers, sem inclusão de métricas mais específicas para extremos de chuva (por exemplo, erros em quantis altos). A análise também evidencia problemas sérios de métodos como EM e RG (valores negativos, alteração de média e desvio), mas não explora em detalhe o impacto disso em modelos de impacto subsequentes, como cheias ou secas. Ainda assim, a discussão é honesta sobre o fato de que imputação não é neutra: pode alterar distribuição, homogeneidade e até a classificação de estações como confiáveis. 

Qualidade:
O estudo é metodologicamente sólido, com descrição clara dos métodos e dos pesos de distância, correlação e altitude; apresenta tabelas extensas com MAE por estação e método, e discute graficamente como estatísticas descritivas se deformam após imputação. A combinação entre avaliação de desempenho (MAE) e avaliação de homogeneidade (SNHT, Buishand, Pettitt) adiciona uma camada importante: não basta imputar com baixo erro médio se a série se torna inhomogênea ou fisicamente estranha. O artigo, portanto, funciona como uma espécie de “guia crítico” de imputação para hidrometeorologia diária. 

## Relação com o TCC

Relevância: muito alta para a parte de dados faltantes e justificativa das nossas decisões de modelagem.

1. Justificativa para não imputar agressivamente todas as variáveis
   O artigo mostra que mesmo métodos relativamente sofisticados podem introduzir artefatos graves, como precipitação negativa ou superestimação sistemática de extremos, especialmente em séries com alta porcentagem de faltantes. Isso dialoga diretamente com a nossa escolha de construir cenários de base sem imputação (por exemplo, bases que apenas convertem sentinelas para NaN e eventualmente removem linhas com missing, em vez de sempre preencher tudo). O trabalho reforça que é cientificamente defensável, e até recomendável, trabalhar com cenários “sem imputação” como linha de base honesta, em vez de assumir que preencher automaticamente sempre melhora o dado. 

2. Critério de corte por porcentagem de missing
   Os autores removem estações com mais de 25 por cento de dados faltantes antes de comparar métodos. Essa prática dá suporte direto à nossa decisão de, no TCC, tratar variáveis extremamente incompletas (como radiação global, com porcentagens de faltantes da ordem de 45–55 por cento em vários anos) como casos especiais: em vez de confiar cegamente em imputação, construímos cenários em que a radiação é simplesmente excluída da base, e só em cenários específicos aplicamos imputação (por exemplo, KNN) para estudar se há ganho marginal de desempenho. 

3. Escolha de métodos e variáveis a imputar
   O artigo mostra que métodos genéricos (EM, regressão) podem ser perigosos para precipitação, enquanto métodos desenhados para o problema (GCIDW, ReddPrec) preservam melhor a estrutura. No nosso TCC, isso respalda duas decisões:
   a) Não imputar variáveis de alvo ou diretamente ligadas à física do foco (por exemplo, FRP, risco de fogo), pois não dispomos de um método especializado para isso e o risco de distorção é alto.
   b) Quando considerarmos imputar radiação global ou outras variáveis climáticas para certos cenários de modelagem, precisamos reconhecer abertamente que KNNImputer é um método relativamente genérico, semelhante em espírito a MICE/EM, e por isso os resultados desses cenários devem ser interpretados com cautela, sempre comparados a um cenário “sem imputação”.

4. Métrica de avaliação dos cenários de imputação
   A adoção de MAE como métrica central pelos autores, com uma discussão explícita sobre a sensibilidade do RMSE a extremos, é um argumento adicional para, no nosso TCC, avaliar o impacto de estratégias de tratamento de missing em termos de métricas robustas (MAE para imputação e métricas como AUC-PR na modelagem). Em particular, se construirmos algum experimento de imputação interna (por exemplo, simular missing e medir erro), podemos usar MAE como métrica principal, alinhado à literatura. 

5. Ligação direta com o desenho das nossas bases A, B, C, D, E, F
   O fato de o artigo mostrar cenários com e sem uso de estações auxiliares, com diferentes níveis de missing, conversa bem com a nossa estratégia de construir várias bases de modelagem:

* bases sem radiação, sem imputação (análogas a “não usar essa estação problemática”);
* bases com radiação, mas removendo linhas com missing (buscando qualidade em detrimento de quantidade);
* bases com imputação via KNN, adicionando complexidade, mas sempre como cenário comparativo.
  Assim, podemos citar este artigo como sustentação teórica para apresentar nossos múltiplos cenários de tratamento de missing como parte do desenho metodológico, e não como “brincadeira de engenharia de atributos”. 

## Tabela resumida

| Item                | Conteúdo                                                                                                                           |
| ------------------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| Variável            | Precipitação diária em duas regiões mexicanas (semiárida e úmida), com séries de décadas de duração                                |
| Métodos             | NR, NRWC, IDW, CCW, MCCW, CIDW, NRIDW, NRIDC, HIDW, GNRIDW, GCIDW, MICE, ReddPrec, EM, RG                                          |
| Validação           | Remoção artificial de dados; comparação estimado versus observado por estação, com MAE e análise de média, máximo e desvio padrão  |
| Métricas principais | MAE como métrica central; discussão crítica de RMSE; correlações de Spearman com % de missing e dispersão                          |
| Melhor desempenho   | Semiárido: GCIDW e ReddPrec; úmido: GCIDW, seguido de EM; métodos genéricos (RG, EM) podem gerar valores negativos                 |
| Limitações          | Foco apenas em precipitação; duas regiões; sem análise de impacto direto em modelos subsequentes; dependência da estrutura de rede |

## Itens acionáveis para o TCC

1. Explicitar em texto que séries ou variáveis com porcentagem muito alta de missing (por exemplo, radiação global) serão tratadas como “estação/variável problemática”, preferindo cenários sem imputação, citando Navarro et al. (2023) como base. 
2. Na seção de metodologia de tratamento de missing, justificar o uso de bases “sem imputação” e de bases com remoção de linhas, usando o argumento de que imputação inadequada pode introduzir valores fisicamente impossíveis ou distorcer média e desvio padrão, como observado com EM e RG para precipitação. 
3. Quando apresentar os cenários com KNNImputer (bases com radiação imputada), mencionar explicitamente que esses cenários devem ser vistos como experimentais, análogos aos métodos gerais analisados no artigo, e que a comparação com as bases sem imputação serve como controle de robustez.
4. Se houver espaço, incluir um parágrafo curto discutindo escolhas de métrica de avaliação de imputação (MAE em vez de RMSE) e conectando isso à escolha de métricas para avaliação dos modelos de previsão de queimadas.
5. Nas conclusões, usar este artigo como parte do argumento de que o TCC não trata imputação como um passo automático, mas como uma decisão metodológica crítica, com cenários explicitamente separados no pipeline para isolar o efeito dessas escolhas.
