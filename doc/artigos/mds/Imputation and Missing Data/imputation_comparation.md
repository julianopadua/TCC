# Chehal23-CompImpute - Comparative Study of Missing Value Imputation Techniques on E-Commerce Product Ratings (2023)

Autores: Dimple Chehal; Parul Gupta; Payal Gulati; Tanisha Gupta.
Periódico: Informatica, v.47, n.3, p.373–382, 2023. 
Tema geral: comparação empírica de técnicas de imputação de dados faltantes em ratings de produtos, com discussão conceitual de mecanismos e padrões de missing.

---

## Ficha técnica

Objeto/região/período:
Base pública de reviews de celulares e acessórios da Amazon (5-core), sem recorte espacial explícito; amostra reduzida para 90 714 linhas e 12 colunas. 

Tarefa/alvo:
Imputar valores faltantes na coluna de rating (Overall) e comparar métodos de imputação com base em erros de reconstrução (não é um modelo preditivo final, e sim um experimento controlado de imputação). 

Variáveis usadas no experimento:
Ratings (Overall), flag Verified, número de votos (Vote); as demais colunas existem mas o foco da análise são essas três.

Métodos comparados:
SimpleImputer (média/mediana), KNNImputer, Hot Deck, Regressão Linear, MissForest, Random Forest Regression, DataWig (deep learning) e MICE (Multivariate Imputation by Chained Equations). 

Desenho do experimento:

1. Dataset original não tinha missing; os autores removem aleatoriamente 4 por cento dos valores da coluna Overall sob um mecanismo MCAR.
2. Aplicam cada técnica para reconstruir a coluna.
3. Comparam o valor imputado com o valor verdadeiro simulado, usando R2, MSE e MAE. 

Métricas:
R2 (quanto mais próximo de 1 melhor), MSE e MAE (quanto menores, melhor).

Resultados principais:
Hot Deck tem melhor desempenho geral em MSE e MAE e R2 muito alto; KNN tem R2 elevado mas MAE ligeiramente maior; MissForest apresenta os piores erros (MSE e MAE muito altos, R2 levemente negativo). 

Tratamento conceitual:
O artigo revisa mecanismos MCAR, MAR e NMAR, bem como padrões de missing (univariado, monotônico, não monotônico) e discute prós e contras de imputação simples versus múltipla. 

Código e reprodutibilidade:
Uso de Python e bibliotecas comuns (scikit-learn, missingpy, DataWig); não há repositório de código público, mas o fluxo é descrito de forma razoável.

---

## Trechos literais do artigo

Os trechos abaixo são recortados com menos de 25 palavras cada.

> “Missing values in a dataset mean loss of important information. These are values that are not present in the dataset and are written as NANs, blanks, or any other placeholders.” 

> “Missing value creates imbalanced observations, biased estimates and in some cases can direct to misleading results.” 

> “Imputation on the other hand is the process of identifying missing values and interchanging them with a substitute value.” 

> “Based on the findings KNN had the best outcomes, while DataWig had the worst results for R-squared error.” 

> “The Hot Deck imputation approach seems to be of interest and should be investigated further in practice.” 

---

## Leitura analítica e crítica

Metodologia:
O trabalho é um estudo de simulação controlada. Como o dataset original não tinha faltantes, os autores introduzem artificialmente 4 por cento de missing apenas na coluna de rating e assumem MCAR. Isso permite conhecer o valor verdadeiro e medir exatamente o erro de cada técnica de imputação. O foco é puramente numérico: qual método reconstrói melhor uma variável numérica contínua, quando a fração de missing é pequena e aleatória.

A revisão conceitual é útil: eles distinguem entre deleção (listwise, pairwise, exclusão de atributos) e imputação, lembrando que a simples exclusão de linhas pode enviesar estimativas e reduzir muito a amostra. Também sistematizam imputaçao simples versus múltipla, destacando que múltipla imputação captura a incerteza estatística, mas é mais custosa e complicada de implementar no dia a dia. 

Resultados:
Num cenário idealizado (MCAR, 4 por cento de missing em uma única coluna), praticamente todos os métodos, exceto MissForest e DataWig, performam bem. Hot Deck atinge MSE e MAE próximo de zero e R2 muito próximo de 1; KNN, SimpleImputer, Random Forest Regression e MICE têm erros de mesma ordem de grandeza. Linear Regression e DataWig apresentam desempenho intermediário, sugerindo que regressão linear simples e redes neurais mais pesadas não garantem melhor imputação nesse tipo de variável. 

O argumento implícito é: quando a estrutura de correlação entre variáveis é relativamente simples e o padrão de missing é MCAR, métodos simples ou baseados em vizinhança (Hot Deck, KNN) podem ser tão bons ou melhores que algoritmos mais complexos. Isso dialoga com a preocupação do nosso TCC de não superestimar o ganho de técnicas sofisticadas de imputação em relação a estratégias mais transparentes.

Limitações:
A extrapolação para outras áreas, como climatologia, é limitada por vários motivos:

* O padrão de missing foi imposto como MCAR; em dados climáticos e de queimadas é mais plausível MAR ou NMAR (falhas sistemáticas de sensores, buracos de série inteiras em estações).
* Só uma coluna é imputada, e a imputação usa basicamente duas variáveis explicativas; no nosso caso, as relações entre variáveis climáticas são mais fortes e estruturadas.
* A fração de missing é pequena (4 por cento), enquanto, no nosso TCC, a coluna de radiação global pode chegar a mais de 50 por cento de ausências em alguns anos.
* Não há avaliação downstream: os autores não verificam o efeito da imputação no desempenho de um modelo final de recomendação, apenas na capacidade de reconstruir a coluna original.

Mesmo assim, como revisão e experimento de referência, o artigo reforça a ideia de que:

1. Imputar muda a distribuição e pode melhorar ou piorar análises posteriores.
2. É necessário comparar técnicas em termos de erro de imputação, não apenas supor que métodos complexos são melhores.

---

## Relação com o TCC

Relevância: moderada a alta, sobretudo no embasamento de como tratar missing e por que construir múltiplos cenários de base com e sem imputação.

O que o artigo nos dá de base conceitual para o TCC:

1. Justificativa geral para não ignorar o problema de missing
   O artigo enfatiza que dados faltantes geram estimativas enviesadas, perda de informação e até conclusões enganosas. Isso fundamenta a decisão de olhar explicitamente para sentinelas e NaN na nossa base INMET + BDQueimadas, em vez de simplesmente deixar o modelo lidar com isso. 

2. Apoio teórico para separar cenários com e sem imputação
   Eles mostram que diferentes técnicas de imputação podem produzir resultados muito distintos em termos de erro (MissForest chega a um MSE várias ordens de magnitude maior que Hot Deck). Isso legitima a nossa decisão metodológica de construir:

   * bases que apenas padronizam missing, sem imputar (cenários base_F_full_original e base_A_no_rad)
   * bases que imputam via KNN (base_B_no_rad_knn e base_E_with_rad_knn)
     e comparar o impacto da imputação no desempenho de modelos de previsão de queimadas.

3. Imputação seletiva e foco em variáveis críticas
   O trabalho imputa apenas a coluna de rating porque é a variável central para o problema de recomendação. No nosso caso, algo análogo é tratar a radiação global como variável especialmente delicada: ela é climaticamente importante, mas sofre com percentuais de missing muito altos. O artigo apoia a ideia de testar cenários em que:

   * a radiação global é removida por completo (bases sem radiação), evitando imputações potencialmente agressivas em uma coluna muito incompleta;
   * a radiação é imputada de forma informada (KNN, talvez com vizinhança climática), para avaliar se a recuperação parcial da variabilidade de radiação melhora o poder preditivo de queimadas.

4. Justificativa para escolha de KNN como baseline de imputação
   Entre os métodos testados, KNN tem desempenho bastante competitivo, com R2 alto e erros baixos, além de ser conceitualmente simples e razoavelmente interpretável. Isso corrobora o uso de KNNImputer como uma das estratégias de imputação nas nossas bases de modelagem, em vez de partir diretamente para métodos mais opacos como MissForest ou redes neurais. 

5. Cuidado com pressupostos de mecanismo de missing
   Embora o artigo assuma MCAR, ele discute MCAR, MAR e NMAR. No nosso TCC, podemos usar essa taxonomia para argumentar que:

   * para muitas variáveis meteorológicas, o missing provavelmente não é MCAR
   * por isso, é metodologicamente honesto manter cenários sem imputação (drop de linhas ou uso de NaN com modelos que lidam bem com isso) ao lado de cenários imputados, sem apresentar a imputação como verdade única.

Em resumo, Chehal23 reforça a ideia de que imputação é uma escolha de modelagem que precisa ser explorada e comparada, não assumida como etapa obrigatória. Isso se alinha diretamente com a estratégia do TCC de gerar múltiplas bases de modelagem com e sem imputação, especialmente em torno da variável de radiação global.

---

## Tabela resumida

| Item                | Conteúdo                                                                                                        |
| ------------------- | --------------------------------------------------------------------------------------------------------------- |
| Domínio             | Ratings de produtos de e commerce (Amazon celulares)                                                            |
| Variável alvo       | Coluna de rating Overall com 4 por cento de missing simulados                                                   |
| Técnicas comparadas | SimpleImputer, KNN, Hot Deck, Regressão Linear, MissForest, Random Forest Regression, DataWig, MICE             |
| Desenho             | Missing MCAR em uma coluna, imputação e comparação com valores verdadeiros usando R2, MSE, MAE                  |
| Melhor desempenho   | Hot Deck (melhor MSE e MAE, R2 elevado) e KNN com desempenho próximo                                            |
| Pior desempenho     | MissForest (MSE e MAE muito altos, R2 negativo), DataWig e Regressão Linear intermediários                      |
| Principais insights | Métodos simples ou de vizinhança podem superar modelos complexos; importância de entender mecanismos de missing |

---

## Itens acionáveis para o TCC

1. Manter explicitamente os cenários sem imputação
   Documentar no capítulo de metodologia que, à luz de estudos comparativos de imputação, optamos por manter bases onde apenas padronizamos missing (sem imputar) para servir de baseline honesto.

2. Justificar KNNImputer como escolha principal de imputação numérica
   Basear a escolha em evidências da literatura de que KNN costuma ter bom compromisso entre simplicidade e desempenho, como mostrado em Chehal23, e usá-lo tanto para bases sem radiação quanto para o cenário com radiação imputada.

3. Tratar radiação global como caso especial
   Argumentar que, devido ao altíssimo percentual de missing, a radiação global entra em dois blocos de experimentos:

   * remoção completa, para avaliar modelos apenas com variáveis mais completas
   * imputação via KNN, para testar se a recuperação da radiação melhora a previsão de queimadas, assumindo um mecanismo mais próximo de MAR.

4. Explicitar no texto os mecanismos de missing
   Adaptar a taxonomia MCAR, MAR e NMAR para o contexto INMET e BDQueimadas, mostrando que a nossa estratégia de construir várias bases é uma resposta direta à incerteza sobre o mecanismo de missing.

5. Conectar a auditoria de missing com a escolha de imputação
   Usar os resultados de dataset_missing_audit.py para justificar quando vale a pena imputar (colunas com porcentuais moderados) e quando a melhor decisão é descartar a variável ou trabalhar sem imputação (como alguns anos de radiação global).
