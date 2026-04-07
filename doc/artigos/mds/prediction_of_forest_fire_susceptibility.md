# Freitas2025-Xingu – Prediction of forest fire susceptibility using machine learning tools in the Triunfo do Xingu Environmental Protection Area, Amazon, Brazil (2025)

## Ficha técnica

Autores: Kemuel Maciel Freitas; Ronie Silva Juvanhol; Christiano Jorge Gomes Pinheiro; Anderson Alvarenga de Moura Meneses.
Periódico: Journal of South American Earth Sciences, v. 153, 2025. 

Área de estudo: Área de Proteção Ambiental (APA) Triunfo do Xingu (Pará, Amazônia).
Período de fogo: 2010–2020 (focos validados).

---

## Objeto do estudo

O objetivo central é mapear a suscetibilidade a incêndios florestais na APA Triunfo do Xingu utilizando algoritmos de aprendizado de máquina (Random Forest e XGBoost), avaliando o papel de fatores ambientais, topográficos e socioeconômicos na ocorrência de fogo. A variável resposta é a densidade de queimadas (kernel density) calculada sobre 15.291 focos de queima validados entre 2010 e 2020. 

Do ponto de vista do TCC, o trabalho é um exemplo direto de uso de modelos de regressão com ensemble de árvores para estimar risco espacial de fogo a partir de dados do BDQueimadas e camadas ambientais.

---

## Dados, preditores e construção da variável-alvo

Os focos ativos foram obtidos do Programa Queimadas/INPE, considerando apenas o satélite de referência AQUA_M-T (1 km), e cruzados com o produto de área queimada do INPE para reter apenas pontos efetivamente associados a cicatrizes de queima (focos “validados”). 

A variável-alvo é a densidade kernel dos focos validados, calculada em QGIS a partir de todos os pontos combinados (2010–2020), com raio de influência definido por estatística de distância média entre pontos e função núcleo quartic, gerando um raster de 250 m. 

Foram usados 11 preditores, agrupados em três blocos:

* Topográficos: altitude, declividade, aspecto, Topographic Wetness Index (TWI) derivados do SRTM (30 m).
* Socioeconômicos: distância a áreas habitadas e a rodovias, obtidas de IBGE e DNIT e transformadas em zonas de buffer.
* Ambientais: uso e cobertura do solo (MapBiomas, coleção 8), Vegetation Continuous Fields (MODIS VCF), NDVI (Landsat 7/8 com pré-processamento e mediana 2010–2020), temperatura de superfície (LST via Landsat), precipitação média anual (CHIRPS). 

Essas escolhas reforçam a ideia de que o risco de fogo é função conjunta de clima (chuva, temperatura), estrutura da vegetação (NDVI, VCF, uso do solo) e pressão antrópica (proximidade de estradas e assentamentos).

---

## Configuração dos modelos de aprendizado de máquina

O problema é tratado como regressão da densidade de queima. Foram ajustados dois modelos: Random Forest (RandomForestRegressor) e XGBoost (XGBRegressor), ambos tratados como ensembles de árvores de decisão. 

O pré-processamento inclui imputação de dados faltantes (média para contínuas, moda para categóricas) e codificação inteira (“integer encoding”) para variáveis categóricas, mantendo o número de colunas controlado. Os hiperparâmetros foram otimizados por GridSearchCV com validação cruzada k-fold (k = 10). As configurações ótimas foram:

* RF: 200 árvores, profundidade máxima não limitada.
* XGBoost: 250 árvores, profundidade máxima 20, learning rate 0,1. 

O desempenho foi avaliado com MAE, RMSE e R² em validação cruzada e em conjunto de validação hold-out (80% treino/teste, 20% validação). Ambos os modelos alcançaram R² ≈ 0,99 e erros baixos (RMSE ≈ 36), com desempenho muito semelhante; XGBoost foi escolhido pelo menor custo computacional e melhor escalabilidade. 

---

## Resultados centrais

O mapa de suscetibilidade, baseado no XGBoost e classificado por Jenks Natural Breaks em cinco classes, indica que áreas de alta e muito alta suscetibilidade representam cerca de 39% da APA, concentradas principalmente nas porções centro-leste e centro-oeste. 

A análise de importância de variáveis (feature_importances_) e de contribuição via SHAP mostra:

* Precipitação média anual como variável dominante (≈70% da importância global), com contribuição negativa para a suscetibilidade (mais chuva → menor risco).
* Distância a áreas habitadas e uso e cobertura do solo como preditores socioeconômicos críticos: regiões mais próximas a áreas povoadas e dominadas por pastagem concentram maior suscetibilidade.
* Altitude, temperatura, distância a rodovias, NDVI e VCF têm influência menor, mas ainda contribuem ao modelo, enquanto TWI, declividade e aspecto são praticamente irrelevantes neste setup específico. 

Comparações entre classes de suscetibilidade indicam que áreas muito suscetíveis são mais quentes, ligeiramente mais secas, mais próximas a rodovias e dominadas por pastagens; áreas pouco suscetíveis estão associadas a maior precipitação, temperatura levemente menor e maior continuidade de floresta densa. 

---

## Pontos para fundamentação teórica do TCC

1. **Validação de ensembles baseados em árvores para risco de fogo**
   O estudo demonstra que Random Forest e XGBoost alcançam alto poder preditivo para suscetibilidade de incêndios, com R² próximo de 1 em ambiente real, após calibração cuidadosa e validação cruzada robusta. Isso fortalece teoricamente a escolha de modelos de árvore de decisão e ensembles no contexto do Cerrado.

2. **Integração de BDQueimadas com múltiplas camadas ambientais e antrópicas**
   A construção da variável-alvo a partir de focos validados (BDQueimadas + produto de área queimada) e o uso combinado de dados climáticos (CHIRPS), topográficos (SRTM), de vegetação (Landsat, MODIS, NDVI, VCF) e de uso do solo (MapBiomas) exemplificam a arquitetura de dados que o TCC busca replicar, porém focada no Cerrado e com outra formulação de alvo (ocorrência/probabilidade).

3. **Relevância de clima e pressão antrópica como preditores-chave**
   A evidência de que precipitação, proximidade a centros habitados e tipo de uso do solo são as variáveis mais importantes reforça que modelos de aprendizado de máquina para fogo devem combinar clima (chuva, temperatura) com proxies de ação humana, e não apenas variáveis meteorológicas isoladas.

4. **Perspectiva de suscetibilidade espacial versus previsão espaço-temporal**
   O artigo trabalha com densidade kernel agregada 2010–2020 e produz um mapa estático de suscetibilidade espacial. Para o TCC, isso fornece uma base teórica para abordar o problema em escala de pixel/grade, mas abre espaço para avançar em direção a modelos espaço-temporais no Cerrado (por exemplo, previsão diária ou mensal de ocorrência de foco).

5. **Lacunas assumidas pelo próprio artigo**
   Os autores apontam que o uso de dados com melhor resolução espacial e temporal tende a aprimorar os modelos e que há espaço para integrar novas técnicas de inteligência artificial e deep learning.  Isso se alinha diretamente com a proposta do TCC de explorar aprendizado de máquina em outro bioma crítico (Cerrado), com recorte e alvo próprios, aproveitando lições de configuração de modelo, seleção de variáveis e interpretação de importância.

Em síntese, Freitas2025-Xingu é uma referência chave para ancorar, na fundamentação teórica, o uso de ensembles de árvores (Random Forest, XGBoost) e de variáveis climáticas, ambientais e socioeconômicas na modelagem de risco de fogo, ao mesmo tempo em que evidencia a lacuna de estudos preditivos similares aplicados especificamente ao Cerrado.
