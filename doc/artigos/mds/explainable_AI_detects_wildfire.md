# Cilli22-MedXAI — Explainable artificial intelligence (XAI) detects wildfire occurrence in the Mediterranean countries of Southern Europe (2022)

Autores: Roberto Cilli; Mario Elia; Marina D’Este; Vincenzo Giannico; Nicola Amoroso; Angela Lombardi; Ester Pantaleo; Alfonso Monaco; Giovanni Sanesi; Sabina Tangaro; Roberto Bellotti; Raffaele Lafortezza. Local/Periódico/Conferência: Scientific Reports (Nature Portfolio). Link/DOI: 10.1038/s41598-022-20347-9.

## Ficha técnica

Objeto/Região/Período: Itália peninsular, 2007–2017, grade 2 km.
Tarefa/Alvo: previsão de ocorrência (binária) em mapa de suscetibilidade.
Variáveis: 16 preditores: FWI (média sazonal JJA), NDVI (média sazonal), classes Corine Land Cover como contínuas (7), densidade de cobertura arbórea, declividade, elevação (DTM), distância a assentamentos/rodovias/ferrovias, densidade populacional.
Modelos: Random Forest (h2o) com XAI (SHAP) e importância por permutação (PIMP); análise espacial de hotspots (Getis‑Ord G*).
Dados & Split: 75.298 pontos de grade (2 km); rótulo 1 se houve ao menos uma ignição no período; validação 5‑fold espacial por células de ~5000 km², estratificada, repetida 100x; subsampling para balancear classes no treino; OOB sem bloqueio espacial para comparação.
Métricas: Acc, F1, Sens, Prec, Spec, AUC.
Pré-processamento: representação contínua de classes CLC; médias sazonais de FWI/NDVI; subsampling de treino; teste de significância de importâncias via permutação (vita/PIMP).
Código/Reprodutibilidade: R 3.6.3 + h2o 3.34.0.3; mapas em QGIS; dados públicos sob solicitação; sem repositório de código.

## Trechos literais do artigo

> “a first attempt to provide an XAI framework for estimating wildfire occurrence using a Random Forest model with Shapley values” (Cilli et al., 2022).
> “a 2‑km resolution map of the ground truth” (Cilli et al., 2022).
> “We designed a stratified 5‑fold cross‑validation (CV) strategy to remove spatial correlation” (Cilli et al., 2022).
> “The most important features were FWI, ‘Forest’ class, Slope and DTM.” (Cilli et al., 2022).
> “All variables except the ‘Wet’ class were significant (p‑value < 0.01).” (Cilli et al., 2022).
> “Classification performances in terms of AUC and accuracy were 81.3 and 69.7%” (Cilli et al., 2022, Table 1).
> “precision of 50.9% […] high sensitivity denotes the effectiveness of the method in detecting wildfires.” (Cilli et al., 2022, Table 1).
> “the Shapley base value (Offset) […] the mean probability value on the training examples (0.53).” (Cilli et al., 2022).

## Leitura analítica e crítica

Metodologia: Estudo de ocorrência com forte ênfase em validação espacial e interpretabilidade. A construção do rótulo binário em janela decenal (1 se ocorreu ao menos uma ignição) reduz o desbalanceamento, mas agrega temporalmente e pode diluir variabilidade interanual. O bloqueio espacial por 55 células reduz vazamento espacial; ainda assim, admitem fronteiras com possível correlação residual (~2% dos pixels de validação nas bordas). O RF foi mantido simples (≈50 árvores, mtry padrão), contrastando OOB vs CV espacial para explicitar o viés de vizinhança. Importâncias avaliadas por PIMP com teste de significância; explicabilidade local e global via SHAP. A decisão por subsampling no treino iguala classes e privilegia sensibilidade; seria útil comparar com class weights.

Resultados: Com CV espacial, AUC 81,3% e Acc 69,7%; F1 cai para 62,0%, refletindo desafio na classe positiva. Precisão 50,9% indica metade dos alertas como falsos positivos, mas Sens 78,7% é relativamente alta. OOB melhora modestamente AUC para 84,1%, quantificando o otimismo quando não há bloqueio espacial. FWI domina a explicação global; CLC‑Forest, Slope e DTM vêm a seguir; Wet não significativo. SHAP mostra heterogeneidade local: fatores distintos podem liderar em áreas específicas, e o offset de 0,53 revela prevalência efetiva maior após agregação.

Limitações: Rotulagem decenal e resolução 2 km podem mascarar dinâmicas sazonais e levar a hotspotting histórico. Subsampling altera prevalência e pode inflar sensibilidade em detrimento da precisão. Não há validação temporal hold‑out; a CV é espacial, não espaço‑temporal. As variáveis climáticas resumem apenas JJA; outros regimes sazonais italianos (invernos no norte) foram reconhecidos, mas a modelagem não incorpora janelas defasadas. Ausência de PR‑AUC dificulta leitura sob desbalanceamento. Código não reprodutível publicamente.

Qualidade: Contribuição metodológica clara ao explicitar o ganho de honestidade com CV espacial e ao incorporar XAI (PIMP+SHAP). Relato estatístico robusto com IC95% por repetição da CV. Transparência boa sobre variáveis e pipeline; abertura de código/dados é parcial.

## Relação com o TCC

Relevância: alta.
Por que importa: Alinha‑se ao nosso objetivo de prever ocorrência com variáveis climáticas e contexto antrópico, demonstrando CV espacial, IC por repetição e XAI via SHAP. Os achados reforçam o papel central de índices meteorológicos (FWI) e de topografia/uso do solo, o que é transferível ao Brasil com INMET + BD Queimadas.

## Tabela resumida

| Item                | Conteúdo                                                                                                           |
| ------------------- | ------------------------------------------------------------------------------------------------------------------ |
| Variáveis           | FWI (JJA), NDVI (JJA), CLC contínuas (7), TCD, Slope, DTM, distâncias a assent./rod./ferrov., densidade pop.       |
| Modelos             | Random Forest; XAI com SHAP; PIMP para significância                                                               |
| Validação           | 5‑fold espacial por células, repetida 100x; comparação com OOB                                                     |
| Métricas principais | AUC 81,3%; Acc 69,7%; F1 62,0%; Sens 78,7%; Prec 50,9%                                                             |
| Melhor desempenho   | RF com CV espacial; FWI como driver dominante                                                                      |
| Limitações          | Sem PR‑AUC; prevalência alterada por subsampling; rótulo decenal; sem hold‑out temporal; reprodutibilidade parcial |

## Itens acionáveis para o TCC

1. Implementar **validação por blocos espaciais** com repetição e IC, além de um **hold‑out temporal** para anos recentes; medir o gap entre OOB/k‑fold aleatório e CV espacial.
2. Reportar **PR‑AUC** e otimizar **thresholds por custo** (falsos positivos vs falsos negativos), incluindo curvas Precision‑Recall e custo operacional.
3. Adotar **SHAP** para explicabilidade global/local; testar se FWI diário ou acumulados (e.g., SPEI/Keetch‑Byram) e topografia elevam PR‑AUC no BD Queimadas; avaliar impacto de **class weights** vs **subsampling**.

---
