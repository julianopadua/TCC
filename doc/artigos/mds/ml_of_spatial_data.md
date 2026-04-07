# Machine Learning of Spatial Data (ISPRS IJGI, 2021)

## Ficha técnica

Autores: Behnam Nikparvar; Jean-Claude Thill.
Tipo: Revisão (survey).
Alvo: propriedades de dados espaciais e como tratá-las em ML; duas trilhas: (i) incorporar propriedades no **matriz de observação**; (ii) incorporar **no algoritmo de aprendizado**.
Escopo: dependência espacial, heterogeneidade espacial, escala; amostragem espacial; features espaciais; redução de dimensionalidade; dados faltantes; DT/RF geograficamente ponderados; SVM com campos aleatórios condicionais; CNN, GNN, SOM, RBF/ART; temas espaciotemporais; MAUP/UGCoP; validação e generalização.



---

## Trechos centrais (literais, sem paráfrase inicial)

> “Spatial data exhibit certain distinctive properties that set them apart… such as **spatial dependence, spatial heterogeneity, and scale**.” 

> “Failure to appropriately include these properties into the ML model can **negatively impact learning**.” 

> “We recognize two broad strands… properties **developed in the spatial observation matrix**… [or] **handled in the learning algorithm itself**.” 

> “The **first law of geography**… ‘near things are more related than distant things’… spatial autocorrelation… Moran’s I, Geary’s C; semivariogram.” 

> “**Spatial heterogeneity**… violations of stationarity… anisotropy… processes operating at different scales.” 

> “**Scale**… MAUP and **UGCoP** can alter conclusions even with o resto mantido constante.” 

> “**Spatial sampling** matters… oversampling espacial inflaciona acurácia aparente; **intra-class imbalance** não é resolvido por CV comum.” 

> “Features espaciais: coordenadas; efeitos fixos regionais; **textura, métricas de forma e contexto**; **Gabor/Wavelets**; segmentação por objetos.” 

> “**GWR/GW-RF** para não-estacionaridade; **SVM + CRF** para dependência; **CNN** automatiza extração de vizinhança; **GNN** para redes irregulares.” 

> “Para dados faltantes… **kriging**; **PPCA** e extensões espaciotemporais.” 

> “Avaliação deve considerar **acurácia espacial** além da acurácia de classificação.” 

---

## Leitura crítica e por que isso importa para o seu TCC

O artigo é **altamente relevante** como base metodológica para sua “Análise Comparativa de Modelos de ML na Previsão de Queimadas com Variáveis Climáticas”. Ele não traz métricas de incêndio em si, mas oferece **princípios operacionais** que reduzem risco de vieses e **fortalecem a validade externa** dos seus resultados:

1. **Dependência espacial**
   Para previsão diária de focos, a autocorrelação é regra, não exceção. Se você usar **validação aleatória** (cv k-fold comum), haverá **vazamento espacial**: treino e teste compartilham vizinhança, inflando AUC/F1. Diretriz acionável: adotar **cross-validation por blocos espaço-temporais** (folds por células/meses ou por tiles contíguos). Isso casa com a crítica do artigo ao uso de CV padrão em dados espaciais e com a sua meta de comparação honesta entre RF/XGB/SVM/MLP.

2. **Heterogeneidade espacial (não-estacionaridade)**
   A relação entre clima e ignição varia por **bioma, estação e regime de uso do solo**. O texto recomenda abordagens **locais** ou ponderadas (GWR/GW-RF) quando coeficientes mudam no espaço. Para o seu estudo comparativo, vale:
   a) medir **variação espacial** das importâncias (ex.: mapas de SHAP agregados por célula/mesorregião),
   b) reportar **spread** de desempenho por região (não só média global),
   c) testar **modelos regionais** vs **modelo único nacional**.

3. **Escala, MAUP e UGCoP**
   Escolhas de **resolução (0,05° vs 0,1°)** e de **unidade contextual** mudam resultados. O artigo alerta que **mudanças de zona/agregação** alteram correlações e acurácias. Para seu TCC: fixe a grade a priori, justifique com base em densidade de estações INMET e footprint dos sensores MODIS/VIIRS, e **faça um experimento de sensibilidade de escala** (pelo menos duas resoluções) para mostrar robustez.

4. **Amostragem e desbalanceamento**
   O texto aponta **intra-class imbalance espacial** e amostragens redundantes como fontes de otimismo. Diretiva:
   a) amostrar **negativos** (sem foco) de modo **estratificado no espaço e no tempo**,
   b) manter razão positiva:negativa realista por célula/tempo,
   c) usar métricas sensíveis a desbalanceamento (PR-AUC, curva de detecção precoce).

5. **Feature engineering espacial**
   A revisão sugere: além de clima diário, criar **features contextuais** consistentes com a vizinhança: agregações móveis (janelas temporais para chuva e VPD), **texturas** (variância espacial local de Tmax/UMID), **métricas sazonais** e **índices derivados**. Isso tende a beneficiar **árvores/boosting**—exatamente os modelos que você vai comparar.

6. **Algoritmos “spatial-aware”**
   O artigo recomenda dois caminhos:
   a) manter modelos padrão e **injetar o espaço** nos dados (features + amostragem + validação correta);
   b) usar modelos que **internalizam** o espaço (GW-RF, SVM-CRF, **GNN** quando há rede/adjacência explícita). Para o TCC, sugiro **primariamente (a)**, com um **experimento secundário** explorando **GW-RF** em subset, discutindo pró-e-contras.

7. **Dados faltantes**
   Para lacunas INMET, o texto cita **kriging** e **PPCA**; sua proposta de **KNN espacial** é coerente, mas vale comparar com **kriging** em um recorte e reportar erro de imputação, pois imputação afeta métricas de predição de fogo.

8. **Métricas com componente espacial**
   A revisão lembra que acurácia “tabular” pode ignorar **distância ao evento real**. Inclua um anexo com **erro espacial médio** (ex.: distância do hotspot previsto mais próximo) e mapas de **resíduo espacial**.

---

## Tabela síntese (conceitos → ação no seu TCC)

| Conceito do artigo   | Risco se ignorado                  | Ação concreta no TCC                                      |
| -------------------- | ---------------------------------- | --------------------------------------------------------- |
| Dependência espacial | AUC/F1 inflados por vazamento      | **CV por blocos espaço-temporais**                        |
| Heterogeneidade      | Média global esconde falhas locais | Reportar **desempenho por região/estação**; mapas de SHAP |
| Escala (MAUP/UGCoP)  | Resultados instáveis               | **Experimento de sensibilidade de resolução** e unidade   |
| Amostragem espacial  | “Otimismo” por redundância         | **Amostragem estratificada espaço-temporal**              |
| Features espaciais   | Perda de sinal útil                | **Janelas climáticas + texturas locais + sazonais**       |
| Dados faltantes      | Viés na modelagem                  | Comparar **KNN espacial vs kriging/PPCA**                 |
| Avaliação espacial   | Métrica “cega ao espaço”           | **PR-AUC + métrica de erro espacial**                     |

---

## Julgamento de relevância para o TCC

Este é um **artigo-base metodológico** e **altamente pertinente**. Não traz um dataset de queimadas, mas estabelece **boas práticas** que **devem** constar da sua metodologia: desenho de amostragem, validação, engenharia de atributos espaciais, tratamento de não-estacionaridade e de escala. Recomendo citá-lo nas seções de **Metodologia** e **Ameaças à Validade**.
