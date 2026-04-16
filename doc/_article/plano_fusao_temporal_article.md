# Plano de Fusao Temporal para as Bases do Artigo

**Ultima atualizacao:** 2026-04-16  
**Referencia cruzada:** `doc/_article/dicionario_dados_article.md`  
**Pipeline existente:** `src/feature_engineering_temporal.py` (branch `article-temporal-fusion`)  
**Dados de entrada:** `data/_article/0_datasets_with_coords/{cenario}/inmet_bdq_{ano}_cerrado.parquet`  
**Dados de saida esperada:** `data/_article/1_datasets_with_fusion/{cenario}/{metodo}/` (proposta)

---

## 1. Motivacao

O pipeline de fusao temporal existente foi projetado para um artigo anterior focado exclusivamente em **precipitacao horaria** como serie z principal. Ele gerou features `tsf_*` (previsoes, residuos, embeddings, clusters) que comprovadamente melhoram a classificacao binaria `HAS_FOCO` (Camada B, avaliada via PR-AUC).

As novas bases do artigo (`_article`) adicionam duas camadas de informacao que nao existiam naquele pipeline:

1. **Coordenadas espaciais** (lat/lon de focos e estacoes) — permitem analise espaco-temporal.
2. **Indices de vegetacao MODIS** (NDVI e EVI, buffer e ponto) — biomassa e a variavel mais diretamente ligada ao combustivel disponivel para queimadas.

Estender a fusao temporal para cobrir essas novas variaveis — e para reavaliar as variaveis meteorologicas ja tratadas — e o proximo passo natural antes de treinar XGBoost/Random Forest nos dados do artigo.

### Por que nao treinar direto XGBoost/RF sem fusao temporal?

- Os classificadores tree-based sao bons em capturar interacoes estaticas entre features, mas **nao modelam dependencia temporal** nativamente.
- Features como `tsf_arima_precip_resid` (anomalia da precipitacao em relacao ao baseline ARIMA) ou `tsf_minirocket_f042` (embedding de padrao de janela temporal) codificam explicitamente a **dinamica temporal** que antecede incendios.
- No artigo anterior, cenarios `*_tsfusion` consistentemente superaram cenarios `*_calculated` em PR-AUC.
- Com biomassa agora disponivel, ha uma oportunidade de capturar a interacao temporal **biomassa × meteorologia → fogo** que era invisivel antes.

---

## 2. Inventario dos algoritmos de fusao temporal existentes

### 2.1 Familias de metodo implementadas

O pipeline `src/feature_engineering_temporal.py` implementa 8 familias de metodo, organizadas em 4 categorias:

#### Categoria 1: Suavizacao e defasagens (rapido, vetorizado)

| Metodo | Descricao | Variaveis cobertas | Saida |
|---|---|---|---|
| **ewma_lags** | Media movel exponencial (3 alphas: 0.1, 0.3, 0.8) + lags (1h, 24h, 168h) | precip, temp, umid, rad | `tsf_ewma_{var}_{alpha}`, `tsf_lag_{var}_{lag}h` |

- **Hipoteses temporais:** nenhuma — e puramente descritivo e retroativo.
- **Robustez a NaN:** moderada (NaN propagam via EWMA; lags simplesmente deslocam NaN).
- **Custo computacional:** muito baixo (operacoes pandas vetorizadas).

#### Categoria 2: Modelos ARIMA-family (por cidade, refit periodico)

| Metodo | Tipo | Endogena | Exogenas | Sazonal | Saida |
|---|---|---|---|---|---|
| **arima** | Univariado | cada slug individualmente | — | Nao | `tsf_arima_{slug}_pred`, `tsf_arima_{slug}_resid` |
| **sarima** | Univariado + sazonal | cada slug individualmente | — | Sim (m=24) | `tsf_sarima_{slug}_pred`, `tsf_sarima_{slug}_resid` |
| **arimax** | Multivariado | precip (padrao) | temp, umid, rad | Nao | `tsf_arimax_{endog}_pred`, `tsf_arimax_{endog}_resid` |
| **sarimax_exog** | Multivariado + sazonal | precip (padrao) | temp, umid, rad | Sim (m=24) | `tsf_sarimax_exog_{endog}_pred`, `tsf_sarimax_exog_{endog}_resid` |

**Parametros compartilhados:**
- Ordem ARIMA padrao: `(2, 1, 2)` — diferenciacao de ordem 1 para lidar com nao-estacionariedade.
- Ordem sazonal (SARIMA/SARIMAX): `(1, 1, 1, 24)` — sazonalidade diaria (24 horas).
- Horizonte de refit (`H`): 168 horas (1 semana).
- Janela de treino (`W`): 720 horas (30 dias).
- Divisao temporal: ultimos N anos sao "teste" (N=2 padrao); anos anteriores sao treino.
- **Variaveis registradas** em `ARIMA_VARS_ALL`: `precip`, `temp`, `umid`, `rad` (4 slugs).
- **Politica de exogenas futuras** (ARIMAX/SARIMAX): ultima linha observada repetida H vezes. Documentado como convencao operacional de feature engineering, nao como previsao meteorologica real.

**Hipoteses temporais criticas:**
- ARIMA exige serie com autocorrelacao e estacionariedade (ou diferenciavel). Precipitacao horaria atende razoavelmente.
- SARIMA exige componente sazonal significativo no periodo `m`. Com m=24 (diario), captura ciclo diurno de precipitacao/temperatura.
- Series com muitos zeros consecutivos (ex.: precipitacao em periodo de seca) podem gerar convergencia fragil; o pipeline lida com isso via blocos de skip e logging de falhas.

#### Categoria 3: Modelo aditivo de series temporais

| Metodo | Descricao | Variavel coberta | Saida |
|---|---|---|---|
| **prophet** | Facebook Prophet: regressao com sazonalidades de Fourier + changepoints | precip (Z_PRIMARY) | `tsf_prophet_precip_pred`, `tsf_prophet_precip_resid` |

- **Hipoteses:** funciona melhor com series que tem sazonalidade forte (diaria/semanal). No pipeline atual, esta restrito a precipitacao.
- **Sazonalidade configurada:** diaria=True, semanal=True, anual=False.
- **Custo:** alto (refit com MCMC simplificado a cada bloco de H horas).

#### Categoria 4: Transformacoes de janela / clustering temporal

| Metodo | Descricao | Canais de entrada | Saida |
|---|---|---|---|
| **minirocket** | MiniROCKET (convolucional): transforma janelas de L timesteps em embeddings de kernels aleatorios | precip, temp, umid, rad | `tsf_minirocket_f000` ... `tsf_minirocket_f083` (84 features por padrao) |
| **tskmeans** | TimeSeriesKMeans: agrupa segmentos semanais (168h) da precipitacao em K clusters | precip (Z_PRIMARY) | `tsf_tskmeans_cluster` (inteiro 0..K-1) |

- **MiniROCKET:** janela de 24 horas, 84 kernels. Fit nos anos de treino; transform nos anos de teste. Nao exige sazonalidade parametrica — captura padroes locais de forma agnostica. Exige series sem NaN na janela.
- **TSKMeans:** K=8 clusters, segmentos de P=168 horas (1 semana). Tolerancia de 30% NaN por segmento (substituidos por 0). Codifica "regimes de precipitacao" como feature categorica.

### 2.2 Resumo comparativo

| Familia | Tipo | Vars atuais | Saida principal | Exige sazonalidade? | Robustez a NaN | Custo |
|---|---|---|---|---|---|---|
| ewma_lags | Descritivo | 4 meteo | Suavizacao + lags | Nao | Moderada | Muito baixo |
| arima | Univariado forecast | 4 meteo | Pred + residuo | Nao (diferencia) | Baixa | Medio |
| sarima | Univariado sazonal | 4 meteo | Pred + residuo | Sim (m=24) | Baixa | Alto |
| arimax | Multivariado forecast | 1 endog + 3 exog | Pred + residuo | Nao | Baixa | Medio-alto |
| sarimax_exog | Multivariado sazonal | 1 endog + 3 exog | Pred + residuo | Sim (m=24) | Baixa | Alto |
| prophet | Aditivo Fourier | 1 (precip) | Pred + residuo | Sim (auto) | Moderada | Alto |
| minirocket | Embedding convolucional | 4 meteo canais | 84 embeddings | Nao | Nenhuma (exige janela limpa) | Medio |
| tskmeans | Clustering temporal | 1 (precip) | 1 cluster ID | Nao (capta padroes) | Moderada (30% tol.) | Medio |

---

## 3. Estrategia de fusao temporal para as bases do artigo

### 3.1 Principio orientador: prioridade por impacto em `HAS_FOCO`

A fusao temporal deve ser priorizada nas variaveis que mais contribuem para explicar a ocorrencia de focos. Com base na literatura de incendios no Cerrado, na EDA existente (`src/article/eda.py`) e nas features physics-informed ja calculadas, a hierarquia de prioridade e:

**Tier 1 — Impacto direto alto (aplicar todos os metodos relevantes):**
- **NDVI/EVI (biomassa):** correlacao negativa com `freq_focos` confirmada pela EDA (Pearson/Spearman semanais). Queda no NDVI antecede/coincide com estacao de fogo; e literalmente o combustivel.
- **Precipitacao:** correlacao negativa forte com fogo; serie z classica ja validada no pipeline anterior.

**Tier 2 — Impacto moderado (aplicar ewma_lags + ARIMAX como exogenas):**
- **Temperatura:** correlacao positiva com fogo (calor = secagem); forte sazonalidade diurna.
- **Umidade relativa:** correlacao negativa com fogo; complementar a temperatura. Limiar critico < 15%.
- **Radiacao solar:** driver de evapotranspiracao; sazonalidade clara.

**Tier 3 — Impacto indireto (manter como canal de MiniROCKET, sem modelo ARIMA dedicado):**
- **Vento:** fator de propagacao, nao de ignicao. Ruidoso como serie univariada.
- **Pressao:** variavel de controle; baixa variancia intra-diaria.
- **Features derivadas (precip_ewma, dias_sem_chuva, fator_propagacao):** ja sao elas mesmas features temporais; fusao sobre elas seria redundante.

### 3.2 Estrategia por grupo de variavel

---

#### 3.2.1 Precipitacao (Tier 1, meteorologico)

**Status atual:** totalmente implementada no pipeline existente como serie z principal.

**Recomendacao para o artigo:** reutilizar o pipeline existente sobre as bases `_article`, preservando os mesmos parametros (ordem ARIMA, janela, horizonte, sazonalidade m=24).

**Metodos a aplicar:**

| Metodo | Justificativa | Saida esperada |
|---|---|---|
| ewma_lags | Captura tendencia recente (alphas 0.1/0.3/0.8) e sazonalidades implicitas (lag 24h = ciclo diurno, lag 168h = ciclo semanal) | `tsf_ewma_precip_a01/a03/a08`, `tsf_lag_precip_1h/24h/168h` |
| arima | Baseline univariado; residuo = anomalia de curto prazo | `tsf_arima_precip_pred`, `tsf_arima_precip_resid` |
| sarima | Captura ciclo diurno (m=24) que o ARIMA nao modela; residuo = anomalia descontada sazonalidade | `tsf_sarima_precip_pred`, `tsf_sarima_precip_resid` |
| arimax | Precipitacao condicionada a temp/umid/rad; residuo = componente de chuva nao explicado pelas outras variaveis | `tsf_arimax_precip_pred`, `tsf_arimax_precip_resid` |
| sarimax_exog | ARIMAX + sazonalidade; o modelo mais informativo (mas mais caro e fragil) | `tsf_sarimax_exog_precip_pred`, `tsf_sarimax_exog_precip_resid` |
| prophet | Alternativa ao SARIMA com sazonalidades de Fourier; menos parametrico | `tsf_prophet_precip_pred`, `tsf_prophet_precip_resid` |

**O que esperar como features uteis para `HAS_FOCO`:**
- `tsf_arima_precip_resid` negativo grande → choveu menos que o esperado → risco aumenta.
- `tsf_sarima_precip_resid` negativo → deficit de precipitacao alem do padrao sazonal.
- `tsf_lag_precip_168h` proximo de zero → semana anterior seca → correlacao direta com `dias_sem_chuva`.

---

#### 3.2.2 NDVI / EVI — Biomassa (Tier 1, nova variavel)

**Status atual:** nao implementada no pipeline de fusao temporal. As colunas `NDVI_buffer`, `NDVI_point`, `EVI_buffer`, `EVI_point` existem nos parquets `_article` mas nunca passaram por modelos de series temporais.

**Caracteristicas da serie temporal de biomassa:**

| Propriedade | Descricao |
|---|---|
| Resolucao nativa | 16 dias (compositos MOD13Q1) |
| Resolucao no dataset | Horaria (via ffill do composito mais recente) |
| Sazonalidade | Forte — ciclo anual seca/chuvosa. No Cerrado, NDVI cai na seca (abr-set) e sobe na chuva (out-mar) |
| Autocorrelacao | Muito alta (serie suave com degraus de 16 dias) |
| Relacao com HAS_FOCO | **Negativa** — NDVI baixo (vegetacao seca) coincide com alta concentracao de focos. Correlacao confirmada na EDA |
| Gaps | Raros no cenario E (ffill cobre gaps iniciais); podem existir em anos com cobertura MOD13Q1 esparsa |

**ATENCAO — Particularidade da serie de biomassa (degraus):** como o ffill propaga o valor do composito de 16 dias para cada hora, a serie horaria de NDVI/EVI nao e continua — ela e constante por trechos de ~16 dias e salta para um novo valor. Isso tem implicacoes diretas na escolha de metodos:

- **ARIMA/SARIMA univariado para NDVI:** funcionara, mas os residuos serao zero dentro de cada composito e terao picos nos "saltos". A sazonalidade relevante nao e diaria (m=24) e sim algo mais proximo de mensal ou sazonal-anual. Recomenda-se:
  - **Usar m sazonal adaptado** para SARIMA: `m=336` (14 dias em horas ≈ periodo do composito) ou `m=8760` (anual, inviavel computacionalmente). Na pratica, `m=336` ou `m=672` (28 dias) sao opcoes razoaveis.
  - Alternativamente, **agregar a serie para resolucao diaria ou semanal** antes de ajustar ARIMA/SARIMA, e depois propagar os residuos de volta para a granularidade horaria. Isso respeita a natureza da serie e reduz custo computacional.

**Metodos recomendados:**

| Metodo | Aplicar em | Justificativa | Adaptacoes necessarias | Saida esperada |
|---|---|---|---|---|
| **ewma_lags** | NDVI_buffer, NDVI_point, EVI_buffer, EVI_point | Captura tendencia de queda/subida recente da biomassa. Lags de 168h (1 semana) e 336h (2 semanas ≈ 1 composito) sao os mais informativos | Adicionar lags de 336h e possivelmente 720h (1 mes). Ajustar alphas | `tsf_ewma_ndvi_buffer_a01/a03/a08`, `tsf_lag_ndvi_buffer_168h/336h` (etc. para cada indice) |
| **arima** | NDVI_buffer, EVI_buffer | Baseline univariado. Residuo positivo = vegetacao mais verde que o esperado → menor risco | Considerar pre-agregar para diario (mean) antes do ARIMA para evitar residuos triviais dentro do degrau de 16 dias | `tsf_arima_ndvi_buffer_pred`, `tsf_arima_ndvi_buffer_resid` |
| **sarima** | NDVI_buffer | Captura ciclo sazonal de seca/chuva que domina a dinamica do NDVI | Usar `m=336` (≈ periodo do composito) ou pre-agregar para semanal com `m=26` (≈ semestre). Custo alto | `tsf_sarima_ndvi_buffer_pred`, `tsf_sarima_ndvi_buffer_resid` |
| **arimax** | precip como endogena, NDVI_buffer como exogena | Modela a precipitacao condicionada ao nivel de biomassa. Residuo positivo = choveu mais do que a biomassa sugeriria → menor risco | Adicionar NDVI_buffer ao vetor de exogenas de ARIMAX | `tsf_arimax_precip_pred` (ja com influencia de biomassa no residuo) |
| **minirocket** | NDVI_buffer + NDVI_point + meteo (multichannel) | Embedding captura padroes conjuntos biomassa-meteorologia sem exigir sazonalidade parametrica. Ideal para detectar "assinaturas pre-fogo" | Aumentar canais de 4 para 6+ (incluir NDVI e EVI). Avaliar janela de 168h (1 semana) e 336h (16 dias) | `tsf_minirocket_f000` ... `tsf_minirocket_fXXX` |
| **tskmeans** | NDVI_buffer | Clustering de segmentos semanais de biomassa revela "regimes de vegetacao" (ex.: cerrado verde, transicao, cerrado seco). Cada regime tem distribuicao de probabilidade de fogo diferente | Ajustar P para 336h (16 dias) ou 672h (1 mes). K=6 a 10 clusters. Usar NDVI_buffer como serie de cluster | `tsf_tskmeans_ndvi_cluster` |

**O que esperar como features uteis para `HAS_FOCO`:**
- `tsf_arima_ndvi_buffer_resid` negativo grande → biomassa caiu mais que o esperado → alta probabilidade de fogo.
- `tsf_ewma_ndvi_buffer_a01` (alpha baixo = suavizacao forte) → tendencia de longo prazo da biomassa; queda sustentada sinaliza estacao seca.
- `tsf_lag_ndvi_buffer_336h` → biomassa 2 semanas atras. Diferenca `NDVI_buffer - tsf_lag_ndvi_buffer_336h` = taxa de variacao recente — valor negativo indica ressecamento ativo.
- `tsf_minirocket_f*` com canais de biomassa → embeddings que codificam o "contexto ambiental" da janela recente; o classificador pode aprender que certas assinaturas precedem fogo.
- `tsf_tskmeans_ndvi_cluster` → permite ao modelo tratar diferentes regimes de biomassa como contextos distintos, sem precisar que a relacao seja monotonica.

---

#### 3.2.3 Temperatura, Umidade e Radiacao (Tier 2)

**Status atual:** arima/sarima univariados ja implementados para `temp`, `umid` e `rad`. ARIMAX usa-as como exogenas da precipitacao.

**Recomendacao para o artigo:** manter o tratamento existente. Nao ha necessidade de criar novos modelos dedicados para essas variaveis — elas ja participam como:
1. Slugs univariados em ARIMA/SARIMA (gerando `tsf_arima_temp_pred/resid`, etc.).
2. Exogenas em ARIMAX/SARIMAX_exog.
3. Canais em MiniROCKET.

**Adaptacao unica necessaria:** incluir NDVI/EVI como exogenas adicionais nos modelos ARIMAX/SARIMAX que ja usam temperatura, umidade e radiacao. Isso permite que o residuo de precipitacao capte a componente de fogo que nao e explicada nem pela meteorologia nem pela biomassa.

---

#### 3.2.4 Vento e Pressao (Tier 3)

**Recomendacao:** nao aplicar ARIMA/SARIMA/Prophet dedicados. Incluir apenas como:
- Canais adicionais em MiniROCKET (se a janela suportar o aumento de dimensionalidade).
- `fator_propagacao` (ja calculado) como feature direta nos classificadores.

**Justificativa:** vento e pressao tem baixa autocorrelacao e alta variancia horaria. Modelos de series temporais univariados tendem a produzir residuos ruidosos sem valor preditivo significativo para `HAS_FOCO`.

---

## 4. Correlacao temporal biomassa × fogo e implicacoes

### 4.1 Mecanismo fisico

O ciclo de incendios no Cerrado esta intimamente ligado a sazonalidade da vegetacao:

1. **Estacao chuvosa (out-mar):** precipitacao alta → NDVI alto (vegetacao verde) → biomassa com alto teor de umidade → baixa inflamabilidade → poucos focos.
2. **Transicao (abr-mai):** precipitacao diminui → NDVI comeca a cair → biomassa comeca a secar → risco crescente.
3. **Estacao seca (jun-set):** precipitacao minima → NDVI baixo (vegetacao seca/senescente) → biomassa altamente inflamavel → pico de focos (especialmente ago-set).
4. **Transicao pos-seca (out):** primeiras chuvas → rebrota → NDVI sobe → focos diminuem rapidamente.

### 4.2 Implicacoes para a fusao temporal

Esse ciclo tem duas consequencias importantes para a escolha de modelos:

**a) NDVI e um indicador antecedente (leading indicator) de fogo:**
A queda no NDVI precede o pico de focos por algumas semanas. Features que capturam a **taxa de variacao** da biomassa (derivadas, residuos, diferencas de lags) sao potencialmente mais discriminativas que o valor absoluto.

- `tsf_lag_ndvi_buffer_336h - NDVI_buffer` = variacao em 2 semanas. Valor positivo (NDVI caiu) correlaciona com aumento iminente de focos.
- `tsf_sarima_ndvi_buffer_resid` negativo = biomassa abaixo do esperado para aquela epoca do ano → sinal de seca antecipada ou mais severa.

**b) A relacao NDVI × HAS_FOCO nao e linear nem instantanea:**
O NDVI pode estar baixo sem fogo (estacao seca "normal") ou alto com fogo (incendio em area ainda verde, causado por acao humana). Por isso:

- Features de **cluster temporal** (TSKMeans) ajudam ao criar "regimes" que distinguem "seca normal" de "seca anormal com fogo".
- Features de **embedding** (MiniROCKET) capturam interacoes nao-lineares entre multiplas variaveis na janela temporal, sem exigir especificacao explicita da relacao funcional.

### 4.3 Biomassa como variavel exogena vs endogena

Dois arranjos sao possiveis nos modelos com variaveis exogenas (ARIMAX/SARIMAX):

| Arranjo | Endogena | Exogenas | Interpretacao do residuo |
|---|---|---|---|
| **A (recomendado)** | Precipitacao | Temp, Umid, Rad + **NDVI_buffer** | "Quanta precipitacao aconteceu alem do que meteorologia + biomassa explicam?" — residuo negativo em area de biomassa seca = deficit hidrico extremo |
| **B (alternativo)** | NDVI_buffer | Precipitacao, Temp, Rad | "Quanta biomassa existe alem do que o clima explica?" — residuo negativo = vegetacao mais degradada que o esperado pelo clima |

**Recomendacao:** implementar ambos, mas priorizar o **Arranjo A** por duas razoes:
1. A precipitacao horaria e a serie com mais pontos e melhor estrutura temporal para ARIMA (a biomassa tem resolucao nativa de 16 dias).
2. O residuo de precipitacao "descontado" pela biomassa e mais interpretavel para o classificador: deficit hidrico em area seca = risco critico.

O **Arranjo B** deve ser explorado como experimento complementar (novo slug `arimax_ndvi_endog`), mas com expectativa de convergencia mais fragil devido a serie de degraus da biomassa.

---

## 5. Matriz variavel x metodo — detalhamento de entradas e saidas

### 5.1 Legenda

- **Entrada:** descricao da serie de entrada para o metodo.
- **Pre-condicoes temporais:** o que a serie precisa ter para o metodo funcionar.
- **Saida tsf_*:** nomes das colunas geradas.
- **Interpretacao para HAS_FOCO:** como o classificador pode usar a feature.
- **Status:** ja implementado / requer adaptacao / novo.

### 5.2 Matriz completa

---

#### EWMA + Lags

| Variavel | Entrada | Pre-condicoes | Saida | Interpretacao para HAS_FOCO | Status |
|---|---|---|---|---|---|
| Precipitacao | Serie horaria por cidade | Nenhuma especifica | `tsf_ewma_precip_a01/a03/a08`, `tsf_lag_precip_1h/24h/168h` | Tendencia recente de chuva; lag 168h = proxy de seca semanal | Implementado |
| Temperatura | Serie horaria por cidade | Nenhuma | `tsf_ewma_temp_a01/a03/a08`, `tsf_lag_temp_1h/24h/168h` | Tendencia de aquecimento; lag 24h = baseline diurno | Implementado |
| Umidade | Serie horaria por cidade | Nenhuma | `tsf_ewma_umid_a01/a03/a08`, `tsf_lag_umid_1h/24h/168h` | Tendencia de ressecamento | Implementado |
| Radiacao | Serie horaria por cidade | Nenhuma | `tsf_ewma_rad_a01/a03/a08`, `tsf_lag_rad_1h/24h/168h` | Tendencia de radiacao acumulada | Implementado |
| **NDVI_buffer** | Serie horaria (degraus 16d) por cidade | Nenhuma | `tsf_ewma_ndvi_buffer_a01/a03/a08`, `tsf_lag_ndvi_buffer_168h/336h` | **Tendencia de queda na biomassa regional; lag 336h = variacao entre compositos** | **Novo** |
| **NDVI_point** | Serie horaria (degraus 16d) por cidade | Nenhuma | `tsf_ewma_ndvi_point_a01/a03/a08`, `tsf_lag_ndvi_point_168h/336h` | Variacao de biomassa local | **Novo** |
| **EVI_buffer** | Serie horaria (degraus 16d) por cidade | Nenhuma | `tsf_ewma_evi_buffer_a01/a03/a08`, `tsf_lag_evi_buffer_168h/336h` | Complementar ao NDVI; melhor em areas densas | **Novo** |
| **EVI_point** | Serie horaria (degraus 16d) por cidade | Nenhuma | `tsf_ewma_evi_point_a01/a03/a08`, `tsf_lag_evi_point_168h/336h` | Complementar ao NDVI local | **Novo** |

**Total de features novas EWMA/lags biomassa:** 4 indices x (3 ewma + 2 lags) = **20 features novas**.

---

#### ARIMA univariado

| Variavel | Entrada | Pre-condicoes | Saida | Interpretacao para HAS_FOCO | Status |
|---|---|---|---|---|---|
| Precipitacao | Serie horaria, min ~15 pontos por bloco | Autocorrelacao; diferenciavel ordem 1 | `tsf_arima_precip_pred`, `_resid` | Residuo negativo = deficit; pred = baseline meteorologico | Implementado |
| Temperatura | Serie horaria | Idem | `tsf_arima_temp_pred`, `_resid` | Residuo positivo = calor anomalo | Implementado |
| Umidade | Serie horaria | Idem | `tsf_arima_umid_pred`, `_resid` | Residuo negativo = secura anomala | Implementado |
| Radiacao | Serie horaria | Idem | `tsf_arima_rad_pred`, `_resid` | Residuo positivo = radiacao anomala | Implementado |
| **NDVI_buffer** | Serie horaria (degraus) | Autocorrelacao OK (muito alta). Residuos triviais dentro do degrau — considerar pre-agregar para diario | `tsf_arima_ndvi_buffer_pred`, `_resid` | **Residuo negativo = biomassa abaixo do baseline = risco** | **Novo (com adaptacao)** |
| **EVI_buffer** | Serie horaria (degraus) | Idem ao NDVI | `tsf_arima_evi_buffer_pred`, `_resid` | Complementar ao NDVI | **Novo (com adaptacao)** |

---

#### SARIMA (univariado sazonal)

| Variavel | Entrada | Pre-condicoes | Saida | Interpretacao para HAS_FOCO | Status |
|---|---|---|---|---|---|
| Precipitacao | Serie horaria | Sazonalidade diaria (m=24) | `tsf_sarima_precip_pred`, `_resid` | Residuo = anomalia descontada sazonalidade diurna | Implementado |
| Temperatura | Serie horaria | m=24 | `tsf_sarima_temp_pred`, `_resid` | Anomalia termica descontada ciclo diurno | Implementado |
| Umidade | Serie horaria | m=24 | `tsf_sarima_umid_pred`, `_resid` | Anomalia de umidade | Implementado |
| Radiacao | Serie horaria | m=24 | `tsf_sarima_rad_pred`, `_resid` | Anomalia de radiacao | Implementado |
| **NDVI_buffer** | Serie horaria (degraus) | **Sazonalidade nao e diaria; usar m=336 (≈16 dias) ou pre-agregar para semanal com m=26** | `tsf_sarima_ndvi_buffer_pred`, `_resid` | **Residuo = desvio da sazonalidade anual de biomassa = seca anormal** | **Novo (requer adaptacao de m)** |

**Nota critica:** SARIMA com m=336 e computacionalmente pesado (matrizes de estado m x m). Recomenda-se:
- Pre-agregar NDVI para resolucao **semanal** (media) → serie de ~52 pontos/ano → SARIMA com m=26 (semestral) fica viavel.
- Aplicar o residuo semanal de volta a granularidade horaria via merge_asof.
- Alternativamente, testar apenas nos anos mais recentes (ex.: 2018-2024) para limitar custo.

---

#### ARIMAX / SARIMAX_exog (multivariado)

| Arranjo | Endogena | Exogenas | Saida | Interpretacao | Status |
|---|---|---|---|---|---|
| Meteorologico puro | precip | temp, umid, rad | `tsf_arimax_precip_pred`, `_resid` | Precipitacao nao explicada por temp/umid/rad | Implementado |
| **Meteorologico + biomassa (Arranjo A)** | precip | temp, umid, rad, **NDVI_buffer** | `tsf_arimax_precip_pred`, `_resid` | **Deficit hidrico residual apos descontar meteorologia E biomassa** | **Novo (adicionar exogena)** |
| **Biomassa endogena (Arranjo B)** | **NDVI_buffer** | precip, temp, rad | `tsf_arimax_ndvi_buffer_pred`, `_resid` | **Biomassa residual nao explicada pelo clima** | **Novo (requer novo slug endog)** |
| Sarimax meteorologico puro | precip | temp, umid, rad | `tsf_sarimax_exog_precip_pred`, `_resid` | Com sazonalidade diaria | Implementado |
| **Sarimax + biomassa** | precip | temp, umid, rad, **NDVI_buffer** | `tsf_sarimax_exog_precip_pred`, `_resid` | Idem ARIMAX + ciclo sazonal | **Novo (adicionar exogena)** |

---

#### Prophet

| Variavel | Entrada | Saida | Interpretacao | Status |
|---|---|---|---|---|
| Precipitacao | Serie horaria por cidade | `tsf_prophet_precip_pred`, `_resid` | Baseline de Fourier; residuo = anomalia | Implementado |
| **NDVI_buffer** | Serie horaria (degraus) por cidade | `tsf_prophet_ndvi_buffer_pred`, `_resid` | **Decomposicao de sazonalidade anual da biomassa. Residuo = anomalia vegetal** | **Novo** |

**Nota:** Prophet lida bem com series nao-homodistribuidas e com sazonalidades multiplas. Para NDVI, configurar `yearly_seasonality=True` (ciclo seca/chuva) e `daily_seasonality=False` (NDVI nao tem padrao diurno).

---

#### MiniROCKET (embedding multicanal)

| Configuracao | Canais | Janela | Saida | Interpretacao | Status |
|---|---|---|---|---|---|
| Meteorologico puro | precip, temp, umid, rad (4) | 24h | `tsf_minirocket_f000..f083` (84) | Assinatura meteorologica da janela | Implementado |
| **Meteorologico + biomassa** | precip, temp, umid, rad, **NDVI_buffer, EVI_buffer** (6) | 24h | `tsf_minirocket_f000..fXXX` | **Assinatura ambiental completa: meteorologia + vegetacao** | **Novo (expandir canais)** |
| **Biomassa + meteorologico, janela longa** | precip, temp, umid, rad, NDVI_buffer, EVI_buffer (6) | **168h** | `tsf_minirocket_168h_f000..fXXX` | **Padrao semanal conjunt biomassa-clima** | **Novo (nova janela)** |

**Notas sobre expansao:**
- Mais canais = mais kernels necessarios para cobertura adequada. Recomendar 168 kernels (dobro do padrao) para 6 canais.
- A janela de 168h com biomassa (que muda a cada ~384h) garante que o embedding capture a transicao entre compositos MODIS dentro da janela.
- Exige que **nenhum canal tenha NaN na janela**. No cenario E (KNN), meteorologia esta limpa; biomassa tambem (ffill). No cenario F, pode falhar mais frequentemente.

---

#### TSKMeans (clustering temporal)

| Variavel | Serie | Periodo (P) | K clusters | Saida | Interpretacao | Status |
|---|---|---|---|---|---|---|
| Precipitacao | precip horaria | 168h (1 semana) | 8 | `tsf_tskmeans_cluster` | Regime de precipitacao semanal (ex.: semana chuvosa, semana seca, transicao) | Implementado |
| **NDVI_buffer** | NDVI horaria (degraus) | **336h (≈ 1 composito MOD13Q1)** | **6-10** | `tsf_tskmeans_ndvi_cluster` | **Regime de vegetacao: cerrado verde, transicao, cerrado seco, etc.** | **Novo** |
| **Multivariado** | precip + NDVI_buffer | 168h | 10 | `tsf_tskmeans_multi_cluster` | **Regime ambiental composto: seco+queimavel, chuvoso+verde, transicao** | **Novo (requer adaptacao)** |

---

## 6. Sequenciamento e estrategia de execucao

### 6.1 Abordagem recomendada: incremental em duas fases

**Fase 1 — Baseline com reuso do pipeline existente + ewma/lags de biomassa**

Escopo: rodar os metodos existentes sobre as bases `_article` sem modificacao de codigo, mais adicionar NDVI/EVI ao registro de `ewma_lags`.

1. Adaptar `TemporalFusionEngineer` (ou criar wrapper `TemporalFusionArticle`) para ler de `data/_article/0_datasets_with_coords/` e gravar em `data/_article/1_datasets_with_fusion/`.
2. Expandir `ARIMA_VARS_ALL` com novos slugs: `ndvi_buffer`, `ndvi_point`, `evi_buffer`, `evi_point`.
3. Expandir `_generate_ewma_lags` para incluir as 4 colunas de biomassa com lags adaptados (168h, 336h).
4. Rodar `ewma_lags` + `arima` + `sarima` (univariados) para todos os slugs incluindo biomassa.
5. Rodar EDA comparativa: correlacoes `tsf_*` × `HAS_FOCO` para validar que as features de biomassa agregam informacao.

**Entregavel:** Parquets `1_datasets_with_fusion` com features `tsf_ewma_*`, `tsf_arima_*`, `tsf_sarima_*` para meteorologia + biomassa. Camada A de metricas para biomassa.

**Fase 2 — Modelos avancados e arranjos biomassa-meteorologia**

Escopo: implementar as adaptacoes mais complexas e avaliar ganho incremental.

1. Adicionar NDVI_buffer como exogena em ARIMAX/SARIMAX (Arranjo A).
2. Implementar ARIMAX com NDVI como endogena (Arranjo B) — novo slug.
3. Expandir MiniROCKET para 6+ canais (incluindo biomassa).
4. Implementar TSKMeans de NDVI_buffer com P=336h.
5. Prophet para NDVI_buffer com `yearly_seasonality=True`.
6. Construir base campea (`build_champion_article_bases.py`) usando method_ranking_train da Fase 1 + metodos da Fase 2.

**Entregavel:** Parquets completos com fusao temporal; method_ranking comparativo; bases campeas prontas para treino XGBoost/RF.

### 6.2 Justificativa para duas fases (vs tudo junto)

- A Fase 1 reutiliza ~90% do codigo existente e pode ser executada rapidamente para obter um baseline.
- A Fase 2 exige adaptacoes (novos slugs endog, canais MiniROCKET, ajuste de m sazonal) que tem risco de bugs e custo computacional incerto.
- Se a Fase 1 ja mostrar ganho significativo em PR-AUC com biomassa, a Fase 2 pode ser priorizada; se nao, o esforco pode ser redirecionado.

### 6.3 Diretorio de saida proposto

```
data/_article/
├── 0_datasets_with_coords/          # (ja existe)
│   ├── base_D_with_rad_drop_rows_calculated/
│   ├── base_E_with_rad_knn_calculated/
│   └── base_F_full_original_calculated/
├── 1_datasets_with_fusion/           # (proposto)
│   ├── base_E_with_rad_knn_calculated/
│   │   ├── ewma_lags/               # inmet_bdq_{ano}_cerrado.parquet
│   │   ├── arima/
│   │   ├── sarima/
│   │   ├── arimax/
│   │   ├── sarimax_exog/
│   │   ├── prophet/
│   │   ├── minirocket/
│   │   ├── tskmeans/
│   │   └── champion_tsfusion/       # melhor combinacao de metodos
│   ├── base_D_with_rad_drop_rows_calculated/
│   │   └── (idem)
│   └── base_F_full_original_calculated/
│       └── (idem)
└── logs/
```

---

## 7. Proximos passos de engenharia

### 7.1 Codigo — modificacoes necessarias

1. **Wrapper para bases `_article`:** criar `src/article/temporal_fusion.py` que:
   - Herda/instancia `TemporalFusionEngineer` com paths apontando para `data/_article/0_datasets_with_coords/` (entrada) e `data/_article/1_datasets_with_fusion/` (saida).
   - Sobrescreve `ARIMA_VARS_ALL` para incluir slugs de biomassa.
   - Sobrescreve `_generate_ewma_lags` para incluir canais de biomassa com lags adaptados.

2. **Expansao de `ARIMA_VARS_ALL`:**
   ```python
   ARIMA_VARS_ALL = {
       "precip":      "PRECIPITACAO TOTAL, HORARIO (mm)",
       "temp":        "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
       "umid":        "UMIDADE RELATIVA DO AR, HORARIA (%)",
       "rad":         "RADIACAO GLOBAL (KJ/m2)",
       "ndvi_buffer": "NDVI_buffer",
       "ndvi_point":  "NDVI_point",
       "evi_buffer":  "EVI_buffer",
       "evi_point":   "EVI_point",
   }
   ```

3. **Config.yaml:** adicionar cenarios `article_*_tsfusion` em `modeling_scenarios` e paths correspondentes em `temporal_fusion_paths` ou num bloco `article_pipeline.temporal_fusion`.

4. **Champion builder:** adaptar `build_champion_temporal_bases.py` para ler de `1_datasets_with_fusion/` e gravar em `1_datasets_with_fusion/{cenario}/champion_tsfusion/`.

### 7.2 Avaliacao (Camadas A e B)

- **Camada A:** metricas MAE/R2 dos modelos temporais na serie de biomassa. Espera-se R2 alto (serie muito suave) — o valor real esta nos residuos.
- **Camada B:** treinar XGBoost/RF com e sem features `tsf_*` de biomassa; comparar PR-AUC. Hipotese: features de biomassa + fusao temporal > features de biomassa brutas > sem biomassa.

### 7.3 Riscos e mitigacoes

| Risco | Mitigacao |
|---|---|
| SARIMA com m=336 nao converge | Pre-agregar para resolucao semanal; usar m=26 |
| MiniROCKET com 6+ canais estoura memoria | Reduzir kernels ou processar por chunks de cidades |
| NDVI_buffer com degraus gera residuos triviais no ARIMA | Pre-agregar para diario antes do ARIMA; validar na Camada A se residuo tem variancia util |
| Cenario F com muitos NaN inviabiliza metodos | Priorizar cenario E (KNN); F como experimento de robustez |
| Exogena NDVI no ARIMAX nao melhora residuo | Comparar metricas do Arranjo A vs pipeline puro; se empate, manter pipeline puro e usar NDVI via ewma_lags |

---

*Documento de referencia para a etapa de fusao temporal do artigo. Deve ser revisado apos execucao da Fase 1 com resultados da Camada A e correlacoes tsf_biomassa × HAS_FOCO.*
