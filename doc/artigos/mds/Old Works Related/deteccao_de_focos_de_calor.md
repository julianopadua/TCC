# Silva2016-GeoAlagoas – Detecção de focos de calor através de satélites nos distintos biomas brasileiros de 1999 a 2015

## Ficha técnica

Autores: Maria Flaviane Almeida Silva; Benjamin Leonardo Alves White. 
Evento: 4º GeoAlagoas – Simpósio sobre geotecnologias e geoinformação no Estado de Alagoas (2016).
Título: “Detecção de focos de calor através de satélites nos distintos biomas brasileiros de 1999 a 2015”.

---

## Objeto, região e período de estudo

* Escala espacial: todos os seis biomas brasileiros (Amazônia, Cerrado, Mata Atlântica, Caatinga, Pantanal e Pampas). 
* Escala temporal: 17 anos (01/01/1999 a 31/12/2015).
* Objetivo central: quantificar e caracterizar a distribuição temporal e espacial dos focos de calor detectados pelos satélites de referência do INPE em cada bioma, em termos absolutos (total de focos) e relativos (focos por km²), e discutir implicações para prevenção e combate às queimadas. 

Conexão com o TCC: o trabalho oferece uma visão panorâmica de longo prazo do regime de focos de calor em todos os biomas, evidenciando o papel destacado do Cerrado em termos de incidência relativa e reforçando a adequação do uso da série do BDQueimadas com satélite de referência para estudos comparativos e de modelagem.

---

## Dados e abordagem metodológica

### Fonte e natureza dos dados

* Dados de focos de calor:

  * Fonte: Portal de Monitoramento de Queimadas e Incêndios do INPE (BDQueimadas). 
  * Período: 1999–2015.
  * Critério de inclusão: somente focos detectados pelos "satélites de referência" do INPE (NOAA-12 até agosto de 2007; AQUA_M-T a partir de então). 
  * Definição operacional de foco de calor: pixel com temperatura superficial maior que 47 °C, detectado por sensores termais e infravermelho (AVHRR, MODIS etc.), o que indica anomalia térmica significativa, mas não garante, por si só, que haja incêndio florestal em progresso. 

* Caracterização dos biomas:

  * Extensão territorial de cada bioma segundo IBGE (Tabela 1) e breve descrição climática e fitofisionômica:

    * Amazônia (clima equatorial úmido, vegetação densa);
    * Cerrado (clima sazonal com estação chuvosa e seca bem definidas, mosaico de savanas, matas e campos);
    * Mata Atlântica (altíssima pluviosidade em partes da região Sudeste);
    * Caatinga (semiárido, vegetação xerófila);
    * Pantanal (mosaico de formações sujeitas à inundação);
    * Pampas (campos subtropicais no RS). 

### Tratamento dos dados e indicadores

* Agregação temporal:

  * Quantificação do número total de focos no Brasil por ano (1999–2015). 
  * Agregação mensal: distribuição média dos focos por mês, considerando todo o período. 

* Agregação espacial:

  * Contagem de focos por bioma para o período 1999–2015. 
  * Cálculo da incidência relativa de focos por área (focos/km²) em cada bioma, usando a área aproximada fornecida pelo IBGE. 

Não há aplicação de modelos estatísticos complexos ou de aprendizado de máquina; trata-se de um estudo descritivo e comparativo, baseado em séries temporais e indicadores simples (totais, médias anuais, normalização por área).

---

## Principais resultados e achados

### Volume total e variabilidade interanual

* Total de focos de calor (Brasil, 1999–2015): 2.990.145 focos detectados pelos satélites de referência. 
* Ano com menor número de focos: 2000.
* Ano com maior número de focos: 2010 (pico associado a condições climáticas e dinâmicas de uso do solo mais favoráveis à ocorrência de queimadas). 

Esse resultado reforça que séries longas de focos de calor apresentam variação interanual importante, condicionada tanto por fatores climáticos quanto por mudanças em práticas de uso da terra e políticas públicas.

### Sazonalidade (distribuição mensal)

* Meses com maior ocorrência de focos no agregado nacional:

  * Setembro, Agosto, Outubro, Novembro, Julho e Dezembro (em ordem decrescente). 
* Meses com menor ocorrência:

  * Abril, Fevereiro, Março, Janeiro, Maio e Junho (em ordem crescente). 

Esse padrão é consistente com estudos anteriores que apontam julho a outubro como principal estação de incêndios no Brasil, com variações regionais: Centro-Oeste, Sul e Sudeste concentrando focos em setembro, enquanto em partes do Norte e do Nordeste o pico ocorre em outubro ou mesmo no início do ano seguinte (dezembro–março) devido a diferenças de regime de chuvas. 

### Comparação entre biomas (valores absolutos)

* Média anual de focos por bioma (1999–2015): 

  * Amazônia: 80.989 focos/ano (maior valor absoluto).
  * Cerrado: 60.639 focos/ano (segundo maior).
  * Caatinga: 16.983 focos/ano.
  * Mata Atlântica: 11.668 focos/ano.
  * Pantanal: 5.137 focos/ano.
  * Pampas: 438 focos/ano (menor).

Os autores relacionam o altíssimo número absoluto na Amazônia ao desmatamento e à conversão de áreas florestais em pastagens e agricultura, ressaltando que o fogo é facilitado após a derrubada da vegetação, quando a biomassa seca e perde umidade. 

No Cerrado, o estudo destaca que as práticas de queimadas são antigas e foram intensificadas nas últimas décadas, em paralelo à expansão da agropecuária, lembrando que muitas espécies vegetais apresentam adaptações ao fogo, o que contribui para regimes recorrentes de queima. 

### Incidência proporcional (focos por km²)

* Quando se ajusta pelo tamanho do bioma (focos/km²), o ranking se altera de forma importante: 

  * Pantanal: 0,58 focos/km² (maior incidência proporcional).
  * Cerrado: 0,51 focos/km² (segunda maior).
  * Caatinga: 0,34 focos/km².
  * Amazônia: 0,33 focos/km².
  * Mata Atlântica: 0,18 focos/km².
  * Pampas: 0,04 focos/km².

Esse resultado é apontado como “curioso” pelos autores, pois o Pantanal aparece como bioma mais crítico em termos relativos, embora seja pouco abordado na literatura sobre fogo, enquanto Cerrado e Amazônia dominam o debate. 

Do ponto de vista do TCC, o valor de 0,51 focos/km² reforça que o Cerrado é um hotspot nacional de ocorrência relativa de fogo, o que fundamenta a escolha desse bioma como objeto central de modelagem preditiva.

### Interpretações por tipo de bioma e uso do solo

O trabalho discute, de forma qualitativa, os fatores associados aos padrões de foco em cada bioma: 

* Amazônia:

  * Fogo fortemente ligado a desmatamento e expansão agropecuária;
  * Vegetação naturalmente úmida, mas que, após corte e secagem, passa a sustentar e propagar o fogo.

* Mata Atlântica:

  * Elevado número de focos associado ao uso intensivo de queimadas em monoculturas de cana-de-açúcar, onde o fogo é parte do manejo (colheita).

* Caatinga:

  * Vegetação seca durante boa parte do ano, propensa à ignição;
  * Queimadas e incêndios associados à exploração de recursos para agropecuária.

* Cerrado:

  * Uso antigo e intensificado do fogo ligado à expansão agropecuária;
  * Muitas espécies adaptadas ou dependentes do fogo, o que molda o regime de queimadas.

* Pantanal:

  * Fogo presente antes da introdução do gado, hoje muito associado a manejo de pastagens naturais e plantadas durante a estiagem;
  * Queimadas usadas para estimular rebrota de gramíneas para o rebanho.

* Pampas:

  * Menor número absoluto de focos, mas ainda com impactos socioambientais;
  * Queimadas como técnica de manejo de pastagens, concentradas em julho–setembro.

Essas interpretações reforçam que o fogo no Brasil resulta da interação entre clima, estrutura da vegetação e usos do solo, produzindo regimes distintos por bioma.

---

## Pontos fortes do artigo para fundamentar o TCC

1. **Validação do uso do BDQueimadas e do conceito de “satélite de referência”**

   * O trabalho descreve explicitamente como o INPE constrói a série temporal utilizando um satélite de referência em cada fase (NOAA-12, depois AQUA_M-T), ressaltando que, embora subestime o número real de focos, a série mantém consistência metodológica e de horário de passagem, adequada para análise de tendências espaciais e temporais. 
   * Isso serve como fundamentação direta para a escolha do BDQueimadas como fonte principal de dados de fogo no TCC, inclusive justificando a opção por trabalhar com o satélite de referência para garantir comparabilidade ao longo do tempo.

2. **Demonstração quantitativa da importância do Cerrado em termos relativos**

   * Ao mostrar que o Cerrado é o segundo bioma em número absoluto de focos e o segundo em incidência relativa (0,51 focos/km²), o artigo reforça que esse bioma é um foco crítico de queimadas no Brasil, o que justifica a seleção do Cerrado como alvo específico de modelagem preditiva de risco de fogo. 

3. **Evidência clara de sazonalidade e variabilidade interanual**

   * A concentração de focos entre julho e outubro, com pico em setembro, e a forte variabilidade entre anos mostram que os regimes de fogo são marcadamente sazonais e variam em resposta a condições climáticas e a dinâmicas de uso do solo. 
   * Isso se conecta com a proposta de modelar a probabilidade de ocorrência de focos a partir de variáveis ambientais que captuem essa sazonalidade (chuva, temperatura, umidade, etc.).

4. **Integração entre biomas e discussão de fire regimes distintos**

   * Ao comparar todos os biomas com uma métrica comum (focos, focos/km²), o estudo evidencia que regimes de fogo são específicos de cada bioma, dependendo de clima, vegetação e usos do solo. 
   * Essa visão comparativa pode ser usada na introdução da seção “Trabalhos anteriores” para argumentar que modelos de aprendizado de máquina para previsão de fogo devem ser construídos de forma específica por bioma ou região, em vez de assumir comportamento homogêneo em todo o país.

---

## Limitações e lacunas

* Natureza descritiva:

  * O estudo restringe-se a contagens, proporções e comparações simples, sem a construção de modelos estatísticos multivariados ou algoritmos de aprendizado de máquina.
  * Não há avaliação formal da influência de variáveis climáticas ou socioeconômicas, nem modelagem preditiva em nível de pixel ou de unidade espacial menor.

* Ausência de variáveis climáticas explícitas:

  * Embora a discussão contextual mencione regimes de chuva, sazonalidade e uso do solo, o artigo não integra explicitamente séries meteorológicas (INMET, reanálises) nem índices climáticos na análise quantitativa.

* Resolução temporal relativamente grossa:

  * A análise é feita em agregados anuais e mensais, sem explorar variação em escalas semanais ou diárias, que são mais relevantes para operação e para modelos de curto prazo.

Essas lacunas abrem espaço para o TCC: avanço em direção a modelagem preditiva baseada em aprendizado de máquina, com integração explícita de variáveis climáticas em maior resolução temporal, focada no Cerrado.

---

## Como usar este artigo na seção “Trabalhos anteriores”

Linha sugerida de uso:

* Apresentar o estudo como uma análise nacional de longo prazo dos focos de calor detectados pelos satélites de referência do INPE entre 1999 e 2015, comparando biomas em termos absolutos e relativos de ocorrência. 
* Destacar três elementos principais:

  1. A descrição do procedimento do INPE com satélites de referência, que fundamenta o uso da mesma série pelo TCC;
  2. A evidência de que o Cerrado está entre os biomas com maior incidência relativa de focos, justificando o recorte do TCC;
  3. A caracterização de forte sazonalidade e variabilidade interanual dos focos, o que reforça a necessidade de modelos capazes de capturar esse comportamento em função de variáveis ambientais.
* Em seguida, apontar que, apesar de fundamental para compreender o panorama nacional, o trabalho é essencialmente descritivo, sem explorar técnicas de aprendizado de máquina ou modelos multivariados de previsão, e que o TCC propõe avançar nesse sentido para o bioma Cerrado.
