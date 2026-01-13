# Silva2020-HVG – Análise de séries temporais de focos de calor em biomas brasileiros utilizando o Grafo de Visibilidade Horizontal (2020)

## Ficha técnica

Autores: Joelma Mayara da Silva; Lidiane da Silva Araújo; Tatijana Stosic; Borko Stosic. 
Periódico: Research, Society and Development, v. 9, n. 9, e308996276, 2020.
Biomas: Amazônia, Cerrado, Caatinga, Mata Atlântica.
Período: 04/07/2002 a 10/09/2019.

---

## Objeto e dados

* Objetivo: caracterizar a dinâmica temporal das queimadas em grandes biomas brasileiros, avaliando se as séries diárias de focos de calor se comportam como processos estocásticos correlacionados ou caóticos, a partir de ferramentas de redes complexas. 
* Dados:

  * Focos de calor (queimadas) registrados pelo INPE, via BDQueimadas.
  * Satélite de referência: AQUA_M-T, garantindo consistência temporal da série. 
  * Série diária de contagem de focos por bioma, mais séries de anomalias padronizadas para reduzir sazonalidade. 
* Normalização (anomalias):

  * Para cada dia do ano, subtrai-se a média histórica daquele dia e divide-se pelo desvio padrão correspondente, gerando séries sem a componente sazonal anual dominante. 

---

## Ferramentas teóricas centrais

### 1. Fogo como sistema dinâmico complexo

* As queimadas são tratadas como um processo dinâmico dependente do tempo, com possíveis características de caos determinístico ou de ruído estocástico correlacionado. 
* O artigo se insere em uma linha de estudos que usa teoria do caos, multifractais e redes complexas para séries temporais ambientais, justificando que o fogo nos biomas brasileiros não deve ser modelado como ruído branco independente. 

### 2. Redes complexas aplicadas a séries temporais

* Ideia geral: transformar uma série temporal em uma rede, na qual cada observação vira um nó e conexões entre nós são definidas por um critério geométrico ou de similaridade. 
* Três classes principais de métodos são destacadas:

  * Redes de proximidade (similaridade);
  * Grafos de visibilidade (VG, HVG);
  * Redes de transição (probabilidades de transição entre estados). 

### 3. Grafo de Visibilidade Horizontal (HVG)

* HVG é um subgrafo do Grafo de Visibilidade Natural (VG), definido por um critério geométrico simples:

  * Cada dado da série temporal $x_i$ vira um nó $i$.
  * Dois nós $i$ e $j$ são ligados se for possível traçar uma linha horizontal entre $x_i$ e $x_j$ sem “passar por baixo” de qualquer ponto intermediário; formalmente, há aresta $(i,j)$ se

    * $x_i, x_j > x_n$ para todo $n$ tal que $i < n < j$. 
* O HVG preserva estrutura temporal e relativa de amplitudes da série, permitindo caracterizar o tipo de processo gerador por meio de medidas topológicas.

### 4. Índices topológicos do HVG

Para cada rede construída a partir da série de focos, são calculados:

1. **Distribuição do grau do nó e Coeficiente λ** 

   * A distribuição de graus $p(k)$ segue comportamento aproximadamente exponencial:

     * $p(k) \sim \exp(-\lambda k)$.
   * A inclinação λ permite classificar o tipo de processo:

     * $\lambda < \ln(3/2)$ → processo caótico;
     * $\lambda = \ln(3/2)$ → processo não correlacionado (tipo ruído branco);
     * $\lambda > \ln(3/2)$ → processo estocástico correlacionado (com memória); quanto maior λ acima desse limiar, maior a correlação.

2. **Coeficiente de Agrupamento (Clustering)** 

   * Mede a tendência dos nós em formarem “grupos” densamente conectados (densidade local de triângulos).
   * É calculado para cada nó a partir das conexões entre seus vizinhos imediatos, e depois se obtém a média na rede.
   * Interpretação qualitativa: redes com maior agrupamento refletem dinâmicas nas quais estados próximos no tempo se conectam mais intensamente entre si.

3. **Comprimento Médio do Caminho (Average Path Length)** 

   * Média das distâncias mínimas entre todos os pares de nós da rede.
   * Interpretação: quantifica quão “integrada” é a rede; caminhos médios menores indicam redes mais conectadas (informação se propaga com poucos passos).

Essas três medidas, combinadas, permitem identificar se a série temporal de focos de calor em cada bioma é governada por um processo aleatório simples, por um ruído com correlações de longo alcance ou por uma dinâmica caótica.

---

## Resultados relevantes para a fundamentação teórica

### 1. Cerrado como bioma com maior incidência relativa de focos

* Tabela 1 mostra número de focos, área de cada bioma e a razão focos/km² (2002–2019): 

  * Amazônia: 2.127.546 focos; 0,51 focos/km².
  * Cerrado: 1.301.723 focos; **0,64 focos/km² (maior valor relativo)**.
  * Mata Atlântica: 0,32 focos/km².
  * Caatinga: 0,39 focos/km².
* Isso reforça teoricamente a caracterização do Cerrado como hotspot de fogo em termos relativos, justificando o recorte do TCC nesse bioma.

### 2. Estrutura da dinâmica temporal dos focos de calor

* As redes HVG construídas a partir das séries **diárias de contagem** e de **anomalias** mostram: 

  * Coeficientes λ para Amazônia, Cerrado e Mata Atlântica indicam **processos estocásticos correlacionados** (λ > ln(3/2)) tanto nas séries de contagem quanto nas de anomalias.
  * Para a série de **anomalias da Caatinga**, λ assume valor compatível com **processo caótico**, indicando dinâmica mais irregular.
* Interpretação teórica:

  * Focos de calor nos grandes biomas não se comportam como ruído branco independente; há **correlações de longo alcance** e estrutura temporal na série.
  * No Cerrado, tanto a alta incidência relativa quanto o padrão de correlação reforçam a visão de um regime de fogo estruturado, com memória, adequado a modelagem preditiva.

### 3. Relação entre topologia, número de focos e área

* Coeficiente de Agrupamento para séries de contagem é maior em Amazônia e Cerrado, biomas com maior número de focos absolutos. 
* Comprimento médio do caminho e λ seguem a ordem de crescimento da razão focos/km²: quanto menor essa razão, mais “conectada” e menos correlacionada é a dinâmica (em termos das métricas HVG). 
* Para anomalias, o padrão se repete, agora em função do número de focos totais, confirmando a sensibilidade dos índices ao regime de fogo de cada bioma.

---

## Pontos-chave para usar na seção “Trabalhos anteriores”

Para a fundamentação teórica do TCC, este artigo oferece:

1. **Base conceitual de que o fogo é um sistema complexo com memória**

   * Mostra, com técnica formal (HVG + λ), que as séries diárias de focos de calor em Amazônia, Cerrado e Mata Atlântica são processos estocásticos correlacionados, não ruído branco. 
   * Esse resultado sustenta a tese de que a ocorrência de fogo é estruturada no tempo, o que justifica o uso de modelos de aprendizado de máquina que exploram padrões e dependências temporais.

2. **Uso de BDQueimadas e AQUA_M-T como padrão de monitoramento**

   * Reafirma a legitimidade do uso de BDQueimadas e do satélite de referência AQUA_M-T para construir séries temporais de focos por bioma, exatamente como proposto no TCC. 

3. **Caracterização do Cerrado como bioma de maior pressão relativa de fogo**

   * Ao apresentar o Cerrado com a maior razão focos/km² entre os biomas analisados, contribui para a justificativa do recorte espacial do TCC.

4. **Referência metodológica avançada (redes complexas) sem modelagem preditiva**

   * O trabalho aprofunda a caracterização da dinâmica temporal usando redes complexas, mas **não constrói modelos de previsão**.
   * Isso abre espaço para posicionar o TCC como um passo seguinte: em vez de apenas classificar o processo como correlacionado ou caótico, utilizar variáveis ambientais e algoritmos de ML para **prever a ocorrência de focos no Cerrado**, sobre uma base (BDQueimadas) já estudada sob o ponto de vista de séries temporais.

Em síntese, o artigo é útil na seção “Trabalhos anteriores” para:
(i) consolidar a visão do fogo como série temporal com correlações e memória;
(ii) reforçar a importância do Cerrado;
(iii) legitimar o uso de BDQueimadas/AQUA_M-T;
(iv) mostrar que já existem análises sofisticadas de dinâmica temporal, mas ainda há lacuna em modelagem preditiva baseada em aprendizado de máquina.
