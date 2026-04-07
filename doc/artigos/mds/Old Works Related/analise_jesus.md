# Jesus2020-Incidência – Análise da incidência temporal, espacial e de tendência de fogo nos biomas e UCs do Brasil (2003–2017)

## Ficha técnica

Autores: Janisson Batista de Jesus; Cristiano Niederauer da Rosa; Íkaro Daniel de Carvalho Barreto; Milton Marques Fernandes. 
Periódico: Ciência Florestal, v. 30, n. 1, p. 176–191, 2020.
Título: “Análise da incidência temporal, espacial e de tendência de fogo nos biomas e unidades de conservação do Brasil”.

Biomas analisados: Amazônia, Caatinga, Cerrado, Mata Atlântica, Pantanal, Pampa.
Período: 2003–2017.

---

## Objeto do estudo e bases de dados

* Objetivo: analisar o comportamento temporal, espacial e a tendência estatística das ocorrências de fogo em todos os biomas brasileiros e nas Unidades de Conservação (UCs), identificando padrões de distribuição e sazonalidade. 
* Focos de fogo:

  * Fonte: Programa Queimadas / INPE (BDQueimadas).
  * Satélite: AQUA_M-T como satélite de referência (série consistente para análise temporal). 
  * Período: 2003–2017, com análise anual e mensal.
* UCs:

  * Fonte: Cadastro Nacional de Unidades de Conservação (MMA).
  * Inclui categorias de Proteção Integral e Uso Sustentável, em âmbitos federal, estadual e municipal. 

Conexão com o TCC: consolida o uso do AQUA_M-T/BDQueimadas como base padrão para séries temporais de fogo e mostra como agregar e analisar focos por bioma e por UCs, alinhado ao recorte em Cerrado.

---

## Ferramentas conceituais e metodológicas importantes

1. **Fogo como fenômeno espaço-temporal com regime próprio**

   * O estudo parte da ideia de “regimes de fogo”: padrões de intensidade, sazonalidade, tamanho e tempo de retorno em múltiplas escalas espaciais, dependentes de clima, material combustível e uso do solo. 
   * Reforça que compreender o padrão espaço-temporal do fogo (quando e onde queima) é pré-condição para qualquer esforço de previsão, planejamento e manejo.

2. **Monitoramento orbital contínuo (BDQueimadas / INPE)**

   * BDQueimadas é tratado como infraestrutura nacional de monitoramento: detecção de focos, cálculo e previsão de risco de fogo via satélite. 
   * A escolha do satélite de referência AQUA_M-T é apresentada como requisito para séries temporais consistentes – exatamente a lógica que fundamenta o uso dessa mesma fonte no TCC.

3. **Análise espacial: densidade Kernel**

   * Aplicação do estimador de densidade Kernel para mapear “manchas” de maior intensidade de fogo, a partir da média da série temporal. 
   * Conceito teórico: o fogo é modelado como um processo pontual no espaço (localização de focos), cuja densidade pode ser suavizada e interpretada em termos de hotspots regionais.
   * Resultado fundamental: identifica o Arco do Desmatamento (transição Amazônia–Cerrado) e a porção norte do Cerrado (Tocantins, Jalapão, Ilha do Bananal) como áreas de altíssima densidade de fogo. 

4. **Análise temporal: Mann-Kendall e Hurst (tendência x variabilidade natural)**

   * O estudo usa o teste de Mann-Kendall para detectar tendência de longo prazo na série de focos por bioma, e o expoente de Hurst para verificar “memória de longo alcance” (persistência). 
   * Conceito central:

     * H > 0,5 indica persistência; o processo “lembra” estados passados e repete padrões.
     * A combinação MK + Hurst (versão MK-PLA) permite separar tendências genuínas de longuíssimo prazo de flutuações naturais com memória.
   * Resultado:

     * Todos os biomas apresentam H > 0,5, indicando memória/persistência na série de focos;
     * Após corrigir para persistência, não há tendência estatisticamente significativa de aumento de focos na maioria dos biomas, exceto um crescimento natural no Pampa. 
   * Implicação teórica para o TCC: o fogo não se comporta como ruído branco; há sazonalidade e memória na série, o que reforça a pertinência de modelos temporais e de ML que capturem padrões persistentes e estruturados, em vez de assumir independência entre observações.

5. **Relação clima–vegetação–fogo**

   * A discussão articula:

     * influência da sazonalidade de chuva e seca (anos secos x úmidos, episódios El Niño/La Niña) na área queimada;
     * papel do tipo de vegetação e do material combustível (carga, estrutura, aridez) na propagação do fogo;
     * efeito das atividades antrópicas (desmatamento, manejo agropecuário, queima de pastagens). 
   * Conceito importante: regimes de fogo são controlados simultaneamente por clima (chave para o TCC, via INMET), estrutura de combustível e uso da terra.

---

## Resultados centrais que fundamentam o TCC

1. **Cerrado como bioma crítico em área queimada**

   * Em número total de focos (2003–2017): Amazônia > Cerrado, mas o Cerrado registra a maior área queimada acumulada (≈ 2,69 milhões km², contra ≈ 1,38 milhões km² na Amazônia). 
   * Interpretação: vegetação mais sazonal e com déficit hídrico maior permite que fogo se propague por áreas mais extensas, reforçando o Cerrado como hotspot de fogo e justificando o foco do TCC nesse bioma.

2. **Sazonalidade marcada (junho–dezembro, pico em setembro)**

   * Todos os biomas apresentam aumento acentuado de focos entre junho e dezembro; o pico nacional ocorre em setembro (Cerrado, Amazônia, Mata Atlântica, Pantanal). Caatinga tem pico em outubro; Pampa, em agosto. 
   * Implicação: a probabilidade de ocorrência de fogo é fortemente sazonal, o que deve ser incorporado na modelagem (features de mês/estação, variáveis climáticas sazonais).

3. **Distribuição espacial concentrada em hotspots**

   * Alta densidade de fogo na transição Amazônia–Cerrado, especialmente no “Arco do Desmatamento” (Maranhão, Pará) e no norte do Cerrado (Tocantins). 
   * Outras áreas críticas: regiões específicas na Caatinga, Mata Atlântica, Pantanal e Pampa, sempre relacionadas a combinações de clima seco, tipo de vegetação e uso do solo.
   * Para o TCC: indica que modelos espaciais ou espaço-temporais devem considerar heterogeneidade regional dentro do Cerrado, não tratando o bioma como homogêneo.

4. **Unidades de Conservação e vulnerabilidade**

   * UCs na Amazônia e no Cerrado concentram a maior quantidade de focos, com destaque para Áreas de Proteção Ambiental de Uso Sustentável (APA Triunfo do Xingu, Arquipélago do Marajó, Ilha do Bananal, Rio Preto etc.). 
   * Conceito: mesmo áreas sob proteção legal apresentam intensos regimes de fogo, o que reforça a importância de ferramentas preditivas e de monitoramento para apoio à gestão.

5. **Tendência estatística e “naturalidade” do processo**

   * Após considerar a persistência de longo alcance, não há evidência de tendência crescente antrópica generalizada na série de focos para a maioria dos biomas; as variações interanuais são interpretadas como compatíveis com variabilidade climática e regimes naturais modulados por uso do solo. 
   * Isso reforça a visão de que o foco do TCC não é “provar” que os incêndios estão aumentando, mas modelar a probabilidade de ocorrência em função de condições ambientais, dentro de um regime já estabelecido.

---

## Limitações e espaço para o TCC

* Natureza descritiva: o estudo aplica estatística clássica (Kernel, Mann-Kendall, Hurst), mas não utiliza modelos preditivos multivariados ou algoritmos de aprendizado de máquina.
* Ausência de variáveis climáticas explícitas na modelagem: a relação clima–fogo é discutida conceitualmente e por referência à literatura, não via integração direta com séries meteorológicas do INMET.
* Resolução temporal: foco em agregados mensais e anuais, sem exploração de previsões em escala diária ou submensal.

Essas limitações abrem espaço para o TCC: usar a mesma base fundamental (BDQueimadas, Cerrado como bioma crítico, sazonalidade e memória do fogo), mas avançando para modelos de aprendizado de máquina alimentados por variáveis climáticas e ambientais em maior resolução temporal para previsão da ocorrência de focos.
