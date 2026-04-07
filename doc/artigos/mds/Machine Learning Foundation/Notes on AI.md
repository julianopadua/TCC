# Alcantara2024 - Notes on artificial intelligence: concepts, applications and techniques (2024)

## Ficha técnica

Autores: Fabíola Alves Alcântara; Eugênio Silva; Rodrigo Siqueira-Batista.  
Local/Periódico: Revista Sociedade Científica, v. 7, n. 1, p. 2970-3008.  
DOI: 10.61411/rsc202457217.  
Escopo: Revisão narrativa sobre conceitos de Inteligência Artificial, interseções com outras áreas, técnicas de Aprendizado de Máquina e aplicações, com foco em aprendizado supervisionado e problemas de classificação.  
Palavras-chave centrais: Inteligência Artificial; Aprendizagem de Máquina; Aprendizagem Supervisionada; Problemas de Classificação.

---

## Trechos centrais (literais, para citação direta)

> [cite_start]"Artificial Intelligence (AI) has been increasingly present in the contemporary world, with applicability in various fields of knowledge."[cite: ???]

> [cite_start]"Machine learning is the science (and art) of programming computers so that they can learn from data."[cite: ???]

> [cite_start]"Machine learning techniques are mainly used to solve problems involving phenomena for which there are no known analytical models that adequately represent them."[cite: ???]

---

## Leitura crítica e fundamentação teórica

### 1. Conceito de Inteligência Artificial e posição entre as áreas

O artigo parte da decomposição etimológica de Inteligência Artificial: inteligência como capacidade de aprender e aplicar o que foi aprendido; artificial como aquilo que é produzido por seres humanos, isto é, um artefato técnico. A partir daí, os autores sintetizam IA como a capacidade de artefatos computacionais executarem tarefas que, historicamente, exigiriam inteligência humana para serem realizadas, como perceber padrões, aprender com a experiência, inferir e tomar decisões sob incerteza.

Com base em Russell e Norvig, o texto organiza as definições de IA em quatro perspectivas complementares: agir como humanos, pensar como humanos, agir racionalmente e pensar racionalmente. Em todas elas, o foco recai sobre sistemas artificiais capazes de desempenhar funções que envolvem raciocínio, aprendizado, percepção, planejamento e adaptação em domínios específicos. A IA é apresentada como campo interdisciplinar, apoiado em filosofia, lógica, matemática, estatística, biologia, psicologia, engenharia e linguística, entre outras.

Do ponto de vista das aplicações, o artigo reforça que soluções baseadas em IA já estão disseminadas em tradução automática, logística, finanças, medicina, reconhecimento de voz e imagens, jogos, robótica e apoio à decisão. Isso fundamenta a ideia de que IA não é apenas especulação teórica ou ficção científica, mas um conjunto de técnicas maduras de ciência, engenharia e matemática aplicadas a problemas reais de alta complexidade.

Para o TCC, essa seção permite enquadrar o trabalho dentro do guarda-chuva da IA como uma aplicação de suporte à decisão em ambiente ambiental: o objetivo não é construir uma “máquina pensante” geral, mas um sistema especializado capaz de antecipar focos de queimadas a partir de dados climáticos e contextuais, colaborando com a gestão de risco.

### 2. Aprendizado de máquina: definição e paradigmas

O artigo trata aprendizado de máquina (AM/ML) como subárea central da IA, responsável por métodos que permitem que um sistema melhore seu desempenho em uma tarefa a partir da experiência, isto é, a partir de dados. Em vez de programar explicitamente todas as regras de decisão, aprende-se um modelo que extrai padrões relevantes de exemplos e os utiliza para fazer previsões sobre novos casos.

Os autores citam definições clássicas, que convergem para três ideias principais:

- o computador ajusta seu comportamento para melhorar a acurácia em uma tarefa,
- o aprendizado é orientado pelos dados disponíveis,
- ML é especialmente indicado quando não há modelos analíticos conhecidos que representem bem o fenômeno.

Em seguida, o texto organiza os paradigmas de aprendizado em função do tipo de feedback e tarefa:

- Aprendizado supervisionado: usa exemplos rotulados, com atributos descritivos e um alvo; inclui tarefas de classificação (alvo categórico) e regressão (alvo numérico).
- Aprendizado não supervisionado: trabalha sem rótulos, buscando descobrir estrutura nos dados, como agrupamentos ou componentes latentes.
- Paradigmas adicionais: semi supervisionado, ativo e por reforço, em que o modelo combina rótulos escassos com muitos dados não rotulados, escolhe quais exemplos rotular ou aprende por meio de interações com um ambiente recebendo recompensas e penalidades.

O artigo enfatiza o paradigma supervisionado, com foco em tarefas de classificação, discutindo a ideia de generalização: o objetivo é construir modelos que não apenas reproduzam os rótulos do conjunto de treino, mas que consigam prever adequadamente o alvo de novas instâncias geradas pelo mesmo processo.

### 3. Famílias de algoritmos de ML e sua relevância para o problema do TCC

Uma contribuição importante do artigo é a descrição de diferentes famílias de algoritmos de ML usadas em classificação supervisionada:

- Métodos baseados em distância: exemplificados pelo k-nearest neighbors (k-NN), que rotula uma nova instância com base nos vizinhos mais próximos no espaço de atributos.
- Métodos probabilísticos: baseados no Teorema de Bayes, como Naive Bayes, que estimam probabilidades condicionais para inferir o rótulo mais provável.
- Métodos baseados em busca: como árvores de decisão, que constroem uma estrutura hierárquica de testes em atributos, gerando regras se-então interpretáveis.
- Métodos baseados em otimização: como redes neurais artificiais e máquinas de vetores de suporte (SVM), que ajustam parâmetros para maximizar alguma função de desempenho, normalmente tratada como problema de otimização.
- Sistemas híbridos: combinações de diferentes técnicas (por exemplo, árvores de decisão com Naive Bayes nas folhas), buscando reunir vantagens complementares e melhorar a capacidade de modelar problemas complexos.

Essas famílias de algoritmos formam o repertório clássico a partir do qual se escolhem modelos para problemas de previsão em dados tabulares. O artigo discute ainda que técnicas modernas de deep learning e sistemas híbridos têm sido empregadas com sucesso em domínios de grande complexidade, o que reforça o papel central de ML dentro da IA aplicada.

No contexto do TCC, essa taxonomia de algoritmos fornece base teórica para justificar a seleção de modelos lineares, árvores de decisão e métodos baseados em otimização (como SVM e redes neurais rasas) como candidatos naturais para modelar a relação entre variáveis climáticas e ocorrência de focos de incêndio. Também apoia a ideia de comparação entre diferentes famílias, uma vez que cada linha de método captura de forma distinta a não linearidade e as interações entre atributos meteorológicos.

### 4. Conexão com o problema de previsão de focos de queimadas

O problema central do TCC pode ser formulado, à luz do artigo, como uma tarefa de aprendizado supervisionado:

- cada instância corresponde a uma unidade espaço-temporal (por exemplo, combinação de município, data e hora),
- os atributos descritivos são variáveis climáticas (temperatura, umidade, precipitação, pressão, radiação, vento etc.) e possivelmente atributos derivados,
- o alvo é um rótulo que indica presença ou ausência de foco de queimadas (classificação binária) ou, em variações, uma medida de intensidade ou contagem de focos (regressão).

A dinâmica física de ignição e propagação do fogo em vegetação é altamente complexa, depende de múltiplos fatores e não é adequadamente capturada por modelos analíticos simples. Essa situação é precisamente o tipo de cenário descrito pelo artigo como apropriado para técnicas de ML: há muitos dados observados (BDQueimadas e INMET), mas não há um modelo fechado que relacione de forma exata os atributos de entrada ao risco de foco.

Assim, o uso de aprendizado de máquina na previsão de focos de incêndio se encaixa teoricamente nas seguintes ideias centrais discutidas por Alcântara et al.:

- IA como campo que desenvolve artefatos capazes de apoiar decisões em contextos complexos.
- ML como subárea que aprende, a partir de dados, funções de predição para problemas em que não se conhece um modelo analítico adequado.
- Aprendizado supervisionado como paradigma natural para tarefas onde se dispõe de pares entrada rótulo históricos, como é o caso de séries de focos de calor associados a condições climáticas.
- Famílias de algoritmos de classificação que podem ser testadas e comparadas sobre a mesma base (baseados em distância, probabilidade, busca, otimização e sistemas híbridos).

Em termos de fundamentação, o artigo permite sustentar que a estratégia adotada no TCC corresponde a uma aplicação típica de IA e ML: construir modelos supervisionados, treinados em dados históricos de queimadas e clima, para estimar o risco de ocorrência de novos focos no bioma Cerrado e, assim, apoiar ações de monitoramento e prevenção.

---

## Tabela síntese (Conceitos do artigo → Ação no TCC)

| Conceito em Alcântara et al. (2024) | Argumento teórico para o TCC | Ação prática na modelagem |
| :--- | :--- | :--- |
| IA como artefato capaz de executar tarefas que exigem inteligência | Justifica o uso de modelos computacionais como apoio à decisão em gestão de queimadas, e não apenas como exercício estatístico | Formular o problema como construção de um sistema de apoio à previsão de focos, integrado ao pipeline de dados BDQueimadas + INMET |
| ML como ciência de programar computadores para aprender com dados | Fenômeno de queimadas é complexo e sem modelo analítico fechado; aprender a partir de séries históricas é abordagem adequada | Organizar o dataset histórico (2004 em diante) como base de treino, validação e teste para modelos supervisionados |
| Aprendizado supervisionado e problemas de classificação | Ocorrência de foco em uma unidade espaço-temporal é naturalmente um alvo categórico (foco vs não foco) | Definir o alvo `HAS_FOCO` e estruturar o problema como classificação binária, com possíveis extensões em regressão para intensidade |
| Famílias de algoritmos (distância, probabilidade, busca, otimização, híbridos) | Diferentes famílias capturam padrões distintos; comparação de modelos é esperada em ML | Testar e comparar modelos lineares, árvores de decisão, métodos baseados em distância e em otimização (por exemplo, RF, SVM, redes rasas) em diferentes cenários de tratamento de missing |

---

## Julgamento de relevância para o TCC

Este artigo é uma peça central da fundamentação teórica da seção de Inteligência Artificial e Aprendizado de Máquina do TCC. Ele fornece:

- uma definição conceitualmente consistente de IA e de ML, alinhada com a literatura clássica,
- uma organização clara dos paradigmas de aprendizado, com ênfase em aprendizado supervisionado,
- uma taxonomia de famílias de algoritmos de classificação,
- e uma discussão sobre quando e por que usar ML em problemas sem modelos analíticos adequados.

Com isso, Alcântara et al. (2024) permitem ancorar, em base científica atual, as escolhas metodológicas do trabalho: tratar a previsão de focos de queimadas como problema de aprendizado supervisionado e empregar algoritmos de ML para modelar a relação entre variáveis climáticas e risco de fogo no bioma Cerrado.