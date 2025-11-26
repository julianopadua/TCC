# RussellNorvig2021 - Artificial Intelligence: A Modern Approach (4th ed.)

## Ficha técnica

Autores: Stuart J. Russell; Peter Norvig.  
Título: Artificial Intelligence - A Modern Approach (4th Edition).  
Editora: Pearson. Local/ano: Hoboken, 2021.  
Escopo: Livro-texto clássico que apresenta a IA como estudo de agentes inteligentes e abrange busca, conhecimento, raciocínio probabilístico, aprendizado de máquina, visão computacional, linguagem natural, robótica, ética e segurança.  
Partes relevantes para este TCC:  
- Parte I - Artificial Intelligence (cap. 1 e 2): definição de IA e de agente racional.  
- Parte V - Machine Learning (cap. 19 a 22): aprendizado a partir de exemplos, modelos probabilísticos, deep learning e aprendizado por reforço.  

---

## Trecho central (literal, para citação direta)

> [cite_start]"We define AI as the study of agents that receive percepts from the environment and perform actions."[cite: 1.1]

---

## Leitura crítica e fundamentação teórica

### 1. Conceito de Inteligência Artificial em Russell e Norvig

Russell e Norvig formulam IA a partir da noção de agente. Um agente é qualquer entidade que percebe o ambiente por meio de sensores e age sobre esse ambiente por meio de atuadores. A proposta central do livro é estudar agentes inteligentes, isto é, agentes que escolhem ações de modo a maximizar uma medida de desempenho associada ao seu objetivo. Essa perspectiva desloca o foco da pergunta abstrata "o que é pensar" para uma formulação operacional: projetar agentes que se comportem bem em tarefas claramente especificadas.

Os autores organizam a IA em torno de diferentes capacidades - busca, planejamento, raciocínio lógico, raciocínio probabilístico, aprendizado, percepção, linguagem, robótica - mas todas sob o mesmo guarda-chuva de agentes racionais. Um agente é racional quando seleciona ações que, com base nas percepções e no conhecimento disponível, aumentam a probabilidade de atingir seus objetivos. Essa visão dá uma base conceitual unificada: IA é o estudo de métodos para construir agentes racionais em ambientes diversos, sob diferentes formas de incerteza e limitação computacional.

Para o TCC, essa definição permite enquadrar o modelo preditivo de queimadas como parte de um agente inteligente: um sistema que observa o estado climático e espacial e gera ações - no caso, previsões ou alertas - que ajudam a maximizar um objetivo de gestão de risco (por exemplo, reduzir a área queimada ou priorizar recursos de combate).

### 2. Aprendizado de máquina como núcleo da IA moderna

No arcabouço de Russell e Norvig, aprendizado de máquina é um componente dentro da arquitetura de agentes. Um agente pode ser projetado apenas com conhecimento codificado manualmente, mas, na prática, os autores enfatizam que sistemas robustos em ambientes complexos dependem de aprendizagem a partir de dados. Em vez de programar explicitamente todas as regras de decisão, define-se uma classe de modelos e um procedimento que ajusta automaticamente seus parâmetros com base em experiências passadas.

Na Parte V, o livro formaliza aprendizado como o processo pelo qual um agente melhora seu desempenho em uma tarefa à medida que acumula experiências. Em termos práticos, isso significa:

- especificar uma tarefa (por exemplo, prever se haverá foco de incêndio),
- definir uma medida de desempenho (acurácia, sensibilidade, risco esperado),
- e descrever a experiência de treino (histórico de pares entrada-saída).

Quando essas três peças estão claras, o problema é caracterizado como um problema de aprendizado de máquina, e diferentes algoritmos podem ser comparados conforme sua capacidade de generalizar para novos dados.

### 3. Tipos de aprendizado relevantes para o problema de queimadas

Russell e Norvig classificam os tipos de aprendizado principalmente em função da forma da experiência e do tipo de feedback:

- Aprendizado supervisionado: o agente recebe pares entrada-saída, isto é, exemplos rotulados. O objetivo é aprender uma função que mapeia estados de entrada para rótulos de saída. Inclui problemas de classificação (saída categórica) e regressão (saída numérica).
- Aprendizado não supervisionado: não há rótulos; o objetivo é descobrir estrutura nos dados, como agrupamentos, componentes latentes ou densidades.
- Aprendizado por reforço: o agente interage com um ambiente ao longo do tempo, recebendo recompensas ou penalidades e aprendendo políticas de ação que maximizam a recompensa acumulada.

A partir dessa taxonomia, o problema do TCC se encaixa de forma direta no aprendizado supervisionado: existem registros históricos (instâncias) que combinam variáveis de entrada (condições climáticas e espaciais) com um rótulo que indica a ocorrência ou não de um foco de incêndio naquela unidade espaço-temporal. A partir disso, aprendem-se modelos que aproximam uma função de risco de fogo.

Além disso, o livro discute que esses problemas supervisionados podem ser tratados tanto como:

- classificação binária - foco vs não foco em um intervalo de tempo e área, ou  
- regressão - previsão de contagem ou intensidade de focos, se o alvo for numérico.

Essas duas perspectivas aparecem na sua própria construção de datasets (variável `HAS_FOCO` como alvo binário e, potencialmente, variáveis como `FRP` ou contagens agregadas).

### 4. Famílias de algoritmos e sua conexão com o TCC

O capítulo 19 apresenta formas de aprendizado e algoritmos clássicos para aprendizado supervisionado:

- Árvores de decisão e modelos baseados em regras, adequados para dados tabulares e relativamente interpretáveis.
- Modelos lineares (regressão linear e logística), que assumem relação aproximadamente linear entre atributos e alvo, com regularização para controlar complexidade.
- Modelos não paramétricos, como k-vizinhos mais próximos, que usam diretamente as instâncias de treino para classificar novos exemplos.
- Métodos de ensemble, como bagging e boosting, que combinam múltiplos modelos de base para obter melhor desempenho e maior robustez.

Essas famílias de modelos se alinham com o conjunto de algoritmos que você pretende testar sobre as bases `base_A ... base_F` do seu pipeline de modelagem. Em particular, a discussão de árvores, modelos lineares e ensembles fornece um enquadramento conceitual para:

- por que modelos baseados em árvore são atraentes em dados climáticos tabulares com relações não lineares,
- por que regressão logística é um baseline importante e interpretável para classificação de risco,
- e por que métodos de ensemble (como Random Forest ou Gradient Boosting, mesmo quando implementados fora do livro) tendem a capturar melhor interações complexas entre variáveis meteorológicas.

### 5. Aplicação direta ao problema de previsão de focos de incêndio

Usando a linguagem de Russell e Norvig, o seu problema pode ser formulado como:

- ambiente: bioma Cerrado, descrito por variáveis climáticas e de contexto em cada hora e localização;
- perceptos: vetores de atributos construídos a partir da integração BDQueimadas + INMET - temperatura, umidade, pressão, precipitação, radiação, vento, hora do dia, época do ano, entre outros;
- ações: previsões ou estimativas de risco emitidas pelo agente (por exemplo, classificar uma combinação tempo-espaço como de alto ou baixo risco de foco);
- medida de desempenho: métricas de acerto e erro relevantes para gestão de risco, como sensibilidade à ocorrência de focos, precisão, curvas ROC e impacto esperado sobre falsa tranquilidade ou alarmes falsos.

Dentro desse enquadramento, o aprendizado de máquina é o mecanismo que permite ao agente ajustar sua função de decisão a partir de dados históricos. O livro fornece os blocos conceituais para justificar que:

- estamos em um cenário típico de aprendizado supervisionado com dados tabulares,
- há forte motivação para comparar diferentes famílias de algoritmos, em vez de escolher um único modelo a priori,
- e o objetivo final é construir um agente que, ao receber novas condições climáticas e espaciais, emita ações (previsões de foco) que maximizem a chance de decisões corretas no manejo do fogo.

---

## Tabela síntese (Conceitos do livro → Ação no TCC)

| Conceito em Russell e Norvig (2021) | Argumento teórico para o TCC | Ação prática na modelagem |
| :--- | :--- | :--- |
| IA como estudo de agentes que percebem e agem | O modelo preditivo é parte de um agente de apoio à decisão em manejo de queimadas | Formular o sistema como agente que observa clima e emite previsões de risco |
| Racionalidade como maximização de medida de desempenho | Previsões são avaliadas por métricas ligadas à redução de risco e uso eficiente de recursos | Definir métricas de avaliação (acurácia, recall para focos, etc.) alinhadas à gestão de fogo |
| Aprendizado de máquina como melhoria por experiência | Histórico BDQueimadas + INMET fornece a experiência necessária para aprender padrões de risco | Organizar dados históricos em conjunto de treino/validação/teste para modelos supervisionados |
| Aprendizado supervisionado para classificação | Ocorrência de foco é um alvo binário natural em cada unidade espaço-temporal | Tratar `HAS_FOCO` como variável alvo e formular o problema como classificação binária |
| Famílias de modelos (árvores, lineares, não paramétricos, ensembles) | Diferentes famílias capturam padrões distintos e têm compromissos diferentes entre interpretabilidade e desempenho | Comparar regressão logística, árvores de decisão e métodos de ensemble em diferentes cenários de tratamento de missing |

---

## Julgamento de relevância para o TCC

Artificial Intelligence - A Modern Approach é a principal referência clássica para definir inteligência artificial e situar o aprendizado de máquina dentro desse campo. Para o TCC, o livro serve como eixo teórico para:

- justificar formalmente o uso de IA em um problema aplicado de previsão de queimadas,
- definir aprendizado de máquina e seus tipos com rigor conceitual,
- e enquadrar a comparação de algoritmos supervisionados sobre dados climáticos como uma aplicação direta da teoria de agentes racionais e aprendizado a partir de exemplos.

Ele complementa bem artigos mais recentes em português, oferecendo um pano de fundo consolidado e amplamente citado para a seção de fundamentação teórica em IA e ML.