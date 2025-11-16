# The Lack of A Priori Distinctions Between Learning Algorithms (Neural Computation, 1996)

## Ficha técnica

Autor: David H. Wolpert
Tipo: Teórico - formula e demonstra os teoremas No Free Lunch para aprendizado supervisionado
Escopo: erro off-training-set, equivalência média entre algoritmos sob hipóteses gerais, implicações para validação cruzada, escolhas de modelo, VC bounds e análise bayesiana. 

---

## Trechos centrais curtos

> Para quaisquer dois algoritmos A e B, existem tantos alvos para os quais A tem menor erro esperado off-training-set quanto alvos para os quais B tem menor erro. Isso vale para perdas homogêneas como zero-um. 

> A conclusão permanece mesmo quando A é cross-validation e B é anti-cross-validation. 

> Não é válido afirmar que baixa taxa empírica de erro, pequena dimensão VC e grande amostra implicam alta probabilidade de pequeno erro off-training-set sem suposições sobre o alvo. 

---

## Por que importa para a parte de comparação

1. Justificativa de comparar múltiplos modelos e múltiplas bases
   Sem suposições explícitas sobre a distribuição alvo, não há preferência a priori entre algoritmos. Portanto, adoto comparação empírica controlada entre famílias de modelos e entre variações de dataset para fundamentar escolhas.

2. Protocolo de validação e testes estatísticos
   Como a equivalência média depende do desenho e da métrica, preciso reportar médias e variâncias sob validações repetidas, além de testes corrigidos, para que conclusões não dependam de partições específicas.

3. Métricas alinhadas ao objetivo
   A teoria exige explicitar a função de perda. Escolho métricas coerentes com o objetivo do estudo e discuto como decisões de limiar alteram conclusões de ranking entre modelos.

4. Limites de generalização sem pressupostos
   Evito extrapolações fortes do tipo um modelo é superior em geral. Declaro explicitamente o domínio de dados, a distribuição de treino e teste e as condições sob as quais um método supera outro.

---

## Tabela síntese conceito → uso imediato na comparação

| Conceito NFL                              | Risco se ignoro                 | Ação concreta                                                                         |
| ----------------------------------------- | ------------------------------- | ------------------------------------------------------------------------------------- |
| Sem distinção a priori entre algoritmos   | Escolha dogmática de modelo     | Comparar várias famílias sob o mesmo protocolo e dados                                |
| Erro off-training-set como foco           | Otimismo por vazamento          | Partições sem sobreposição e validação estratificada espaço-temporal quando aplicável |
| Cross-validation não é garantia universal | Vantagens espúrias por partição | Repetições, 5x2-CV ou 10-fold repetido, testes corrigidos                             |
| Dependência da métrica de perda           | Ranking inconsistente           | Definir métricas alvo e reportar trade-offs por métrica                               |
| Necessidade de pressupostos explícitos    | Generalizações indevidas        | Declarar hipóteses de dados e limites de inferência                                   |

---

## Onde cito

Seção Metodologia - Comparação de modelos e protocolos de validação. Introduzo os resultados No Free Lunch para justificar um design comparativo rigoroso, múltiplas métricas e análise estatística das diferenças entre modelos. 
