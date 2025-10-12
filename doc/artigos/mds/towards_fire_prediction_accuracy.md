# Shu21-DWCNB — Towards Fire Prediction Accuracy Enhancements by Leveraging an Improved Naïve Bayes Algorithm (2021)

Autores: Liang Shu; Haigen Zhang; Yingmin You; Yonghao Cui; Wei Chen. Local/Periódico/Conferência: Symmetry (MDPI). Link/DOI: 10.3390/sym13040530.

## Ficha técnica

Objeto/Região/Período: Previsão/detecção de incêndios em ambiente controlado (laboratório); dados NIST (12 testes, 4972 amostras).
Tarefa/Alvo: classificação de estado do fogo: open flame (OF), smoldering fire (SF), no fire (NF).
Variáveis: temperatura, concentração de fumaça, concentração de CO (3 atributos).
Modelos: Naive Bayes (NB); Double-Weighted Naive Bayes (DWNB); DWCNB (double‑weighted NB com coeficiente de compensação).
Dados & Split: treino 2984, teste 1988 (NIST); avaliações adicionais com 3480 amostras em subconjuntos aleatórios.
Métricas: Precisão (Precision), Recall, F‑measure (F1), Acurácia.
Pré-processamento: discretização, normalização, suavização de Laplace, uso de log para evitar underflow; pesos para atributos e valores; compensação de probabilidade a priori com desenho ortogonal (L16 e L25) para ξ.
Código/Reprodutibilidade: implementação em Python; hardware embarcado (STM32L151) para verificação experimental.

## Trechos literais do artigo

> “a double weighted naive Bayes with compensation coefficient (DWCNB) method is proposed” (Shu et al., 2021).
> “dataset from the National Institute of Standards and Technology (NIST) […] 12 tests and 4972 fire datasets” (Shu et al., 2021).
> “the average prediction accuracy of the proposed method is 98.13%” (Shu et al., 2021).
> “the recall rate of SF was up to 97.35%” (Shu et al., 2021).
> “the ξ values are ξOF = 1.1, ξSF = 2.7, ξNF = 3.3” (Shu et al., 2021).

## Leitura analítica e crítica

Metodologia: O trabalho introduz um NB ponderado em dois níveis (atributos e valores) e adiciona um coeficiente de compensação na probabilidade a priori (ξ) sintonizado via testes ortogonais (L16 e L25). Conjunto de dados pequeno, com apenas três sensores e três classes; tarefa é detecção/estágio de fogo em câmara de combustão. Divide o NIST em treino/teste fixos e realiza comparações com NB e DWNB. A decisão por métricas de classificação (P/R/F1) é coerente para classes múltiplas, mas o contexto é de laboratório, sem variáveis ambientais ou espaciais.

Resultados: DWCNB supera NB e DWNB em acurácia média (≈97–98%) e em P/R/F1 para OF, SF e TF (OF+SF). O ganho é atribuído ao balanceamento entre termos a priori e verossimilhanças e à ponderação dependente da classe. A seleção de ξ via desenho ortogonal indica forte sensibilidade de desempenho a ξOF.

Limitações: Domínio distinto de incêndios florestais; apenas três variáveis sensoriais imediatas, sem clima, vegetação ou contexto humano. Não há validação espaço-temporal nem análise de generalização; testes são intra‑laboratório. Métricas reportadas não incluem PR‑AUC, e a avaliação presume prevalências artificiais do conjunto. Resultados podem não transferir para previsão de queimadas em paisagens abertas.

Qualidade: Apresentação clara das fórmulas e do fluxo do algoritmo; descrição do desenho ortogonal é transparente. Reprodutibilidade parcial (não há repositório de código/dados processados). Estatísticas de incerteza limitadas.

## Relação com o TCC

Relevância: baixa.
Se baixa relevância: Foco em detecção de fogo indoor com sensores específicos (temperatura/fumaça/CO) em ambiente controlado. Não utiliza variáveis climáticas, não aborda validação espaço‑temporal, nem desbalanceamento típico de ocorrência de queimadas. Contribui apenas como referência metodológica sobre ponderação e compensação em NB.

## Tabela resumida

| Item                | Conteúdo                                                                       |
| ------------------- | ------------------------------------------------------------------------------ |
| Variáveis           | Temperatura, fumaça, CO                                                        |
| Modelos             | NB; DWNB; DWCNB                                                                |
| Validação           | Split fixo NIST; tuning de ξ via L16/L25                                       |
| Métricas principais | Precision, Recall, F1, Acurácia                                                |
| Melhor desempenho   | DWCNB, acurácia ≈ 98.1%                                                        |
| Limitações          | Ambiente de laboratório; sem clima/espacial; sem PR‑AUC; generalização incerta |

## Itens acionáveis para o TCC

1. Registrar como referência de técnica: ponderação por atributo/valor e ajuste de coeficiente de classe (ideia adaptável a NB/XGB via class_weight ou prior tuning).
2. Se NB for usado como baseline no TCC, testar variações com pesos por classe e ajuste explícito da prior (ex.: calibrar odds para reduzir FN em detecção precoce).
3. Documentar na seção de “ameaças à validade” que estudos de detecção indoor não são comparáveis a previsão de queimadas baseada em clima/uso do solo.

---
