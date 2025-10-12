# Andri24-Review - Machine Learning and Deep Learning for Wildfire Spread Prediction: A Review (2024)

Autores: Henintsoa S. Andrianarivony; Moulay A. Akhloufi. Local/Periódico/Conferência: Fire (MDPI), 7(12):482. Link/DOI: 10.3390/fire7120482.

## Ficha técnica

Objeto/Região/Período: Revisão sistemática de métodos de previsão de propagação de incêndios (sem recorte temporal; artigos acessíveis em IEEE/Scopus/Scholar).
Tarefa/Alvo: previsão de propagação (frente/perímetro, máscara de queima, taxa de expansão) com ML/DL.
Variáveis: síntese de usos recorrentes - meteorologia (temperatura, umidade, vento, precipitação), vegetação/combustível (NDVI/LAI/umidade do combustível), topografia (declividade, aspecto, elevação), antrópicas (densidade populacional, estradas), máscaras de fogo anteriores.
Modelos: ML (SVM, ensembles como RF/XGBoost, GPR, SVR); DL (CNN, ConvLSTM/CRN, U‑Net, Transformers/Swin‑U‑Net, RL, GNNs); híbridos com autômatos celulares/simuladores (FARSITE, Prometheus, Spark) e data‑driven emulados.
Dados & Split: revisão de benchmarks (Next Day Wildfire Spread; GeoMAC/Landsat; LANDFIRE; VIIRS) e dados simulados (Rothermel/FARSITE). Sem novo experimento.
Métricas: classificação (acurácia, precisão, recall, F1, PR‑AUC); espaciais (IoU/Jaccard, Sorensen–Dice); regressão (MAE, RMSE, MAPE); ênfase na dificuldade de comparabilidade entre estudos.
Pré-processamento: discussão de normalização, discretização, engenharia multimodal, mas sem protocolo único.
Código/Reprodutibilidade: não há repositório; compila referências e lacunas de benchmarks/padrões.

## Trechos literais do artigo

> “DL approaches […] excel at handling the spatiotemporal complexities of wildfire data.” (Andrianarivony & Akhloufi, 2024, p. 2–3).
> “Classification metrics […] accuracy, precision and recall, and F1‑score.” (Andrianarivony & Akhloufi, 2024, p. 4).
> “Spatial metrics such as Intersection over Union (IoU) […] and Sorensen–Dice Coefficient are utilized.” (Andrianarivony & Akhloufi, 2024, p. 4).
> “High‑quality data are essential to build efficient and reliable […] models for fire spread prediction.” (Andrianarivony & Akhloufi, 2024, p. 24–28).
> “deep learning models exhibit the best and most significant accuracy in terms of fire spread prediction.” (Andrianarivony & Akhloufi, 2024, p. 28–29).

## Leitura analítica e crítica

Metodologia: Revisão estruturada com protocolo (palavras‑chave, PRISMA, 37 estudos). Organiza por famílias de modelos (ML tradicionais; DL CNN/CRN; Transformers; RL; GNN) e por dados (tabulares vs. sensoriamento remoto; dados simulados + testes em fogo real). A discussão de métricas é útil, mas faltam recomendações firmes para cenários altamente desbalanceados (PR‑AUC aparece, porém sem pautar thresholds operacionais). Ponto forte: visão integrada multimodal e ênfase em datasets/benchmarks; ponto fraco: não prescreve um protocolo de validação espaço‑temporal padronizado, nem trata MAUP/UGCoP explicitamente.

Resultados: A revisão compila evidências de desempenho alto para DL em curtos horizontes (CNN/ConvLSTM/U‑Net) e mostra crescimento de Transformers e arquiteturas com atenção. Relata avanços com dados híbridos (simuladores + casos reais) e modelos em tempo quase real. Aponta variáveis meteorológicas e de combustível como centrais; dados de manejo e intervenções são raros e recomendados como prioridade futura.

Limitações: (i) Comparabilidade limitada por métrica heterogênea e ausência de benchmarks amplamente aceitos; (ii) Transferência regional incerta e pouca avaliação fora de EUA/Europa/China; (iii) Pouca padronização de validação por blocos espaço‑temporais; (iv) Falta de métricas centradas em decisão (lead time, falso alarme operacional); (v) Transparência variando - poucos estudos com explicabilidade consistente.

Qualidade: Síntese ampla, bem organizada e atualizada; boa curadoria de datasets e famílias de modelos. Faltam diretrizes prescritivas para evitar vazamento espaço‑temporal e para reportar incerteza. Útil como mapa do campo e agenda de pesquisa.

## Relação com o TCC

Relevância: média‑alta.
Por que importa: Oferece panorama de métricas, datasets e arquiteturas que podem ser adaptados à previsão diária de ocorrência/propagação com variáveis climáticas do INMET + BD Queimadas. Fornece argumentos para incluir métricas espaciais (IoU/Dice) e PR‑AUC, e para testar arquiteturas CNN/ConvLSTM como linha de base de spread, além de RF/XGB/SVM para ocorrência.
Se baixa relevância: não se aplica - foco é propagação, mas os insumos e recomendações (métricas, dados, explainability) dialogam diretamente com o TCC.

## Tabela resumida

| Item                | Conteúdo                                                                                                |
| ------------------- | ------------------------------------------------------------------------------------------------------- |
| Variáveis           | Clima (T, UR, precip., vento), vegetação/combustível (NDVI/LAI/umidade), topografia, fatores antrópicos |
| Modelos             | ML (RF, XGB, SVM, GPR, SVR); DL (CNN, ConvLSTM/U‑Net, Transformers, RL, GNN)                            |
| Validação           | Heterogênea; uso frequente de dados simulados + testes; lacuna em blocos espaço‑temporais padronizados  |
| Métricas principais | F1, PR‑AUC, acurácia, IoU/Dice; MAE/RMSE para área/velocidade                                           |
| Melhor desempenho   | DL em horizontes curtos (ConvLSTM/CNN) reporta F1 alto; Transformers emergentes                         |
| Limitações          | Falta de benchmarks unificados, generalização espacial, pouca explicabilidade operacional               |

## Itens acionáveis para o TCC

1. Adotar **PR‑AUC** como métrica principal para ocorrência e **IoU/Dice** para mapas de spread; relatar também **lead time** e taxa de falsos alarmes operacionais.
2. Implementar **validação por blocos espaço‑temporais** e teste fora‑da‑região; registrar risco de MAUP/UGCoP e fazer análise de sensibilidade de resolução (0,05° vs 0,1°) e janela temporal.
3. Considerar um experimento **híbrido**: treinar/pré‑treinar com dados simulados (Rothermel/FARSITE) e calibrar com BD Queimadas + INMET; incluir **SHAP**/Grad‑CAM para interpretabilidade.

---
