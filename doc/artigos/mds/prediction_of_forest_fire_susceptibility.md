# Freitas25-TXingu — Prediction of Forest Fire Susceptibility Using Machine Learning Tools in the Triunfo do Xingu Environmental Protection Area, Amazon, Brazil (2025)

Autores: Kemuel M. Freitas; Ronie S. Juvanhol; Christiano J. G. Pinheiro; Anderson A. M. Meneses. Local/Periódico/Conferência: Journal of South American Earth Sciences 153:105366. Link/DOI: 10.1016/j.jsames.2025.105366.

## Ficha técnica

Objeto/Região/Período: APA Triunfo do Xingu, Pará, Brasil; 2010–2020.
Tarefa/Alvo: suscetibilidade a incêndios (regressão da densidade kernel de queimadas validadas) e classificação em classes de risco (Jenks).
Variáveis: ambiente/topografia/socioeconômicas: Altitude, Slope, Aspect, TWI, Precipitação (CHIRPS), Temperatura (LST Landsat), Distância a áreas habitadas, Distância a rodovias, Uso e cobertura da terra (MapBiomas, coleção 8), VCF (MOD44B), NDVI (Landsat).
Modelos: Random Forest Regressor; XGBoost Regressor.
Dados & Split: densidade kernel construída a partir de 15.291 pontos de queimada confirmados (2010–2020); grid de 250 m convertido em pontos; split 80% treino/teste com 10‑fold CV, 20% validação hold‑out; GridSearchCV para hiperparâmetros.
Métricas: RMSE, MAE, R² (treino/CV e validação); teste de Mann‑Whitney para comparar modelos.
Pré-processamento: imputação de faltantes (média/moda); integer encoding para categóricas; reprojeções e composições GEE; kernel quartic; resolução raster 250 m.
Código/Reprodutibilidade: Python 3.10 (scikit‑learn, xgboost), Spyder; QGIS 3.28; parâmetros ótimos reportados; dados sob solicitação.

## Trechos literais do artigo

> “This research aims to map areas susceptible to forest fires … employing machine learning algorithms” (Freitas et al., 2025).
> “15,291 validated active fire … between 2010 and 2020” (Freitas et al., 2025).
> “the XGBoost algorithm was chosen … due to lower computational cost” (Freitas et al., 2025).
> “The environmental and socioeconomic variables had greater importance” (Freitas et al., 2025).
> “areas of high and very high susceptibility occupying 39% of the total area” (Freitas et al., 2025).
> “Precipitation 70.45% … Distance from Inhabited areas 15.02% … Land Use 7.87%” (Freitas et al., 2025, Table 4).
> “RMSE = 35.73, MAE = 18.74, and R² = 0.99” (Freitas et al., 2025, CV).
> “very high … 13.05% of the total area” (Freitas et al., 2025, Fig. 11).

## Leitura analítica e crítica

Metodologia: O alvo é contínuo (densidade kernel de queimadas validadas) e, depois, discretizado por Jenks em cinco classes de suscetibilidade. A escolha de regressão sobre densidade suavizada facilita mapeamento contínuo, mas pode induzir autocorrelação espacial e herdar vieses de escolha de raio e função kernel. As features cobrem clima, uso do solo e topografia; clima é anualizado (precipitação média anual e LST anual), o que sacrifica variações intra‑sazonais e defasagens críticas para ignição e propagação. Treino com 10‑fold CV aleatório em pontos e validação hold‑out aleatória de 20% não impõem blocos espaciais ou temporais, o que eleva risco de vazamento espacial, dada a proximidade entre pixels de 250 m e a própria suavização por kernel. Hiperparâmetros via grid: RF n_estimators 100–250; max_depth 10–None; XGB com n_estimators 250, max_depth 20, learning_rate 0,1 como melhor configuração. Comparação entre modelos com teste de Mann‑Whitney é positiva, mas faltam IC e repetição sob diferentes seeds.

Resultados: Desempenho alto e muito próximo entre RF e XGB no CV (R² ≈ 0,993; RMSE ≈ 35–36) e validação (R² ≈ 0,993; RMSE ≈ 34). O ganho prático que motivou a escolha de XGB foi tempo de execução e escalabilidade, não acurácia. A importância global concentra‑se em Precipitação (70,45%), Distância a áreas habitadas (15,02%) e Uso da terra (7,87%); a contribuição média por SHAP confirma sinal negativo de precipitação e papel antrópico (maior proximidade urbana aumenta suscetibilidade). O mapa final destaca faixas centrais leste‑oeste como alto/altíssimo risco; altíssimo ocupa 13,05% e alto+altíssimo 39% da APA, coerente com pressão antrópica e pastagens dominantes nas classes mais suscetíveis.

Limitações: (i) Validação não espacializada nem temporal, susceptível a superestimação de generalização; (ii) Alvo derivado de kernel de pontos históricos pode reforçar hotspots passados e reduzir sensibilidade a mudanças futuras; (iii) Agregação climática anual e NDVI mediano 2010–2020 perdem sazonalidade e extremos; (iv) Métricas só de regressão, sem avaliação centrada na decisão para detecção de positivos raros (PR‑AUC/F1 por classe) após discretização; (v) Integer encoding para LULC pode induzir ordens artificiais; one‑hot seria preferível; (vi) Falta de análise de sensibilidade ao raio kernel e à resolução; (vii) Reprodutibilidade parcial, sem código aberto.

Qualidade: Escopo bem delineado, pipeline claro e fundamentado em dados oficiais (INPE, MapBiomas, CHIRPS). Justificativa transparente da escolha de XGB por custo computacional. Boas descrições de variáveis e mapas; porém, estatística de incerteza é restrita e a validação poderia ser mais rigorosa com blocos espaço‑temporais.

## Relação com o TCC

Relevância: alta.
Por que importa: Estudo brasileiro recente com BD Queimadas e variáveis ambientais análogas às disponíveis para o TCC. Evidencia a dominância da precipitação e do contexto antrópico, e utiliza SHAP, reforçando nossa ênfase em interpretabilidade. Serve de benchmark regional amazônico e de alerta sobre vazamento espacial em CV aleatória.

## Tabela resumida

| Item                | Conteúdo                                                                                                          |
| ------------------- | ----------------------------------------------------------------------------------------------------------------- |
| Variáveis           | Precipitação, Temperatura (LST), NDVI, VCF, LULC, Dist. a áreas habitadas/rodovias, Altitude, Slope, Aspect, TWI  |
| Modelos             | RF Regressor; XGBoost Regressor                                                                                   |
| Validação           | 10‑fold CV aleatório em pontos; 20% hold‑out aleatório                                                            |
| Métricas principais | CV XGB: RMSE ≈ 35,7; MAE ≈ 18,7; R² ≈ 0,993. Validação: RMSE ≈ 34,0; MAE ≈ 18,1; R² ≈ 0,993                       |
| Melhor desempenho   | XGBoost escolhido por custo de execução; acurácia similar ao RF                                                   |
| Limitações          | Sem blocos espaço‑temporais; alvo kernel; clima anualizado; discretização sem PR‑AUC; encoding categórico simples |

## Itens acionáveis para o TCC

1. Substituir CV aleatória por **validação por blocos espaço‑temporais** e teste fora da região; quantificar o otimismo da CV aleatória vs espacial.
2. Modelar **ocorrência diária** (binária) e comparar com alvo de densidade; reportar **PR‑AUC**, F1 e curvas PR após discretização por risco.
3. Testar **janelas climáticas sazonais e defasadas** (ex.: acumulados de precipitação 7–30 dias, SPEI, KBDI) e comparar com médias anuais.
4. Para LULC, usar **one‑hot** e avaliar granularidade MapBiomas; executar **SHAP** para dependências e heterogeneidade local.
5. Rodar **análise de sensibilidade** ao raio kernel e à resolução (250 m vs 500 m) e documentar impactos na importância de variáveis e PR‑AUC.

---
