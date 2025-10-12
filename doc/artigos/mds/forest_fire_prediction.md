# Forest Fire Prediction Using Machine Learning Techniques (CONIT 2021)

## Ficha Técnica
- **Autores:** Preeti T., Dr. Suvarna Kanakaraddi, Aishwarya Beelagi, Sumalata Malagi, Aishwarya Sudi  
- **Instituição:** KLE Technological University, Hubli, Karnataka, India  
- **Conferência:** International Conference on Intelligent Technologies (CONIT), 2021  
- **Dados utilizados:** Kaggle – Montesano Natural Park (518 observações, 13 variáveis)  
- **Variáveis climáticas consideradas:** temperatura, umidade relativa, vento, chuva  
- **Modelos avaliados:** Decision Tree (DT), Random Forest (RF), Support Vector Regression (SVR), Artificial Neural Networks (ANN)  
- **Métricas reportadas:** MAE, MSE, RMSE  
- **Melhor resultado:** Random Forest (com RandomizedSearchCV) – MAE = 0.03, MSE = 0.004, RMSE = 0.07

---

## Trechos Centrais do Artigo

> “To predict the occurrences of a forest fire the proposed system processes using the meteorological parameters such as temperature, rain, wind and humidity”.  

> “Random forest regression and Hyperparameter tuning using RandomizedSearchCV algorithm ... gives best results of Mean absolute error (MAE) 0.03, Mean squared error (MSE) 0.004, Root mean squared error (RMSR) 0.07”.  

> “Comparative study of different models for predicting forest fire such as Decision Tree, Random Forest, Support Vector Machine, Artificial Neural networks (ANN)”.  

> “Artificial neural network model ... result are: MAE 0.71, MSE 3.96, RMSE 1.99”.  

> “Meteorological factors (Temperature, Relative Humidity and Wind Speed) are taken into account. Extreme temperatures, moderate humidity, high wind speeds, significantly raise the chance of burning”.  

---

## Leitura Crítica e Correlação com o TCC

Este artigo se alinha diretamente com o objetivo do seu TCC, pois aplica **variáveis exclusivamente climáticas** para previsão de queimadas, sem integrar combustível ou pressão antrópica, o que permite comparação direta com sua proposta inicial de usar **INMET + BD Queimadas**.

O destaque é o **Random Forest**, que superou SVM, Decision Tree e ANN em precisão. Isso reforça achados de outros contextos (China, Paquistão, Brasil) onde RF tende a ser robusto mesmo em datasets pequenos.  

Os resultados negativos da ANN (MAE > 0.7, RMSE ~2) indicam que redes neurais não tiveram desempenho competitivo neste cenário de poucas observações e variáveis tabulares simples, o que conecta à sua hipótese de que modelos mais profundos exigem datasets mais amplos e de maior resolução.

O uso de **RandomizedSearchCV** mostra a importância do ajuste de hiperparâmetros para RF, algo que você já previu em sua metodologia com Bayesian Search.  

Outro ponto: os autores confirmam a relevância de **temperatura, umidade relativa e vento** como variáveis decisivas, em linha com os fatores climáticos que você listou na seção de fundamentação.

---

## Tabela Comparativa (Artigo CONIT 2021)

| Modelo             | Variáveis usadas                       | MAE   | MSE   | RMSE  |
|--------------------|----------------------------------------|-------|-------|-------|
| Decision Tree      | Temp, chuva, umidade, vento            | n/a   | n/a   | n/a   |
| Random Forest      | Temp, chuva, umidade, vento            | 0.03  | 0.004 | 0.07  |
| Support Vector Reg | Temp, chuva, umidade, vento            | n/a   | n/a   | n/a   |
| ANN                | Temp, chuva, umidade, vento            | 0.71  | 3.96  | 1.99  |

---

## Insights para o TCC
1. Random Forest mostra-se sistematicamente superior em datasets climáticos de pequena escala.  
2. O fracasso do ANN neste estudo pode servir como argumento sobre limitações de modelos profundos em contextos de baixa amostragem.  
3. A importância de variáveis climáticas simples (temperatura, umidade, vento) reforça a adequação da sua base INMET.  
4. A metodologia de pré-processamento (EDA, encoding, correlação) é replicável ao seu pipeline proposto, fortalecendo a seção de metodologia.  
