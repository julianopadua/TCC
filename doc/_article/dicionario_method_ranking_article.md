# Dicionário de dados — `method_ranking_article.csv`

**Artefato:** ranking global de features de fusão temporal (`tsf_*`) para o pipeline do artigo.  
**Caminho:** `data/eda/temporal_fusion/method_ranking_article.csv`  
**Última referência de geração:** Camada A (`src/article/feature_selection_article.py`), pós-fusão em `data/_article/1_datasets_with_fusion/{cenário}/{método}/`.

---

## 1. Papel no pipeline

Este ficheiro consolida **todas** as colunas `tsf_*` produzidas pelos métodos de fusão temporal configurados em `article_pipeline.temporal_fusion.methods` (p.ex. `ewma_lags`, `sarimax_exog`, `minirocket`). Para cada feature, reporta:

- associação monotónica com o alvo binário **`HAS_FOCO`** (Spearman);
- informação mútua com o mesmo alvo (Mutual Information);
- um **score composto** após normalização global, usado para ordenar o ranking.

O ficheiro serve para **situação de trabalho**: entender qual método e qual variável de série subjacente dominam, comparar magnitude de efeitos e ausência de dados, e cruzar com `selected_features_article.json` (top‑*k* efetivamente promovidas à modelagem).

**Relação com outros artefatos**

| Artefato | Conteúdo |
|----------|----------|
| `method_ranking_article.csv` | Ranking **completo** (uma linha por feature `tsf_*` sobrevivente ao filtro de NaN). |
| `selected_features_article.json` | Subconjunto **top‑*k*** (`article_pipeline.temporal_fusion.top_k`), com metadados (`test_years_cutoff`, pesos, cenário). |
| `layer_a_detail.csv` (se existir) | Detalhe adicional por execução; o CSV aqui descrito é a visão tabular agregada do ranking. |

---

## 2. População estatística e vazamento temporal

- **Apenas anos de treino:** os parquets concatenados são os com `ANO < cut_year`, onde `cut_year` é o primeiro dos últimos `test_size_years` anos disponíveis (config: `article_pipeline.temporal_fusion.test_size_years`, padrão 2). Anos de teste **não** entram no cálculo — evita que métricas de seleção de features antecipem o holdout usado no XGBoost.
- **Spearman:** calculado em **todas** as linhas de treino com pares `(feature, HAS_FOCO)` finitos; observações com NaN na feature são excluídas **por feature** (o número efetivo aparece em `n_obs`).
- **Mutual Information:** se o número de linhas de treino ≥ `mi_sample_cutoff`, usa-se uma amostra de até `mi_sample_size` linhas, com estratificação por `HAS_FOCO` quando `stratify_by` está definido (config `feature_selection`). Valores em falta são imputados pela **mediana** da coluna antes do MI (apenas neste passo).
- **Normalização do score:** `spearman_abs_norm` e `mi_norm` são obtidos dividindo, **em todo o conjunto de linhas do ranking**, o valor absoluto de Spearman e o `mi_score` pelo **máximo** observado nessa coluna no ficheiro. Por isso, o “1,0” em `spearman_abs_norm` ou `mi_norm` significa “máximo entre todas as features ranqueadas nesta execução”, não um teto teórico universal.

---

## 3. Convenção de nomes das features (`feature_name`)

Prefixo comum: **`tsf_`** (temporal fusion).

| Padrão | Método de origem típico | Significado resumido |
|--------|-------------------------|----------------------|
| `tsf_ewma_{slug}_{aXX}` | `ewma_lags` | Média móvel exponencial da variável `slug` com parâmetro α (`a01`, `a03`, `a08`). |
| `tsf_lag_{slug}_{N}h` | `ewma_lags` | Valor da variável `slug` defasado de *N* horas. |
| `tsf_sarimax_exog_{slug}_pred` | `sarimax_exog` | Previsão one-step-ahead (blocos) do modelo SARIMAX com endógena configurada. |
| `tsf_sarimax_exog_{slug}_resid` | `sarimax_exog` | Resíduo (endógena − predição). |
| `tsf_minirocket_f{iii}` | `minirocket` | Dimensão *iii* do embedding MiniROCKET (multicanal). |

Os campos `method` e `target_var` derivam do nome da feature por regras em código (`_method_from_feature_name`, `_target_var_from_feature_name`). Embeddings MiniROCKET recebem `target_var = multicanal`.

---

## 4. Dicionário de colunas (todas)

| Coluna | Tipo | Descrição |
|--------|------|-----------|
| `feature_name` | string | Identificador único da coluna no parquet de fusão (prefixo `tsf_`). |
| `method` | string | Método de fusão temporal de origem: `ewma_lags`, `sarimax_exog`, `minirocket`, etc. |
| `target_var` | string | Variável de série subjacente inferida pelo nome (`precip`, `temp`, `umid`, `rad`, `ndvi_buffer`, `evi_buffer`, …) ou `multicanal` para MiniROCKET. |
| `spearman_r` | float | Coeficiente de correlação de Spearman entre a feature e `HAS_FOCO` (−1 a 1). Valores `NaN` se houver menos de 30 observações válidas. |
| `spearman_p` | float | *p*-valor bilateral do teste de Spearman. Com *N* muito grande, pode underflow para 0,0 em CSV. |
| `spearman_abs_norm` | float ∈ [0, 1] | \(\lvert \texttt{spearman\_r} \rvert\) dividido pelo máximo de \(\lvert r \rvert\) entre **todas** as linhas deste ranking. |
| `mi_score` | float ≥ 0 | Estimativa de informação mútua (`mutual_info_classif`, `n_neighbors=3`, features contínuas). |
| `mi_norm` | float ∈ [0, 1] | `mi_score` dividido pelo máximo de MI entre **todas** as linhas deste ranking. |
| `score_composite` | float ∈ [0, 1] | Combinação linear normalizada: `spearman_weight * spearman_abs_norm + mi_weight * mi_norm` (pesos em `config.yaml` → `feature_selection`; padrão 0,5 / 0,5). |
| `rank` | int | Posição 1 = maior `score_composite` após ordenação decrescente global. |
| `n_obs` | int | Número de observações válidas (feature e `HAS_FOCO` finitos) usadas no Spearman para essa feature. |
| `pct_nan` | float ∈ [0, 1] | Fração de valores em falta na coluna **no conjunto de treino concatenado** do método correspondente, antes do filtro que descarta features com excesso de NaN. |

---

## 5. Exemplos de linhas (features) — leitura substantiva

A tabela abaixo ilustra **como interpretar** entradas típicas; não exaure o espaço de nomes possíveis.

| `feature_name` | Leitura académica |
|----------------|-------------------|
| `tsf_lag_rad_1h` | Radiação global defasada 1 h; captura condição de seca/insolação recente associada a risco. |
| `tsf_ewma_rad_a08` | EWMA da radiação com α alto (resposta rápida à série); realça picos recentes face a α mais baixos (`a01`, `a03`). |
| `tsf_lag_rad_168h` | Radiação há uma semana (168 h); memória de médio prazo da variável. |
| `tsf_ewma_temp_a08` | Temperatura suavizada com EWMA “rápida”; útil para calor acumulado recente. |
| `tsf_lag_umid_24h` | Umidade relativa defasada 1 dia; seca antecedente. |
| `tsf_ewma_umid_a03` | Umidade com suavização intermédia; equilíbrio entre ruído horário e tendência. |
| `tsf_ewma_ndvi_buffer_a08` | NDVI (buffer espacial GEE) com EWMA rápida; biomassa/estresse de vegetação em escala curta. |
| `tsf_lag_evi_buffer_336h` | EVI defasado 336 h (14 dias); alinhado à cadência aproximada MOD13Q1 + memória multi-semanal. |
| `tsf_sarimax_exog_umid_resid` | Resíduo do SARIMAX com endógena `umid`; desvio da umidade em relação ao esperado dado exógenas (meteo + biomassa na configuração do artigo). |
| `tsf_sarimax_exog_umid_pred` | Trajetória prevista da umidade no esquema de previsão por blocos; associação frequentemente mais fraca que o resíduo se a série já explica bem o alvo. |
| `tsf_ewma_precip_a01` | Precipitação com EWMA lenta (α baixo); ênfase em tendência de longo prazo. |
| `tsf_lag_precip_168h` | Chuva acumulada na lógica de lag semanal; proxy de solo úmido ou secagem. |

*(Se o pipeline incluir MiniROCKET, exemplos como `tsf_minirocket_f042` seriam coeficientes de um kernel convolucional multicanal sem rótulo físico direto — daí o papel de `target_var = multicanal`.)*

---

## 6. Leitura crítica dos valores

- **Sinais de Spearman:** correlações negativas com `HAS_FOCO` em variáveis de umidade ou biomassa são interpretáveis como “valores mais altos de umidade/vegetação associam-se a menor probabilidade de foco”, coerente com mecanismos ecológicos; radição/temperatura tendem a correlacionar positivamente com o alvo em contextos de estiagem.
- **MI ≈ 0 com Spearman moderado:** pode ocorrer se a relação for fortemente não monotónica ou se a MI tiver sido estimada num subconjunto estocástico diferente; o score composto ainda combina ambas as fontes.
- **`n_obs` inferior ao tamanho total do treino:** indica exclusão de linhas com NaN naquela feature (comum em lags longos ou janelas MiniROCKET).
- **Ranking mistura métodos:** a ordenação é **global**; não há um bloco separado por método. Para comparar métodos entre si, filtrar por `method` ou usar documentação complementar por método.

---

## 7. Regeneração e dependências

1. Gerar parquets em `data/_article/1_datasets_with_fusion/{cenário}/{método}/` (`temporal_fusion_article` / orquestrador).  
2. Executar a Camada A (`feature_selection_article.run_feature_selection`).  
3. Parâmetros relevantes: `config.yaml` → `article_pipeline.temporal_fusion` (cenário, `test_size_years`, `methods`, `top_k`, bloco `feature_selection`).

Qualquer alteração em `test_size_years`, métodos ou filtros de NaN **invalida** comparações diretas com rankings antigos; documente a data da execução nos commits ou logs em `logs/`.
