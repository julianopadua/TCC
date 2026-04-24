# Auditoria Técnica do Pipeline ML — 2026-04-24

**Branch:** `audit/pipeline-refactor` (commit `12e0ae7`)
**Escopo:** `/src` completo — artigo + legado TCC

---

## 1. Fluxo real de execução

### Caminho do artigo (canônico)

```
src/article/run_pipeline.py
  etapa 0: enrich_coords.py       → coordenadas BDQ + estação
  etapa 1: gee_biomass.py         → NDVI_buffer, EVI_buffer via MOD13Q1 (MODIS, 16d, buffer 50km)
  etapa 2: eda.py                 → séries temporais + correlação
  → data/_article/0_datasets_with_coords/{cenario}/inmet_bdq_{ano}_cerrado.parquet

src/article/article_orchestrator.py
  etapa 1: temporal_fusion_article.py
    método ewma_lags   → tsf_ewma_{var}_{alpha} + tsf_lag_{var}_{h}h
    método sarimax_exog → tsf_sarimax_exog_{endog}_{pred,resid}
    método minirocket  → tsf_minirocket_f{000..167}
    → data/_article/1_datasets_with_fusion/{cenario}/{método}/
  etapa 2: feature_selection_article.py
    Camada A: Spearman + MI sobre anos de treino → selected_features_article.json
  etapa 3: _build_champion
    originais + TOP-K tsf_* → 1_datasets_with_fusion/{cenario}/champion/

src/train_runner.py --article
  cenários coords:   python src/train_runner.py run --article -s base_E_with_rad_knn_calculated -m xgboost -v 1
  cenários champion: python src/train_runner.py run --article -s tf_E_champion -m xgboost -v 1
  resultados: data/_article/_results/{model_type}/{variacao}/{cenario}/
```

### Caminho TCC legado (não-artigo)

```
src/feature_engineering_physics.py
  → calcula precip_ewma, dias_sem_chuva, risco_*, fator_propagacao
  → distribui para todas as bases (A..F) via merge
  → data/modeling/{base_*_calculated}/

src/feature_engineering_temporal.py
  métodos: ewma_lags + sarimax_exog (sem MiniRocket)
  → data/temporal_fusion/{base_*_calculated}/{método}/

src/train_runner.py (sem --article)
  → qualquer chave de modeling_scenarios no config.yaml
  → data/modeling/results/{model_type}/{variacao}/{cenario}/
```

---

## 2. Código morto / legado

Os scripts abaixo são da fase de coleta/pré-processamento de dados (anterior ao ML). Não fazem parte do pipeline ML do artigo. Podem ser arquivados mas não deletados.

| Arquivo | Descrição |
|---------|-----------|
| `src/bdqueimadas_scraper.py` | Scraper BDQueimadas |
| `src/inmet_scraper.py` | Scraper INMET |
| `src/bdqueimadas_consolidated.py` | Consolidação BDQ |
| `src/inmet_consolidated.py` | Consolidação INMET |
| `src/bdq_build_biome_dict.py` | Dicionário de biomas |
| `src/build_dataset.py` | Montagem do dataset base |
| `src/dataset_missing_audit.py` | Auditoria de faltantes |
| `src/audit_city_coverage.py` | Auditoria de cobertura |
| `src/audit_consolidated_sources.py` | Auditoria de fontes |
| `src/audit_databases.py` | Auditoria de bases |
| `src/explore_risco_fogo.py` | EDA exploratório |
| `src/merge_risco_validation.py` | Validação de risco |

`src/feature_engineering_temporal.py` ainda é usado pelo caminho TCC legado, mas foi supersedido por `src/article/temporal_fusion_article.py` para o artigo.

---

## 3. Validação do pipeline vs especificação

| Etapa | Especificação | Status |
|-------|---------------|--------|
| Carga do dataset base | `base_*_calculated` parquets | ✅ |
| Feature engineering EWMA | `precip_ewma`, `dias_sem_chuva` em `feature_engineering_physics.py` | ✅ |
| Adicionar NDVI + EVI (buffer) | `gee_biomass.py` → `NDVI_buffer`, `EVI_buffer` | ✅ |
| Salvar dataset intermediário | `0_datasets_with_coords/` | ✅ |
| Treinar RF/XGBoost com **SOMENTE** NDVI+EVI | **DESVIO** — sempre incluía features meteo/física junto | ⚠️ Corrigido |
| Fusão temporal — variações EWMA | `ewma_lags`: 3 alphas (0.1, 0.3, 0.8) + lags 1h/24h/168h por variável | ✅ |
| Fusão temporal — MiniRocket | 5 canais, janela L=168h, n_kernels=168 | ✅ |
| Salvar resultados em `/results` | `data/_article/_results/` | ✅ |

**Correção aplicada:** novo modo `modeling_biomass_mode: biomass_only` em `config.yaml`.
Para rodar a etapa 5 com APENAS NDVI+EVI, alterar a chave antes do treino:
```yaml
# config.yaml
article_pipeline:
  modeling_biomass_mode: biomass_only
```

---

## 4. Vazamento de dados (Data Leakage)

**Resultado: nenhum vazamento crítico encontrado.**

| Componente | Risco avaliado | Veredito |
|------------|---------------|---------|
| EWMA (`tsf_ewma_*`) | Look-ahead dentro do ano | ✅ SEGURO — `ewm(adjust=False)` em dados ordenados é causal; reinicia por ano |
| Lags (`tsf_lag_*`) | Look-ahead | ✅ SEGURO — `shift(lh)` correto |
| MiniRocket — fit | Contaminação treino/teste | ✅ SEGURO — `_minirocket_fit_global` chamado só com `years < cut_year` |
| MiniRocket — janelas | Look-ahead cross-year | ✅ SEGURO — janela `vals[i-L:i]` é causal; i < L produz NaN |
| Seleção de features (Camada A) | Contaminação por teste | ✅ SEGURO — Spearman + MI calculados só nos anos de treino |
| SARIMAX rolling forecast | Look-ahead no horizonte | ✅ SEGURO — cada bloco forecasta a partir de janela estritamente passada |
| `precip_ewma` base | Look-ahead | ✅ SEGURO — `ewm(alpha=0.5, adjust=False)` em dados ordenados com memória cross-year |
| Split treino/teste | Contaminação por ano | ✅ SEGURO — holdout temporal por ano, últimos `test_size_years=2` como teste |

### Problema de integridade nos dados do champion (não é leakage, mas afeta qualidade)

Os parquets champion de 2003–2006 têm contagem de linhas ~16× maior que os coords:

```
champion/2003: 6.336.896 linhas  vs  coords/2003: 396.056 linhas  (delta: 5.940.840)
champion/2004: 6.343.808 linhas  vs  coords/2004: 396.488 linhas  (delta: 5.947.320)
```

**Causa:** chaves duplicadas em `(cidade_norm, ts_hour)` nos parquets de fusão, antes do patch de dedup no `_build_champion`. O código já foi corrigido (dedup presente), mas os artefatos gerados antes precisam ser regenerados.

**Efeito:** o modelo vê os padrões de 2003–2006 com frequência ~16× maior que anos recentes — viés de treino, sem vazamento de informação futura.

**Correção dos artefatos:**
```bash
python src/article/article_orchestrator.py --overwrite
```

**Guarda adicionada no código** (`article_orchestrator.py`): aborta e loga erro se o champion gerado tiver >5% mais linhas que a base.

---

## 5. Viabilidade Real-Time / InMatch

| Componente | Viável? | Custo por predição | Obs. |
|------------|---------|-------------------|------|
| **EWMA + Lags** | **SIM** | O(1) | Mantém estado EWMA + buffer circular `max_lag=168` obs por cidade/variável |
| **MiniRocket** | **CONDICIONAL** | ~140K MAC × n_canais | Modelo pré-fitado offline (joblib); buffer 168h por estação; sem re-fit na inferência |
| **SARIMAX** | **NÃO** | O(minutos/cidade) | Fitting de ARIMA é batch; impraticável < 1s |
| Seleção de features | Offline | — | Executa uma vez no treino; lista fixa na inferência |
| GridSearch / SMOTE | Offline | — | Somente treino |

**Stack recomendado para InMatch:**
```
EWMA + Lags  +  MiniRocket (pré-fitado)  +  XGBoost/RF
```

**Requisitos para deploy do MiniRocket:**
1. Serializar `engine._minirocket_model` via `joblib.dump` após fit offline
2. Manter `deque(maxlen=168)` por estação por canal (5 canais = 840 floats por estação)
3. Primeiras 168h após reinício de dados → output NaN ou fallback para predição só-EWMA

---

## 6. Bugs corrigidos (branch `audit/pipeline-refactor`)

### Bug 1 — `_coerce_binary_target` não propagava filtro

**Arquivo:** `src/train_runner.py:109–116`

```python
# ANTES — rebind local; linhas com target não-binário passavam pelo filtro
df = df.loc[df[target].isin([0, 1])]

# DEPOIS — drop in-place no índice correto
non_binary = ~df[target].isin([0, 1])
if non_binary.any():
    df.drop(index=df.index[non_binary], inplace=True)
```

### Bug 2 — Lambda sem captura de variável em `feature_engineering_temporal`

**Arquivo:** `src/feature_engineering_temporal.py:462–469`

```python
# ANTES — frágil (depende de transform() ser síncrono)
lambda x: x.ewm(alpha=alpha, adjust=False).mean()
lambda x: x.shift(lag_h)

# DEPOIS — captura explícita por default-arg (alinha com o módulo do artigo)
lambda x, a=alpha: x.ewm(alpha=a, adjust=False).mean()
lambda x, lh=lag_h: x.shift(lh)
```

### Bug 3 — Explosão de linhas no champion sem detecção

**Arquivo:** `src/article/article_orchestrator.py`

Adicionada guarda antes do `to_parquet`: se `len(merged) > len(base) * 1.05`, aborta com `log.error` e pula o ano, evitando que parquets corrompidos cheguem ao treino silenciosamente.

### Feature — Modo `biomass_only` (etapa 5 do artigo)

**Arquivos:** `config.yaml`, `src/train_runner.py`, `src/article/config.py`

`modeling_biomass_mode: biomass_only` → feature set restrito a `NDVI_buffer + EVI_buffer` somente.

---

## 7. Consistência entre módulos

| Constante / Coluna | `tsf_constants.py` | `feature_engineering_temporal.py` | `temporal_fusion_article.py` | `train_runner._base_features` |
|--------------------|--------------------|------------------------------------|------------------------------|-------------------------------|
| `COL_PRECIP` | ✅ | ✅ importa | ✅ importa | ✅ hardcoded igual |
| `COL_TEMP` | ✅ | ✅ importa | ✅ importa | ✅ |
| `COL_UMID` | ✅ | ✅ importa | ✅ importa | ✅ |
| `COL_RAD` | ✅ | ✅ importa | ✅ importa | ✅ |
| `NDVI_buffer` | — | — | definido localmente | via `_extend_with_article_biomass_columns` |
| `EVI_buffer` | — | — | definido localmente | via `_extend_with_article_biomass_columns` |

**Inconsistência de endog SARIMAX:** o artigo usa `endog: "umid"` (config.yaml) enquanto o legado TCC usa `arimax_endog="precip"` como default. Intencional — pipelines diferentes.

---

## 8. Dados do experimento (estado atual dos parquets)

Baseado em `data/_article/1_datasets_with_fusion/base_E_with_rad_knn_calculated/audit.md`:

| Método | Anos | Colunas tsf_* | Linhas (ex. 2019) |
|--------|------|---------------|-------------------|
| ewma_lags | 22 | 34 | 6.791.696 |
| minirocket | 22 | 168 | 6.791.696 |
| sarimax_exog | 1 (2020 apenas) | 2 | 6.885.872 |
| champion | 22 | 50 | 6.791.696 |

Champion 2003–2006: **REQUER REGENERAÇÃO** (ver seção 4).
