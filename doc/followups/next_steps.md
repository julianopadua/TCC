# Follow-up e planejamento (dataset + fusão temporal)

## Atualização 2026-04-09 — fusão temporal por método

**Plano executado:** [Fusão temporal para o artigo](../planos/fusao_temporal_artigo_2026-04-07.md)

### Checklist de execução (ordem recomendada)

1. **Smoke test — método leve, um ano, só base D:**
   ```
   python src/feature_engineering_temporal.py --years 2020 --methods ewma_lags --scenarios base_D_calculated
   ```
   Conferir pasta `data/temporal_fusion/base_D_with_rad_drop_rows_calculated/ewma_lags/`.

2. **Diagnóstico de NaN antes de rodar a sério:** os logs INFO já imprimem `[NAN]` com % de NaN
   por coluna (precip, temp, umid, rad, vento, pressão) para cada base/ano antes dos métodos.
   Para ver exceções individuais dos blocos ARIMA/ARIMAX, subir nível de log para `DEBUG` em
   `config.yaml` (`logging.level: "DEBUG"`).

3. **Rodagem completa — por método para controlar tempo:**
   ```
   python src/feature_engineering_temporal.py --output-layout split --methods arima
   python src/feature_engineering_temporal.py --output-layout split --methods sarima
   python src/feature_engineering_temporal.py --output-layout split --methods arimax
   python src/feature_engineering_temporal.py --output-layout split --methods sarimax_exog
   python src/feature_engineering_temporal.py --output-layout split --methods prophet
   python src/feature_engineering_temporal.py --output-layout split --methods minirocket
   python src/feature_engineering_temporal.py --output-layout split --methods tskmeans
   ```
   Por padrão processa bases D, E e F. Usar `--scenarios base_D_calculated base_F_calculated`
   para restringir a D e F somente.

4. **Testar ordens ARIMA diferentes (uma rodada por ordem):**
   ```
   python src/feature_engineering_temporal.py --methods arima --arima-order 1 0 0 --scenarios base_D_calculated --years 2018 2019
   python src/feature_engineering_temporal.py --methods arima --arima-order 2 1 2 --scenarios base_D_calculated --years 2018 2019
   ```

5. **ARIMA/SARIMA só em variáveis de risco específicas:**
   ```
   python src/feature_engineering_temporal.py --methods arima sarima --arima-vars precip temp umid
   ```
   ARIMAX com endog diferente de precip:
   ```
   python src/feature_engineering_temporal.py --methods arimax sarimax_exog --arimax-endog temp
   ```

6. **Conferir Camada A e run metrics:**
   ```
   data/eda/temporal_fusion/method_ranking_train.csv   # (method, target, mae_mean, r2_mean)
   data/eda/temporal_fusion/tsf_run_metrics.csv        # (scenario, year, method, ok, fail, skipped, fail_types)
   data/eda/temporal_fusion/layer_a_detail.csv
   ```

7. **Construir bases campeãs (top-k métodos por MAE, bases D/E/F):**
   ```
   python src/build_champion_temporal_bases.py --top-k 2
   ```
   Ou com métodos explícitos após análise do ranking:
   ```
   python src/build_champion_temporal_bases.py --methods arima arimax prophet --bases D E F
   ```

8. **Treinar XGBoost/RF em cada cenário:**
   - Rodar `train_runner.py` selecionando os cenários `tf_D_*` / `tf_E_*` / `tf_F_*`
     (aparecem no menu automaticamente via `temporal_fusion_paths` no config.yaml).
   - Para comparação controlada: mesma variação de hiperparâmetros entre cenários.
   - Cenários a comparar: `base_D_calculated` (sem tsf) vs `tf_D_ewma_lags` vs
     `tf_D_arima` … vs `tf_D_arimax` … vs `tf_D_champion`.

9. **Registrar no TCC/artigo:**
   - Ganho por método (PR-AUC Camada B) vs proxy de ajuste (MAE Camada A por variável).
   - ARIMAX/SARIMAX_exog: documentar que `exog_future = última linha repetida` é convenção
     de feature engineering operacional, não previsão meteorológica literal.
   - Ablação de métodos e limitações de dependências opcionais.
   - Avisar que seleção do campeão usa anos de treino para evitar viés.

**Decisões de engenharia contínuas:** [followup_decisions.md](./followup_decisions.md)

---

## Histórico — planejamento de construção do dataset consolidado

*Texto abaixo descreve etapas iniciais planejadas; parte delas evoluiu para os scripts atuais (`bdqueimadas_consolidated.py`, `inmet_consolidated.py`, `build_dataset.py`, etc.).*

Este documento descreve as etapas necessárias para a criação de um dataset final unindo dados do BDQueimadas e do INMET, a ser utilizado em modelos de previsão de focos de queimadas.

## Coleta e Organização Inicial
- ~~Criar script `consolidated_bdqueimadas.py`~~ **Implementado como** `src/bdqueimadas_consolidated.py` (merge manual × processado; saídas em `data/consolidated/BDQUEIMADAS/` conforme `config.yaml`).
- Scraping de zips COIDS: `src/bdqueimadas_scraper.py`.

## Processamento INMET
- Utilizar e ajustar o script existente `src/inmet_consolidated.py` (evolução do nome `consolidated_inmet.py`) para:
  - Padronizar tipos de dados (`numeric`, `datetime`, `categorical`).
  - Remover valores inválidos ou sentinelas (`-9999`).
  - Salvar dados limpos em `data/processed/INMET/`.

## Processamento BDQueimadas
- Padronizar e limpar os dados do BDQueimadas:
  - Normalizar colunas de localização (`Estado`, `Municipio`, `Bioma`).
  - Converter campo `DataHora` para UTC e arredondar para hora cheia.
  - Substituir ou marcar valores inválidos (exemplo: `DiaSemChuva = -999`).
  - Salvar em `data/processed/BDQUEIMADAS/`.

## Consolidação das Bases
- Definir como base principal a tabela do INMET com granularidade hora a hora por município.
- Realizar junção com BDQueimadas pelos campos:
  - `Municipio` normalizado
  - `Estado`
  - `DataHora` arredondada para hora
- Criar novas colunas:
  - `ehFoco?`: flag binária (0 para ausência de foco, 1 para ocorrência de foco).
  - `FRP`: intensidade de radiação associada ao foco.
  - `foco_count`: número de focos por município-hora.
  - `risco_fogo_mean`: valor médio do risco de fogo no período e município (opcional para análise).
  - `foco_id` e `id_bdq`: mantidos apenas para rastreabilidade e removidos na etapa de treino.

## Dataset Final
- Exportar em formato Parquet (`.parquet`) com compressão para `data/processed/datasets/final/`.
- Produzir duas versões:
  1. Dataset completo, incluindo identificadores (`foco_id`, `id_bdq`).
  2. Dataset para treino, sem identificadores técnicos.

## Estrutura Final do Dataset

**Chaves**
- datahora_utc
- municipio
- estado

**Variáveis Meteorológicas (INMET)**
- Precipitação
- Pressão atmosférica
- Radiação global
- Temperatura do ar
- Temperatura do ponto de orvalho
- Umidade relativa do ar
- Vento (direção, rajada, velocidade)

**Variáveis de Queimadas (BDQueimadas)**
- ehFoco?
- FRP
- foco_count
- risco_fogo_mean

**Auxiliares**
- foco_id
- id_bdq

## Expectativas
- Dataset com granularidade de uma linha por município-hora.
- Classes desbalanceadas, com poucos casos positivos de foco.
- Estrutura compatível para treinar modelos de classificação binária (ehFoco?) e regressão (FRP).
