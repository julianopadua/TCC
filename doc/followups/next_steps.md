# Follow-up e planejamento (dataset + fusão temporal)

## Atualização 2026-04-07 — branch `article-temporal-fusion`

**Plano executado (diagramas, artefatos, checklist de validação):** [Fusão temporal para o artigo](../planos/fusao_temporal_artigo_2026-04-07.md)

**Próximos passos imediatos (validação):**

1. Smoke test de `python src/feature_engineering_temporal.py --years <ano> --methods ewma_lags` e conferir pastas `*_tsfusion`.
2. Comparar métricas Camada A em `data/eda/temporal_fusion/layer_a_summary.csv`.
3. Rodar `train_runner` em par (`base_F_calculated` vs `base_F_calculated_tsfusion`) com a mesma variação de menu.
4. Registrar no TCC/artigo: ablação de métodos e limitações de dependências opcionais.

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
