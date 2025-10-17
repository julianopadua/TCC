# Planejamento de Construção do Dataset Consolidado

Este documento descreve as etapas necessárias para a criação de um dataset final unindo dados do BDQueimadas e do INMET, a ser utilizado em modelos de previsão de focos de queimadas.

## Coleta e Organização Inicial
- Criar script `consolidated_bdqueimadas.py` para:
  - Automatizar scraping do BDQueimadas.
  - Garantir que cada foco possua identificadores únicos (`id_bdq` e `foco_id`).
  - Salvar dados coletados em `data/consolidated/BDQUEIMADAS/`.

## Processamento INMET
- Utilizar e ajustar o script existente `consolidated_inmet.py` para:
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
