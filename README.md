# TCC - Previsão de Queimadas (BDQueimadas + INMET)

Projeto de ETL e análise para construir séries anuais de focos de calor (BDQueimadas/INPE) e variáveis climáticas (INMET), visando modelagem e previsão de queimadas, com foco inicial no bioma Cerrado.

## Sumário

* [1. Visão geral](#1-visão-geral)
* [2. Estrutura de pastas](#2-estrutura-de-pastas)
* [3. Fluxo geral do pipeline](#3-fluxo-geral-do-pipeline)
* [4. Módulos principais](#4-módulos-principais)

  * [4.1 BDQueimadas - download automático (bdqueimadas_scraper.py)](#41-bdqueimadas---download-automático-bdqueimadasscraperpy)
  * [4.2 BDQueimadas - consolidação MANUAL × PROCESSADO (consolidated_bdqueimadas.py)](#42-bdqueimadas---consolidação-manual--processado-consolidated_bdqueimadaspy)
  * [4.3 INMET - scraping e consolidação](#43-inmet---scraping-e-consolidação)
  * [4.4 Build do dataset conjunto BDQ + INMET (build_dataset.py)](#44-build-do-dataset-conjunto-bdq--inmet-build_datasetpy)
  * [4.5 Auditoria de dados faltantes (dataset_missing_audit.py)](#45-auditoria-de-dados-faltantes-dataset_missing_auditpy)
  * [4.6 Bases para modelagem (modeling_build_datasets.py)](#46-bases-para-modelagem-modeling_build_datasetspy)
* [5. Pré-requisitos](#5-pré-requisitos)
* [6. Ambiente Python (venv)](#6-ambiente-python-venv)

  * [6.1 Windows](#61-windows)
  * [6.2 Linux/macOS](#62-linuxmacos)
* [7. Configuração do projeto](#7-configuração-do-projeto)
* [8. Execução rápida](#8-execução-rápida)

  * [8.1 Pipeline BDQueimadas](#81-pipeline-bdqueimadas)
  * [8.2 Pipeline INMET](#82-pipeline-inmet)
  * [8.3 Join BDQ + INMET, auditoria e modelagem](#83-join-bdq--inmet-auditoria-e-modelagem)
* [9. Convenções, nomes e logs](#9-convenções-nomes-e-logs)
* [10. Pontos em aberto e próximos passos](#10-pontos-em-aberto-e-próximos-passos)
* [11. Solução de problemas](#11-solução-de-problemas)

---

## 1. Visão geral

Objetivo central do repositório:

* Extrair dados anuais de focos de calor do BDQueimadas (fonte COIDS/INPE, coleção Brasil_sat_ref) e dados climáticos do INMET.
* Padronizar, limpar e consolidar essas fontes em bases anuais e multi-anos.
* Focar no bioma Cerrado para construção de um dataset integrado BDQueimadas + INMET voltado a modelagem preditiva de risco de queimadas.

Decisão de desenho importante:

* O BDQueimadas é consumido em duas frentes complementares:

  * Exportações manuais do portal (arquivos `exportador_*_ref_YYYY.csv`), que contêm variáveis como `RiscoFogo` e `FRP`, mas não possuem `foco_id`.
  * Arquivos anuais oficiais `focos_br_ref_YYYY.zip` do COIDS (Brasil_sat_ref), que, após extração, trazem `foco_id` e identificadores (`id_bdq` etc.), mas não trazem `RiscoFogo` e `FRP`.
* A consolidação (`consolidated_bdqueimadas.py`) foi pensada justamente para unir esses dois mundos:

  * Manter a física do foco (`FRP`, `RiscoFogo`).
  * Associar cada registro a identificadores consistentes (`FOCO_ID`, `ID_BDQ`) usados na própria infraestrutura governamental.

Todo o fluxo é orquestrado de forma modular, com utilitários de caminho, logging e leitura de configuração centralizados em `src/utils.py` e `config.yaml`. As decisões de limpeza e transformação mais relevantes são documentadas em `doc/followup_decisions.md`.

---

## 2. Estrutura de pastas

Estrutura atual (resumida e comentada):

```text
.
├─ addons/
├─ config.yaml
├─ requirements.txt
├─ README.md
├─ data/
│  ├─ raw/
│  │  ├─ BDQUEIMADAS/          # exportações manuais: exportador_*_ref_YYYY.csv
│  │  └─ ID_BDQUEIMADAS/       # zips focos_br_ref_YYYY.zip baixados via scraper
│  │
│  ├─ processed/
│  │  ├─ ID_BDQUEIMADAS/       # extração dos zips em subpastas por ano
│  │  └─ INMET/                # saídas intermediárias do pipeline INMET (detalhe a documentar)
│  │
│  ├─ external/
│  │  └─ BDQUEIMADAS/          # bdq_targets_*.csv (MANUAL × PROCESSADO consolidados)
│  │
│  └─ dataset/
│     └─ inmet_bdq_{ANO}_cerrado.csv   # bases já integradas, focadas no Cerrado (join BDQ + INMET)
│
├─ doc/
│  └─ followup_decisions.md    # diário das decisões de ETL, filtros e modelagem
│
├─ images/                     # figuras para o TCC e documentação
├─ logs/                       # arquivos de log rotacionados
├─ src/
│  ├─ bdqueimadas_scraper.py          # baixa e extrai focos_br_ref_YYYY.zip
│  ├─ consolidated_bdqueimadas.py     # faz o match MANUAL × PROCESSADO e gera bdq_targets_*.csv
│  ├─ inmet_scraper.py                # scraping INMET (detalhamento na próxima iteração do README)
│  ├─ inmet_consolidated.py           # consolidação INMET (nome ilustrativo, confirmar ao integrar)
│  ├─ build_dataset.py                # join BDQueimadas + INMET com filtro por Cerrado
│  ├─ dataset_missing_audit.py        # auditoria de valores faltantes e sentinelas
│  ├─ modeling_build_datasets.py      # construção de bases específicas para modelagem
│  ├─ TCC.py                          # scripts auxiliares ou notebooks exportados relacionados ao TCC
│  └─ utils.py                        # loadConfig, get_path, logging, helpers de download etc.
```

Observável a partir do código:

* `get_path("paths", "data", "raw")`, `get_path("paths", "data", "processed")` e `get_path("paths", "data", "external")` são usados em vários módulos. Estes caminhos devem existir no `config.yaml`.
* A pasta `external/BDQUEIMADAS` foi escolhida para separar as saídas consolidadas do BDQ das demais camadas de processamento.

---

## 3. Fluxo geral do pipeline

Visão macro, fim a fim:

1. BDQueimadas - coleta automática:

   * `bdqueimadas_scraper.py` descobre todos os `focos_br_ref_YYYY.zip` na página anual `Brasil_sat_ref` do COIDS.
   * Baixa estes zips para `data/raw/ID_BDQUEIMADAS`.
   * Extrai os zips em `data/processed/ID_BDQUEIMADAS/focos_br_ref_YYYY/`.

2. BDQueimadas - exportações manuais:

   * O usuário faz o download manual dos CSVs `exportador_*_ref_YYYY.csv` pelo portal BDQueimadas.
   * Esses arquivos são salvos em `data/raw/BDQUEIMADAS/`.

3. BDQueimadas - consolidação MANUAL × PROCESSADO:

   * `consolidated_bdqueimadas.py` lê para cada ano:

     * MANUAL: `exportador_*_ref_YYYY.csv` de `data/raw/BDQUEIMADAS`.
     * PROCESSADO: `focos_br_ref_YYYY.csv` de `data/processed/ID_BDQUEIMADAS/focos_br_ref_YYYY/`.
   * Gera uma junção um para um por hora arredondada e local (país, estado, município) para agregar:

     * De MANUAL: `RiscoFogo` e `FRP`.
     * De PROCESSADO: `FOCO_ID`, `ID_BDQ`, coordenadas.
   * Exporta arquivos anuais ou multi-anos em `data/external/BDQUEIMADAS/bdq_targets_*.csv`, com opção de filtro por bioma (por exemplo, Cerrado).

4. INMET - ingestão e consolidação:

   * `inmet_scraper.py` busca as séries do INMET.
   * `inmet_consolidated.py` organiza e padroniza os CSVs em `data/processed/INMET/`.
   * Detalhes de granularidade, filtros e formato final serão documentados de forma mais fina quando os scripts estiverem estabilizados.

5. Build do dataset integrado BDQ + INMET:

   * `build_dataset.py` junta as saídas consolidadas de BDQueimadas e INMET.
   * Usa uma interface de linha de comando para filtrar especificamente o bioma Cerrado.
   * Gera arquivos no padrão `data/dataset/inmet_bdq_{ANO}_cerrado.csv`, que são a base principal para análise exploratória e modelagem.

6. Auditoria de dados:

   * `dataset_missing_audit.py` carrega os CSVs consolidados (por ano ou multi-anos) e:

     * Analisa proporções de valores ausentes e sentinelas.
     * Gera tabelas e estatísticas que alimentam decisões sobre descartar variáveis, restringir períodos ou construir tratamentos específicos.

7. Modelagem:

   * `modeling_build_datasets.py` consome as bases de `data/dataset/` e monta variações de datasets prontos para modelos (engenharia de atributos, filtros, cortes temporais etc.).
   * Esta etapa está em uso e evoluindo; as decisões ainda estão sendo consolidadas e serão detalhadas explicitamente no README em uma próxima rodada.

---

## 4. Módulos principais

### 4.1 BDQueimadas - download automático (bdqueimadas_scraper.py)

Resumo das responsabilidades:

* Descobrir todos os links de `.zip` na página de referência:

  * URL base utilizada:
    `https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/`
* Filtrar apenas arquivos no padrão `focos_br_ref_YYYY.zip`.
* Baixar os arquivos para `data/raw/<folder_name>` (por padrão `ID_BDQUEIMADAS`).
* Opcionalmente extrair todos os `.zip` para `data/processed/<folder_name>/<zip_stem>/`.

Principais decisões técnicas implementadas:

* Sessão HTTP reutilizável (`get_requests_session`) para evitar overhead de conexão.
* Descoberta de links mais defensiva:

  * Utiliza `list_zip_links_from_page`.
  * Normaliza URLs com `urljoin`.
  * Filtra explicitamente por prefixo `focos_br_ref_` e extensão `.zip`.
* Filtragem por ano:

  * A função `filter_links_by_year` recebe uma lista de anos e mantém apenas os links cujo nome termina com o ano correspondente.
* Organização de diretórios:

  * `get_target_raw_dir(folder_name)` envia os zips para `data/raw/<folder_name>`.
  * `get_target_processed_dir(folder_name)` guarda os CSVs extraídos em `data/processed/<folder_name>`, com subpastas por zip para evitar colisão de nomes.
* Logs:

  * Usa `get_logger("bdqueimadas.scraper", kind="scraper", per_run_file=True)` para rastrear cada execução com arquivo de log próprio.

Interface de linha de comando:

* Argumentos principais:

  * `--folder`: nome da pasta sob `data/raw/` e `data/processed/` (default `ID_BDQUEIMADAS`).
  * `--years`: lista de anos a baixar (por exemplo `--years 2019 2020 2021`).
  * `--overwrite`: força re-download mesmo se o arquivo já existir.
  * `--no-extract`: se fornecido, apenas baixa os zips sem extrair.

Racional de design:

* A pasta padrão `ID_BDQUEIMADAS` é usada para deixar explícito que esses arquivos representam uma camada de identificação (contendo `FOCO_ID`, `ID_BDQ` etc.) que será posteriormente casada com a camada manual que traz `FRP` e `RiscoFogo`.
* A extração para `data/processed/ID_BDQUEIMADAS/focos_br_ref_{ANO}/` isola por ano e permite que a consolidação saiba exatamente de onde ler `focos_br_ref_{ANO}.csv`.

---

### 4.2 BDQueimadas - consolidação MANUAL × PROCESSADO (consolidated_bdqueimadas.py)

Objetivo do módulo:

* Unir, linha a linha, as exportações manuais e a camada processada do BDQueimadas para cada ano, de modo a produzir uma base que contenha simultaneamente:

  * Informações de risco e física do foco (RiscoFogo, FRP) provenientes dos arquivos manuais.
  * Identificadores e coordenadas alinhados com a infraestrutura oficial do BDQueimadas (FOCO_ID, ID_BDQ, lat, lon).

Arquitetura de caminhos:

* RAW manual:

  * `RAW_BDQ_DIR = data/raw/BDQUEIMADAS`
  * Arquivos no padrão `exportador_*_ref_YYYY.csv`.
* Processado via scraper:

  * `PROC_BDQ_DIR = data/processed/ID_BDQUEIMADAS`
  * Arquivo esperado: `focos_br_ref_{year}/focos_br_ref_{year}.csv`.
* Saídas:

  * `OUT_DIR = data/external/BDQUEIMADAS`
  * Nome padrão:

    * Um ano: `bdq_targets_{YYYY}[_<bioma>].csv`
    * Intervalo: `bdq_targets_{Y1}_{Y2}[_<bioma>].csv`
    * Todos os anos disponíveis: `bdq_targets_all_years[_<bioma>].csv`

Decisão de chave de junção:

* Junção é feita por uma chave sintética `__KEY` que combina:

  * Data e hora arredondada à hora cheia.
  * País, estado e município normalizados.
* Para o MANUAL:

  * Datetime em `DataHora`, parseado como `%Y/%m/%d %H:%M:%S`.
  * Colunas de localização originais: `Pais`, `Estado`, `Municipio`, `Bioma`.
* Para o PROCESSADO:

  * Datetime em `data_pas`, parseado como `%Y-%m-%d %H:%M:%S`.
  * Colunas de localização: `pais`, `estado`, `municipio`.

A construção de `__KEY` segue:

* Conversão de `__DT_H` (hora cheia) para inteiro e depois string.
* Concatenação com as chaves normalizadas:

  * `__PAIS_KEY`, `__UF_KEY`, `__MUN_KEY`.
* Mesma regra em MANUAL e PROCESSADO garante consistência na junção.

Tratamento de texto e acentuação:

* `_strip_controls` remove caracteres de controle e espaços especiais.
* `_ascii_upper_no_diacritics`:

  * Remove diacríticos sem descartar letras.
  * Exemplo: "Uberlândia" vira "UBERLANDIA".
* Colunas de saída usam este formato:

  * `PAIS_OUT`, `ESTADO_OUT`, `MUNICIPIO_OUT`.

Pipeline interno para cada ano:

1. Descoberta de arquivos:

   * `list_manual_year_files` lista todos os `exportador_*_ref_*.csv` e infere o ano.
   * `processed_file_for_year` verifica se `focos_br_ref_{year}.csv` existe.

2. Carregamento do MANUAL:

   * `_read_csv_smart` tenta múltiplas codificações (`utf-8-sig`, `utf-8`, `latin1`) para ler de forma robusta.
   * `load_manual`:

     * Cria `__DT` e `__DT_H` a partir de `DataHora`.
     * Gera chaves normalizadas via `normalize_key`.
     * Mapeia colunas de saída sem diacríticos.
     * Converte `Latitude`, `Longitude` e `FRP` para numérico se presentes.
     * Aplica filtro opcional por bioma (quando informado).
     * Gera `__KEY`.
     * Mantém apenas colunas necessárias para merge:

       * `__KEY`, `__DT_H`, `PAIS_OUT`, `ESTADO_OUT`, `MUNICIPIO_OUT`, `RiscoFogo`, `FRP`.

3. Carregamento do PROCESSADO:

   * `_read_csv_smart` novamente para robustez.
   * `load_processed`:

     * Cria `__DT` e `__DT_H` a partir de `data_pas`.
     * Gera as mesmas chaves `__PAIS_KEY`, `__UF_KEY`, `__MUN_KEY` e `__KEY`.
     * Seleciona colunas:

       * `__KEY`, `foco_id`, `id_bdq`, `lat`, `lon`.
     * Deduplica por `__KEY` para garantir a relação muitos para um na junção.

4. Merge:

   * `merge_manual_processed` executa um `left join` do MANUAL com o PROCESSADO usando `__KEY`.
   * Loga estatísticas:

     * Esperado (linhas do MANUAL após filtro).
     * Total de linhas após merge.
     * Quantidade de linhas com `ID_BDQ` não nulo (match).
     * Quantidade de linhas sem `ID_BDQ` (sem par no processado).

5. Construção da saída:

   * `build_output` monta um DataFrame legível com:

     * `DATAHORA` (hora cheia).
     * `PAIS`, `ESTADO`, `MUNICIPIO`.
     * `RISCO_FOGO`, `FRP`.
     * `ID_BDQ`, `FOCO_ID`.
   * Ordena por data, estado e município.
   * `write_output` grava o CSV final no `OUT_DIR`.

6. Consolidação multi-anos:

   * `run` recebe uma lista de anos ou, se omitida, utiliza todos os anos com arquivos manuais.
   * Junta os arquivos anuais em um único `all_years` ou intervalo `Y1_YN`.

Validação e modo rápido:

* O módulo oferece um modo de validação (`--validation`) que:

  * Limita o MANUAL às 100 primeiras linhas.
  * Limita o PROCESSADO a 500 mil linhas após deduplicação.
* Útil para testar o pipeline sem carregar tudo.

Interface de linha de comando:

* Argumentos principais:

  * `--years`: anos específicos a consolidar.
  * `--biome`: filtro opcional (por exemplo `--biome "Cerrado"`).
  * `--overwrite`: sobrescreve saídas existentes.
  * `--validation`: ativa modo rápido para testes.
  * `--output-filename`: nome customizado para o arquivo multi-anos.
  * `--encoding`: encoding usado para leitura e escrita (default `utf-8`).

---

### 4.3 INMET - scraping e consolidação

Status atual nesta documentação:

* Existem os scripts:

  * `inmet_scraper.py`
  * `inmet_consolidated.py` (nome sujeito a ajuste dependendo do código real)
* O papel deles, na arquitetura do projeto, é:

  * Fazer o scraping dos dados do INMET.
  * Padronizar, limpar e escrever saídas organizadas em `data/processed/INMET/`.

O detalhamento fino (quais variáveis, granularidade temporal exata, filtros por estação e tratamento de sentinelas específicos do INMET) será integrado ao README assim que os scripts forem enviados e consolidados, para evitar suposições.

---

### 4.4 Build do dataset conjunto BDQ + INMET (build_dataset.py)

Função na arquitetura:

* Consumir:

  * As bases consolidadas de BDQueimadas em `data/external/BDQUEIMADAS/bdq_targets_*.csv`.
  * As bases já consolidadas do INMET em `data/processed/INMET/`.
* Produzir:

  * Arquivos de dataset integrados por ano e bioma, com foco inicial no Cerrado, seguindo o padrão:

    * `data/dataset/inmet_bdq_{ANO}_cerrado.csv`.

Ponto fundamental já conhecido:

* O script expõe uma CLI que permite escolher o bioma, e o uso atual é filtrar explicitamente para o Cerrado.
* Os detalhes de como o join é feito (chave exata, granularidade temporal, distância espacial etc.) serão incorporados ao README quando o código estiver aqui para análise.

---

### 4.5 Auditoria de dados faltantes (dataset_missing_audit.py)

Este módulo é a camada formal de auditoria de qualidade dos arquivos consolidados `inmet_bdq_{ANO}_cerrado.csv` em `data/dataset/`. Ele não mexe na base, só lê os CSVs e escreve estatísticas ano a ano em `data/eda/dataset/{ANO}`.

Objetivos principais:

- Medir, para cada ano e para cada coluna de feature, o quanto de dado está faltando.
- Levar em conta:
  - NaN / null
  - strings vazias (depois de aplicar `strip`)
  - códigos sentinela negativos `-999` e `-9999`
- Ignorar nas métricas por coluna:
  - colunas alvo (`RISCO_FOGO`, `FRP`, `FOCO_ID`)
  - a coluna de label `HAS_FOCO`
  - colunas explicitamente excluídas (`DATA (YYYY-MM-DD)`, `HORA (UTC)`, `CIDADE`, `cidade_norm`, `ts_hour`)

Desenho dos caminhos:

- Entrada:
  - `DATASET_DIR` aponta para `paths.data.dataset` no `config.yaml`
  - o módulo procura arquivos com padrão `inmet_bdq_*_cerrado.csv`
- Saída:
  - `DATASET_EDA_DIR = data/eda/dataset`
  - para cada ano encontrado, cria:
    - `data/eda/dataset/{ANO}/missing_by_column.csv`
    - `data/eda/dataset/{ANO}/README_missing.md`

Regras de missing:

- A função `build_missing_matrix` constrói uma matriz booleana indicando se cada célula é considerada faltante:
  - colunas numéricas:
    - `isna()` ou valor em `{ -999, -9999 }`
  - colunas de texto:
    - `NaN`
    - string vazia depois de `strip`
    - string igual a `"-999"` ou `"-9999"` (forma texto dos sentinelas)
  - colunas booleanas:
    - apenas `isna()`

Cálculos produzidos:

- `compute_feature_breakdown_for_year` gera, para cada coluna de feature (já filtrando `TARGET_COLS`, `HAS_FOCO` e colunas excluídas), as seguintes métricas:

  - `missing_total`
  - `missing_focus` (somente linhas com `HAS_FOCO == 1`)
  - `missing_nonfocus`
  - `pct_missing_total`
  - `pct_missing_focus`
  - `pct_missing_nonfocus`

- Isso é salvo em `missing_by_column.csv`, ordenado decrescentemente por `pct_missing_total`.

README automático por ano:

- A função `write_year_readme` gera `README_missing.md` dentro de `data/eda/dataset/{ANO}`, contendo:

  - resumo geral do ano:
    - `rows_total`
    - número e proporção de linhas com foco (`HAS_FOCO == 1`)
  - explicação de todas as colunas do `missing_by_column.csv`
  - uma tabela com as 5 colunas com maior `pct_missing_total` no ano

Interface de linha de comando:

```bash
# auditoria de missing para todos os anos detectados
python src/dataset_missing_audit.py

# auditoria apenas para alguns anos específicos
python src/dataset_missing_audit.py --years 2004 2005 2015
```
Esse passo é usado como insumo direto para decisões registradas em doc/followup_decisions.md, como:
  - quais variáveis descartar
  - quais anos têm buracos graves
  - quais colunas exigem tratamento especial antes de ir para modelagem

---

### 4.6 Bases para modelagem (modeling_build_datasets.py)

Depois de consolidar e auditar `inmet_bdq_{ANO}_cerrado.csv`, o módulo `modeling_build_datasets.py` constrói múltiplas visões da base já pensadas para modelagem. Ele lê exclusivamente os CSVs em `data/dataset/` e grava saídas em parquet em `data/modeling/<cenário>/inmet_bdq_{ANO}_cerrado.parquet`.

Objetivo geral:

- Padronizar a semântica de missing (NaN + sentinelas).
- Harmonizar a coluna de radiação global entre anos.
- Gerar, para cada ano, 6 cenários de base, variando:
  - presença ou não da coluna de radiação
  - estratégia para lidar com missing em features (manter, dropar linhas ou imputar via KNN).

Caminhos usados:

- entrada:
  - `DATASET_DIR = paths.data.dataset`
  - padrão de arquivo: `inmet_bdq_*_cerrado.csv`
- saída:
  - `MODELING_DIR = paths.data.modeling` (ou `data/modeling/` por padrão)
  - para cada ano e cenário:
    - `data/modeling/<cenario>/inmet_bdq_{ANO}_cerrado.parquet`

Definições centrais:

- códigos tratados como faltantes: `{ -999, -9999 }`
- colunas excluídas das features (não entram no KNN nem nos filtros de linhas):
  - `DATA (YYYY-MM-DD)`, `HORA (UTC)`, `CIDADE`, `cidade_norm`, `ts_hour`, `ANO`
- colunas alvo (não são imputadas):
  - `RISCO_FOGO`, `FRP`, `FOCO_ID`
- label:
  - `HAS_FOCO`
- coluna canônica de radiação global:
  - `RADIACAO GLOBAL (KJ/m²)`
  - caso exista também `RADIACAO GLOBAL (Kj/m²)`, os valores são unificados nesta coluna e a variante antiga é dropada

Detecção de anos:

- `discover_year_files` procura todos os CSVs com o padrão configurado.
- Extrai o ano dos dígitos no nome do arquivo (`2004`, `2015` etc.).
- Cria o dicionário `{ano: caminho}` e processa em ordem crescente.

Semântica de missing aplicada antes de qualquer cenário:

1. `apply_missing_semantics`:
   - colunas numéricas:
     - substitui `-999` e `-9999` por `NaN`
   - colunas de texto:
     - strings vazias viram `NaN`
2. `coerce_feature_columns_to_numeric`:
   - identifica colunas de feature a partir de:
     - todas as colunas menos:
       - `TARGET_COLS`
       - `LABEL_COL`
       - `EXCLUDE_NON_NUMERIC`
   - tenta converter todas essas features para numérico (`errors="coerce"`, ou seja, lixo textual vira `NaN`)

Opcionalmente, se existirem as colunas, a base é ordenada por:

- `ANO`
- `DATA (YYYY-MM-DD)`
- `HORA (UTC)`

Cenários gerados por ano:

para cada ano, o builder tenta gerar os 6 cenários a seguir (pulando aqueles cujo parquet já exista, a menos que `overwrite_existing=True`):

1. `base_F_full_original`
   - Mantém a coluna de radiação global.
   - Aplica apenas:
     - harmonização de radiação
     - semântica de missing (sentinelas viram NaN)
     - coerção numérica nas features
   - Não remove linhas.
   - Não faz imputação.

2. `base_A_no_rad`
   - A partir da base já harmonizada (`df_year`), remove a coluna de radiação global, se existir.
   - Não remove linhas.
   - Não faz imputação.

3. `base_B_no_rad_knn`
   - Começa de uma cópia de `base_A` (sem radiação).
   - Identifica colunas de feature numéricas.
   - Aplica `KNNImputer` (vizinho mais próximo) apenas nessas colunas numéricas.
   - Label e targets permanecem como estão (não entram no KNN).

4. `base_C_no_rad_drop_rows`
   - Começa de uma cópia de `base_A` (sem radiação).
   - Remove todas as linhas que tenham qualquer `NaN` em colunas de feature.
   - Mantém linhas completas em termos de features.

5. `base_D_with_rad_drop_rows`
   - Começa da base `df_year` com radiação.
   - Remove todas as linhas com qualquer `NaN` em colunas de feature.
   - Label e targets são preservados, mas só para linhas completas.

6. `base_E_with_rad_knn`
   - Começa da base `df_year` com radiação.
   - Aplica `KNNImputer` nas colunas de feature numéricas (incluindo radiação, se numérica).
   - Mantém a radiação na base final.

Controle de sobrescrita:

- Por padrão, `overwrite_existing=False`:
  - se todos os 6 parquets para um ano já existem, o ano inteiro é pulado.
  - se apenas um cenário existir, só aquele cenário é pulado, os demais são construídos normalmente.
- Para refazer tudo de um ano (ou de todos), usar a flag `--overwrite-existing` na CLI.

Interface de linha de comando:

```bash
# gerar todos os cenários de modelagem para todos os anos detectados
python src/modeling_build_datasets.py

# gerar cenários só para alguns anos
python src/modeling_build_datasets.py --years 2004 2005 2015

# usando mais vizinhos no KNN e sobrescrevendo saídas existentes
python src/modeling_build_datasets.py --n-neighbors 7 --overwrite-existing

# se o nome da coluna de radiação canonizada mudar
python src/modeling_build_datasets.py --radiacao-col "RADIACAO GLOBAL (kJ/m2)"
```

---

## 5. Pré-requisitos

* Python 3.10 ou superior (idealmente 3.11).
* Acesso à internet para:

  * Instalação de dependências via `pip`.
  * Download de dados do COIDS/INPE e do INMET.
* Permissão de escrita no diretório do projeto para criação de:

  * Logs em `logs/`.
  * Arquivos de dados em `data/`.

---

## 6. Ambiente Python (venv)

Recomendação:

* Sempre usar um ambiente virtual dedicado ao projeto para garantir reprodutibilidade e evitar conflitos de versões.

### 6.1 Windows

```powershell
# na raiz do projeto
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# se a ativação for bloqueada:
# Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

pip install --upgrade pip
pip install -r requirements.txt
```

### 6.2 Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Para sair do venv:

```bash
deactivate
```

---

## 7. Configuração do projeto

O arquivo `config.yaml` na raiz centraliza:

* Caminhos de dados:

  * `paths.data.raw`
  * `paths.data.processed`
  * `paths.data.external`
* Configuração de logs:

  * Local padrão de logs.
  * Nível de logging.
  * Tamanho máximo e número de arquivos rotacionados.
* Outras opções de I/O:

  * Por exemplo, se diretórios ausentes devem ser criados automaticamente.

Todos os scripts devem importar a configuração por meio de `utils`:

```python
from utils import loadConfig, get_logger, get_path

cfg = loadConfig()
log = get_logger("exemplo.modulo")
raw_inmet = get_path("paths", "data", "raw")
proc_inmet = get_path("paths", "data", "processed")
```

Essa abordagem evita caminhos hardcoded espalhados e torna o pipeline relocável, desde que o `config.yaml` seja ajustado.

---

## 8. Execução rápida

### 8.1 Pipeline BDQueimadas

Baixar e extrair os zips de Brasil_sat_ref para anos específicos:

```bash
# exemplo: baixar 2015 a 2017 e extrair para data/processed/ID_BDQUEIMADAS
python src/bdqueimadas_scraper.py --years 2015 2016 2017
```

Se quiser apenas baixar, sem extrair:

```bash
python src/bdqueimadas_scraper.py --years 2015 2016 2017 --no-extract
```

Consolidar MANUAL × PROCESSADO para um ano específico e bioma Cerrado:

```bash
# certifique-se de já ter:
# - exportador_*_ref_2015.csv em data/raw/BDQUEIMADAS
# - focos_br_ref_2015.csv extraído em data/processed/ID_BDQUEIMADAS/focos_br_ref_2015/
python src/consolidated_bdqueimadas.py --years 2015 --biome "Cerrado"
```

Consolidar multi-anos, gerando um único arquivo:

```bash
python src/consolidated_bdqueimadas.py --years 2015 2016 2017 --biome "Cerrado"
```

O resultado típico ficará em:

```text
data/external/BDQUEIMADAS/bdq_targets_2015_2017_cerrado.csv
```

### 8.2 Pipeline INMET

A execução exata do pipeline INMET depende dos parâmetros implementados em `inmet_scraper.py` e `inmet_consolidated.py`. Como a documentação precisa evitar adivinhações, os comandos a seguir são meramente ilustrativos de formato e serão substituídos por exemplos reais quando os scripts forem incorporados:

```bash
# exemplo ilustrativo de chamada
python src/inmet_scraper.py
python src/inmet_consolidated.py
```

Resultado esperado em alto nível:

* Saídas intermediárias em `data/processed/INMET/`, organizadas por ano ou estação.

### 8.3 Join BDQ + INMET, auditoria e modelagem

Construir datasets integrados por ano para o Cerrado:

```bash
# formato ilustrativo; detalhes exatos dependem da implementação de build_dataset.py
python src/build_dataset.py --biome "Cerrado"
```

Rodar auditoria de missing em uma base consolidada:

```bash
# pretendendo auditar, por exemplo, data/dataset/inmet_bdq_2015_cerrado.csv
python src/dataset_missing_audit.py --year 2015 --biome "Cerrado"
```

Gerar bases específicas de modelagem:

```bash
# formato ilustrativo; será detalhado após envio de modeling_build_datasets.py
python src/modeling_build_datasets.py
```

Assim que os scripts correspondentes forem anexados, esta seção será atualizada com comandos reais, parâmetros aceitos e exemplos de saída.

---

## 9. Convenções, nomes e logs

Nomes de arquivos (convenções atuais):

* BDQueimadas:

  * Exportações manuais:

    * `exportador_{export_date}_ref_{ref_year}.csv` em `data/raw/BDQUEIMADAS/`.
  * Zips Brasil_sat_ref:

    * `focos_br_ref_{YYYY}.zip` em `data/raw/ID_BDQUEIMADAS/`.
    * Após extração: `data/processed/ID_BDQUEIMADAS/focos_br_ref_{YYYY}/focos_br_ref_{YYYY}.csv`.
  * Consolidados:

    * `bdq_targets_{YYYY}[_<bioma>].csv`
    * `bdq_targets_{Y1}_{Y2}[_<bioma>].csv`
    * `bdq_targets_all_years[_<bioma>].csv`

* Datasets integrados BDQ + INMET:

  * `inmet_bdq_{ANO}_cerrado.csv` em `data/dataset/`.

Logs:

* Cada módulo configura seu logger via `get_logger`, por exemplo:

  * `get_logger("bdqueimadas.scraper", kind="scraper", per_run_file=True)`
  * `get_logger("bdqueimadas.consolidate", kind="load", per_run_file=True)`
* O uso de `per_run_file=True` permite rastrear execuções individuais (com timestamp ou sufixo), facilitando debugging.

---

## 10. Pontos em aberto e próximos passos

Para deixar explícito o que ainda está em construção ou precisa ser decidido:

* Detalhamento do pipeline INMET:

  * Definir e documentar:

    * Quais variáveis do INMET são usadas.
    * Qual a granularidade temporal final (horária, diária etc.).
    * Como são tratados sentinelas específicos do INMET.
* Documentação de `build_dataset.py`:

  * Descrever exatamente:

    * Qual é a chave de junção entre BDQueimadas e INMET.
    * Quais filtros espaciais e temporais são aplicados.
    * Estrutura final dos arquivos em `data/dataset/`.
* Estratégia definitiva para variáveis com alta taxa de missing:

  * Em especial `RISCO_FOGO` e `FRP`, que apresentam percentuais muito elevados de ausência.
  * Ainda está em aberto se:

    * Serão utilizadas apenas em subset de anos mais completos.
    * Serão tratadas por imputação especializada.
    * Serão excluídas de alguns experimentos de modelagem.
* Documentação detalhada de `dataset_missing_audit.py`:

  * Estrutura dos relatórios gerados.
  * Como os resultados são integrados a `doc/followup_decisions.md`.
* Documentação de `modeling_build_datasets.py`:

  * Definir e registrar:

    * Como é construída a variável alvo (por exemplo, definição operacional de risco de fogo ou ocorrência de foco).
    * Como são feitas as splits de treino e teste.
    * Quais features entram na primeira rodada de modelos.
* Integração com notebooks do TCC:

  * Logo que os notebooks principais forem estabilizados e exportados para `.py`, descrever a ligação entre eles e os scripts de ETL.

---

## 11. Solução de problemas

Alguns problemas recorrentes e como lidar com eles:

* Erro ao ativar venv no Windows:

  * Rodar o PowerShell como usuário e executar:

    * `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned`
* Diretórios não encontrados:

  * Verificar se `paths.data.raw`, `paths.data.processed` e `paths.data.external` estão corretamente apontando para as pastas dentro do projeto.
  * Garantir que a opção de criação automática de diretórios, se existir no `config.yaml`, está habilitada.
* Conflitos de versão de dependências:

  * Atualizar `pip` e reinstalar:

    * `pip install --upgrade pip`
    * `pip install -r requirements.txt`
* Problemas de encoding em CSVs:

  * O leitor `_read_csv_smart` já tenta `utf-8-sig`, `utf-8` e `latin1`.
  * Se mesmo assim houver erro, verificar manualmente o encoding do arquivo problemático e registrar a decisão em `doc/followup_decisions.md`.
