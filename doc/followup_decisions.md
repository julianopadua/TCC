# Follow-up de Decisões - Projeto TCC (Previsão de Queimadas)

> Documento vivo para registrar decisões de engenharia de dados, convenções e trade-offs ao longo do TCC. Alinhado aos objetivos/metodologia definidos no manuscrito do TCC. 

## Sumário
- [1. Estrutura de pastas e do projeto](#1-estrutura-de-pastas-e-do-projeto)
- [2. Extração inicial - BD Queimadas (INPE)](#2-extração-inicial-bd-queimadas-inpe)
  - [2.1 Escopo e fonte](#21-escopo-e-fonte)
  - [2.2 Parâmetros de exportação adotados](#22-parâmetros-de-exportação-adotados)
  - [2.3 Fluxo manual atual (passo a passo)](#23-fluxo-manual-atual-passo-a-passo)
  - [2.4 Convenção de nomenclatura de arquivos](#24-convenção-de-nomenclatura-de-arquivos)
  - [2.5 Justificativa técnica da convenção](#25-justificativa-técnica-da-convenção)
  - [2.6 Riscos/limitações conhecidos](#26-riscoslimitações-conhecidos)
  - [2.7 Próximos passos (automatização)](#27-próximos-passos-automatização)
- [3. Scraper - INMET](#3-scraper-inmet)
  - [3.1 Escopo e fonte](#31-escopo-e-fonte)
  - [3.2 Fluxo do algoritmo (pipeline)](#32-fluxo-do-algoritmo-pipeline)
  - [3.3 Logging e rastreabilidade](#33-logging-e-rastreabilidade)
  - [3.4 Decisões de desenho (design)](#34-decisões-de-desenho-design)
  - [3.5 Nota sobre consolidação anual (pós-extração)](#35-nota-sobre-consolidação-anual-pós-extração)
  - [3.6 Convenções de diretórios](#36-convenções-de-diretórios)
  - [3.7 Assunções e limitações](#37-assunções-e-limitações)
  - [3.8 Parâmetros de configuração usados](#38-parâmetros-de-configuração-usados)
  - [3.9 Trade-offs e porquês](#39-trade-offs-e-porquês)
  - [3.10 Próximos passos](#310-próximos-passos)
- [4. Consolidação](#4-consolidação)

---

## 1. Estrutura de pastas e do projeto
_em construção…_

---

## 2. Extração inicial - BD Queimadas (INPE)

### 2.1 Escopo e fonte
Os dados de focos de calor são obtidos via BDQueimadas (módulo TerraBrasilis/INPE), um WebGIS que permite filtrar e exportar pontos (focos) por recortes espaciais/temporais e camadas (país, estado, bioma, etc.), com exportação em CSV/GeoJSON/KML/Shapefile.   
Na própria interface “BDQueimadas”, os filtros incluem Continentes, Países, Estados, Municípios, UCs/TIs, período (Data Início/Fim), satélites (incluindo a opção “Satélite de referência (Aqua Tarde)”) e Biomas (Brasil); a aba “Exportar Dados” envia o arquivo para o e-mail informado no formato escolhido. 

**Definição operacional de “foco”:** foco indica a existência de fogo em um elemento de resolução (pixel) da imagem de satélite, cuja dimensão varia conforme o sensor (≈375 m a 5×4 km). 

**Por que usar “Satélite de referência (Aqua Tarde)”:** o INPE utiliza um satélite de referência para garantir comparabilidade temporal das séries; a própria “Situação Atual” do portal e notas técnicas estaduais baseadas no INPE destacam que as comparações interanuais usam **apenas** o satélite de referência (AQUA Tarde). 

### 2.2 Parâmetros de exportação adotados
- **Continentes:** América do Sul  
- **Países:** Brasil  
- **Estados:** Todos os estados  
- **Municípios / UCs/TIs:** em branco  
- **Satélites:** Satélite de referência (Aqua Tarde)  
- **Biomas (Brasil):** Todos  
- **Janela temporal:** 1º de janeiro a 31 de dezembro de cada ano  
- **Formato de exportação:** CSV (enviado por e-mail; portal informa uso do e-mail apenas para envio e estatísticas de acesso) 

### 2.3 Fluxo manual atual (passo a passo)
1. Acessar o BDQueimadas no TerraBrasilis e configurar os filtros da seção 2.2.   
2. Definir o intervalo anual completo (01/01-31/12) e **Aplicar**.   
3. Informar o e-mail na seção **Exportar Dados** e selecionar **CSV**.   
4. Receber um **ZIP** por e-mail contendo o CSV e **extrair** localmente.   
5. O arquivo chega com padrão **`exportador_YYYY-MM-DD HH:MM:SS.ssssss.csv`** (timestamp da exportação).  
6. Renomear conforme a convenção definida na seção 2.4.

### 2.4 Convenção de nomenclatura de arquivos
- **Original (do portal):**  
  `exportador_YYYY-MM-DD HH:MM:SS.ssssss.csv`  
- **Padrão adotado no projeto:**  
  `exportador_YYYY-MM-DD_ref_YYYY.csv`  
  **Ex.:** `exportador_2025-09-16_ref_2024.csv`

### 2.5 Justificativa técnica da convenção
- **Rastreabilidade dupla (evento x conteúdo):** preserva a **data da exportação** (audit trail) e torna explícito o **ano de referência** do conteúdo do CSV (01/01-31/12 daquele ano), reduzindo ambiguidade quando múltiplas exportações ocorrem no mesmo dia.  
- **Consistência com a prática do INPE (séries anuais):** a análise interanual oficial utiliza **apenas** o **satélite de referência**; ao explicitar `_ref_YYYY`, a série fica alinhada à comparabilidade temporal recomendada/inferida pelo próprio portal e relatórios técnicos estaduais baseados no INPE. 0}  
- **Prevenção de colisões e legibilidade em pipelines:** o sufixo `_ref_YYYY` favorece _parsing_ determinístico (regex simples) e organização por partição (e.g., `year=YYYY`) em data lakes, sem depender de metadados externos.  
- **Diagnóstico de regressões:** caso o INPE altere camadas/atributos, é possível correlacionar a mudança com a **data da extração** embutida no nome, mantendo a série legível para _debug_.  
- **Compatibilidade com múltiplas fontes:** a mesma convenção pode ser espelhada para outros provedores anuais (e.g., INMET), facilitando _joins_ por chave `ref_ano`.

### 2.6 Riscos/limitações conhecidos
- **Lacunas do satélite de referência:** houve interrupções conhecidas no MODIS/AQUA (ex.: 31/03/2022-13/04/2022), afetando séries que dependem exclusivamente do satélite de referência; é prudente registrar _flags_ de disponibilidade e, quando necessário, considerar satélites alternativos (VIIRS) com devida harmonização. 1}  
- **Diferenças de sensor/resolução entre satélites:** VIIRS (≈375 m) detecta mais focos que MODIS (≈1 km), o que inviabiliza comparações diretas “Todos os satélites” sem normalização; manter o **AQUA Tarde** como base de série reduz esse viés. 2}  
- **Semântica de “foco”:** um foco é uma detecção por pixel; não é sinônimo de “número de incêndios” nem “área queimada”. Interpretar métricas com essa ressalva. 3}

### 2.7 Próximos passos (automatização)
- Implementar _scraper/exporter_ reproduzindo fielmente os filtros da UI do BDQueimadas (incluindo _headers_ e o _payload_ necessário para geração e envio por e-mail), ou, se disponível, migrar para endpoints/documentação estável do TerraBrasilis. 4}  
- Padronizar _ingest_ para salvar diretamente como `year=YYYY/…/exportador_YYYY-MM-DD_ref_YYYY.csv` com _hash_ do conteúdo para controle de versão.  
- Criar rotina de **validação** pós-download (contagem de linhas, campos esperados, faixa de datas, distribuição por UF/bioma) e _data quality checks_ (percentual de nulos, domínios).  
- Registrar **metadados**: filtros, _query hash_, data/hora UTC da extração, versão do dicionário de atributos.  

---

## 3. Scraper - INMET

### 3.1 Escopo e fonte

O scraper automatiza a **coleta dos históricos do INMET** publicados no portal público de dados históricos (`/dadoshistoricos`). O objetivo é **baixar todos os .zip disponíveis**, extrair os CSVs e **consolidar** por ano em `data/processed/INMET/inmet_{ano}.csv`, preparando a etapa de *consolidation* global.

**Entradas**: página HTML com links para arquivos `.zip`.
**Saídas**:

* `data/raw/INMET/*.zip` (artefatos brutos)
* `data/raw/INMET/csv/<nome_zip>/*` (CSV extraído de cada zip)
* `data/processed/INMET/inmet_{YYYY}.csv` (um CSV por ano, pós-processado)

---

### 3.2 Fluxo do algoritmo (pipeline)

1. **Descoberta de links** (`discover_inmet_zip_links`)

   * Baixa o HTML e extrai **todas as âncoras que terminam em `.zip`**.
   * Normaliza para **URLs absolutas**.
   * Decisão: *crawler* simples e robusto (sem depender de estrutura estável da página).

2. **Download** (`download_inmet_archives`)

   * Usa `requests.Session` com **retries e backoff exponencial**.
   * **Streaming** de download para `*.part` e *rename* atômico ao final (evita arquivos corrompidos).
   * **Idempotência**: *skip* se o arquivo destino já existir.

3. **Extração** (`extract_inmet_archives`)

   * Extrai cada `.zip` para `raw/INMET/csv/<nome_do_zip>/` (um subdiretório por pacote).
   * **Skip se já extraído** (idempotência).

4. **Consolidação anual** (`consolidate_inmet_after_extract` → `utils.process_inmet_years`)

   * Para cada ano detectado, lê todos os CSVs de estação, **uniformiza cabeçalhos**, adiciona metadados (`ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`) e grava `inmet_{YYYY}.csv` em `processed/INMET`.
   * Observações técnicas desta etapa estão na Seção 3.5.

---

### 3.3 Logging e rastreabilidade

* **Estrutura de logs por dia**: ao iniciar, o utilitário cria `logs/log_YYYYMMDD/`.
* **Arquivo por execução**:

  * Scraper: `logs/log_YYYYMMDD/scraper_HHMMSS.log`
  * Load/Consolidação: `logs/log_YYYYMMDD/load_HHMMSS.log`
* **Nomes de *logger***: `"inmet.scraper"`, `"inmet.load"`, `"inmet.consolidate"`.
* **Rotação de arquivo** conforme `logging.max_bytes`/`backup_count` no `config.yaml`.
* **Motivação**: auditoria, *debug* e isolamento de runs.

---

### 3.4 Decisões de desenho (design)

* **Config-driven**: `inmet.base_url`, `paths.providers.inmet.*`, `filenames.patterns.inmet_csv` vêm do `config.yaml`.
* **Resiliência de rede**: `Retry` (429/5xx), *streaming* e escrita atômica (`.part` → final).
* **Idempotência**: *skip* de downloads já existentes e *skip* de extrações já realizadas.
* **Simplicidade do crawler**: filtro direto por sufixo `.zip` → menos frágil a mudanças de layout.
* **Sem paralelismo** por padrão: respeita *rate limits* implícitos e evita carga no servidor (pode ser habilitado depois com *throttling*).

---

### 3.5 Nota sobre consolidação anual (pós-extração)

A etapa `utils.process_inmet_years/process_inmet_year` padroniza cada `inmet_{YYYY}.csv` com as regras:

* **Leitura do cabeçalho** na **linha 9** dos CSVs originais de estação (index 8), conforme padrão INMET.
* **Metadados fixos**: adiciona `ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`.
* **Remoções**: descarta colunas intradiárias redundantes (máx/mín da hora anterior etc.).
* **Tolerância a desalinhamentos**: se o número de colunas nos dados ≠ cabeçalho, cria **`COLUNA_EXTRA_n`** ou **pula** quando faltarem campos (evita *shift* silencioso).
* **Saída**: um CSV anual **homogêneo** por ano em `data/processed/INMET/`.

> **Sobre os avisos “Colunas extras detectadas”**: sinalizam casos de **desalinhamento** (por exemplo, separadores `;` a mais, *labels* vazios no cabeçalho, aspas quebradas). O *loader* trata isso sem perda de campos relevantes; as colunas *extra* são descartadas depois.

---

### 3.6 Convenções de diretórios

* **Raw**: `data/raw/INMET/*.zip` e `data/raw/INMET/csv/<zip>/*`
* **Processed (por ano)**: `data/processed/INMET/inmet_{YYYY}.csv`
* **Consolidated (toda a série)**: `data/consolidated/INMET/inmet_all_years.csv`
* **Justificativa**: separa claramente o que é bruto, o que é normalizado por ano e o agregado final.

---

### 3.7 Assunções e limitações

* A página do INMET **expõe todos os .zip** via *links* (sem *auth*). Mudanças estruturais na página podem quebrar a descoberta.
* A estrutura interna dos zips (nomes/pastas) **pode variar** entre anos; a extração cria um subdir por zip para isolar.
* Os CSVs de estação trazem **`-9999`** como *sentinel* de ausentes; **não limpamos** nessa fase (limpeza é posterior).
* **Lat/Long** frequentemente vêm com **vírgula decimal** (`"-15,78"`); mantemos como *string* no *processed* (limpeza/conversão na fase de modelagem).

---

### 3.8 Parâmetros de configuração usados

Trechos relevantes do `config.yaml`:

```yaml
inmet:
  base_url: "https://portal.inmet.gov.br/dadoshistoricos"
  years: []                # opcional; se vazio, inferimos pelos diretórios
paths:
  providers:
    inmet:
      raw: "./data/raw/INMET"
      processed: "./data/processed/INMET"
filenames:
  patterns:
    inmet_csv: "inmet_{year}.csv"
logging:
  level: "INFO"
  file: "./logs/app.log"   # usado como base; utilitário cria logs/log_YYYYMMDD/...
  max_bytes: 5242880
  backup_count: 3
io:
  create_missing_dirs: true
```

---

### 3.9 Trade-offs e porquês

* **Crawler simples vs. *scraper* estruturado**: optamos por simplicidade (procura `.zip`), reduzindo acoplamento à UI.
* **Sem multithread no download**: menor risco de *ban* e menor carga no servidor; performance é aceitável pelo volume.
* **Normalização em duas fases**: primeiro **padroniza por ano** (confiável para *joins*), depois consolida o histórico completo (I/O puro, mais rápido e robusto).

---

### 3.10 Próximos passos

* **Checksums** (hash) pós-download para verificar integridade e evitar reprocessos desnecessários.
* **Catálogo de metadados**: registrar versão do dicionário de campos do INMET por ano e *diffs* de cabeçalho.
* **Throttle configurável** (+ paralelismo moderado) para acelerar com civilidade.
* **Validadores** pós-extração: contagem de linhas por estação, *range* de datas, distribuição de nulos.

---

## 4. Consolidação