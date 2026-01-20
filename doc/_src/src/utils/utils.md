# utils.py – Documentação Técnica Interna  

---  

## 1️⃣ Visão geral e responsabilidade  

`utils.py` reúne funções auxiliares de uso geral para o projeto **TCC**.  
Ele centraliza:

* **Configuração** – carregamento e normalização de `config.yaml`, resolução de caminhos relativos e criação automática de diretórios.  
* **Logging** – fábrica de `logging.Logger` configurado a partir do mesmo arquivo de configuração, com suporte a arquivos rotativos ou diários.  
* **Operações de sistema de arquivos** – criação garantida de diretórios, listagem recursiva de arquivos, normalização de chaves textuais.  
* **HTTP / Scraping** – sessão `requests` com política de retries, extração de links `.zip` de páginas HTML e download em streaming.  
* **Manipulação de arquivos ZIP** – extração segura e opcional de múltiplos arquivos.  
* **Helpers específicos de provedores** – caminhos padronizados para os datasets INMET e BDQUEIMADAS.  
* **Processamento de dados INMET** – leitura, limpeza e consolidação de CSVs anuais em um único arquivo pronto para análise.  

O módulo não contém lógica de negócio; ele fornece infraestrutura reutilizável para os demais pacotes.  

---  

## 2️⃣ Posicionamento na arquitetura  

| Camada / Domínio | Papel |
|------------------|-------|
| **Utilitários / Infraestrutura** | Funções de apoio (config, logging, I/O, HTTP, ZIP). |
| **Domínio de Dados** | `process_inmet_*` converte arquivos brutos do provedor INMET em formato analítico. |
| **Camada de Orquestração** | O bloco `if __name__ == "__main__"` demonstra uso como script de teste, mas a API principal é consumida por outros módulos (ex.: pipelines de ingestão). |

Não há dependência direta de UI ou camada de apresentação.  

---  

## 3️⃣ Interfaces e *exports*  

O módulo exporta (via importação direta) as seguintes funções/classes:

| Nome | Tipo | Descrição |
|------|------|-----------|
| `loadConfig` | `Callable[[str|Path|None, bool|None], Dict[str, Any]]` | Carrega e cacheia a configuração do projeto. |
| `get_path` | `Callable[[str, ...], Path]` | Recupera caminhos aninhados a partir da configuração. |
| `get_logger` | `Callable[[str, *, kind:str|None, per_run_file:bool], logging.Logger]` | Cria logger configurado (console + arquivo). |
| `ensure_dir` | `Callable[[Path|str], Path]` | Garante existência de diretório e devolve caminho absoluto. |
| `list_files` | `Callable[[Path|str, Iterable[str]], List[Path]]` | Busca arquivos por padrões glob recursivos. |
| `normalize_key` | `Callable[[str|None], str]` | Normaliza strings para comparações robustas. |
| `get_requests_session` | `Callable[[int, float], requests.Session]` | Sessão HTTP com política de retries. |
| `list_zip_links_from_page` | `Callable[[str, requests.Session|None], List[str]]` | Extrai URLs de arquivos `.zip` de uma página. |
| `stream_download` | `Callable[[str, Path|str, requests.Session|None, int, logging.Logger|None], Path]` | Faz download em streaming com progresso opcional. |
| `unzip_file` / `unzip_all_in_dir` | `Callable[...]` | Extrai arquivos ZIP, com controle de sobrescrita. |
| `get_inmet_paths`, `get_bdqueimadas_paths` | `Callable[[], Tuple[Path, Path]]` | Retorna diretórios *raw* e *csv* para cada provedor. |
| `process_inmet_year`, `process_inmet_years` | `Callable[...]` | Consolida CSVs do INMET por ano ou por lote. |

---  

## 4️⃣ Dependências e acoplamentos  

| Tipo | Bibliotecas | Motivo |
|------|--------------|--------|
| **Padrão** | `os`, `sys`, `logging`, `pathlib`, `typing`, `datetime`, `re`, `unicodedata`, `zipfile` | Operações de sistema, tipagem e manipulação de datas. |
| **Terceiros** | `pandas`, `requests`, `urllib3`, `yaml`, `beautifulsoup4` (lazy), `lxml` (opcional) | Processamento de dados tabulares, HTTP resiliente, leitura de YAML e parsing HTML. |
| **Internas** | *Nenhuma* (arquivo autônomo) | Não há imports de outros módulos do repositório. |

Acoplamento externo está limitado a versões recentes das bibliotecas citadas; a presença de `zoneinfo` (Python ≥ 3.9) é opcional, com fallback para timezone local.  

---  

## 5️⃣ Leitura guiada (top‑down)  

### 5.1 Variáveis globais de cache  

```python
_CONFIG_CACHE: Dict[str, Any] | None = None
_ROOT_CACHE: Path | None = None
```

*Cache* evita recarregamento de `config.yaml` e recomputação da raiz do projeto.  

### 5.2 Descoberta da raiz do projeto  

```python
def _find_project_root() -> Path:
    """Assume <root>/src/utils.py e retorna <root>."""
    return Path(__file__).resolve().parents[1]
```

A função usa a posição física do arquivo para inferir a raiz, garantindo que caminhos relativos sejam resolvidos de forma determinística.  

### 5.3 Resolução de caminhos  

* `_expand_path` → expande `~` e variáveis de ambiente, converte para absoluto relativo à raiz.  
* `_resolve_paths` → percorre recursivamente dicionários/listas e aplica `_expand_path` a strings que aparentam ser caminhos.  

Essas funções mantêm a **invariável** de que todos os caminhos expostos em `config.yaml["paths"]` são absolutos e existentes (quando `create_dirs=True`).  

### 5.4 `loadConfig`  

```python
def loadConfig(config_path: str | Path | None = None,
               create_dirs: bool | None = None) -> Dict[str, Any]:
    """Carrega config.yaml, injeta paths.root, resolve caminhos e cria diretórios."""
    ...
```

* Prioridade de localização: argumento → variável de ambiente `PROJECT_CONFIG` → `<root>/config.yaml`.  
* Usa `yaml.safe_load` e garante a presença da chave `paths`.  
* Resolve caminhos com `_resolve_paths`.  
* Opcionalmente cria diretórios via `_create_all_paths`.  
* Resultado é armazenado em `_CONFIG_CACHE`.  

### 5.5 Recuperação de caminhos (`get_path`)  

```python
def get_path(*keys: str) -> Path:
    cfg = loadConfig()
    node: Any = cfg
    for k in keys:
        node = node[k]
    return Path(node)
```

Permite acesso tipado a caminhos aninhados (`paths.data.processed`, etc.).  

### 5.6 Criação de diretórios (`_create_all_paths`)  

Coleta recursivamente todos os valores string dentro de `cfg["paths"]` e do arquivo de log, chamando `Path.mkdir(parents=True, exist_ok=True)`.  

### 5.7 Funções de data/hora  

* `_now_tz` → devolve `datetime.now()` no fuso horário configurado (`project.timezone`).  
* `_build_daily_log_file` → gera caminho `logs/log_YYYYMMDD/kind_HHMMSS.log`.  

### 5.8 Logging (`get_logger`)  

```python
def get_logger(name: str = "app", *, kind: str | None = None,
               per_run_file: bool = False) -> logging.Logger:
    """Configura logger com console + (rotativo ou diário) arquivo."""
    ...
```

* Nível de log vem de `config.yaml["logging"]["level"]`.  
* Se `per_run_file` e `kind` forem informados, usa `_build_daily_log_file`.  
* `RotatingFileHandler` recebe `max_bytes` e `backup_count` da configuração.  

### 5.9 Helpers de filesystem  

* `ensure_dir` – cria diretório e devolve `Path` absoluto.  
* `list_files` – `Path.rglob` para múltiplos padrões.  
* `normalize_key` – normaliza strings (unicode NFKD, remoção de diacríticos, casefold, remoção de símbolos).  

### 5.10 HTTP / Scraping  

* `get_requests_session` – `requests.Session` com `urllib3.Retry` (exponencial, 3 tentativas por padrão).  
* `_soup` – lazy import de `BeautifulSoup`; escolhe parser `lxml` quando disponível.  
* `list_zip_links_from_page` – extrai links `.zip` absolutos de uma página.  
* `stream_download` – download em blocos, grava em arquivo temporário `.part` e move ao final; opcionalmente loga progresso.  

### 5.11 ZIP  

* `unzip_file` – extrai um ZIP, opcionalmente pula se o diretório já existir; retorna lista de arquivos extraídos.  
* `unzip_all_in_dir` – itera sobre todos os `*.zip` em um diretório e delega a `unzip_file`.  

### 5.12 Provedores específicos  

* `get_inmet_paths` / `get_bdqueimadas_paths` – retornam tupla `(raw_dir, csv_dir)` a partir de `config.yaml["paths"]["providers"]`.  

### 5.13 Processamento INMET  

Fluxo resumido:

1. Detecta diretório do ano (`_inmet_year_dir`).  
2. Para cada CSV:  
   * Lê cabeçalho na linha 9 (`_parse_inmet_header`).  
   * Carrega dados com `pandas.read_csv(sep=';', skiprows=9)`.  
   * Ajusta desalinhamento de colunas (adiciona `COLUNA_EXTRA_n` ou descarta).  
   * Insere metadados (`ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`).  
   * Remove colunas configuradas em `_INMET_DROP_COLS`.  
3. Concatena todos os DataFrames e grava em `processed/inmet_{year}.csv`.  

### 5.14 Bloco de teste (`__main__`)  

Carrega configuração, cria logger de teste e processa os anos definidos em `config.yaml["inmet"]["years"]`.  

---  

## 6️⃣ Fluxo de dados / estado / eventos  

1. **Inicialização** – ao primeiro `loadConfig`, o módulo lê `config.yaml` e popula `_CONFIG_CACHE`.  
2. **Estado global** – `_CONFIG_CACHE` e `_ROOT_CACHE` permanecem imutáveis após carregamento; funções de leitura (ex.: `get_path`) dependem apenas desse estado.  
3. **Eventos de I/O** – `stream_download`, `unzip_file` e `process_inmet_year` emitem logs (INFO/WARN/ERROR) que podem ser capturados por handlers externos.  
4. **Fluxo de dados** – Dados brutos (ZIP/CSV) → download/extração → normalização (`normalize_key`) → consolidação (`process_inmet_year`) → CSV processado.  

---  

## 7️⃣ Conexões com outros arquivos do projeto  

| Módulo | Tipo de relação | Comentário |
|--------|----------------|------------|
| `src/config.yaml` | **Leitura** | Fonte única de parâmetros (paths, logging, io, filenames, etc.). |
| `src/pipelines/*.py` (ex.: `ingest_inmet.py`) | **Importa** | Utiliza `loadConfig`, `get_logger`, `stream_download`, `unzip_file`, `process_inmet_year`. |
| `src/models/*.py` | **Possível** | Pode consumir CSVs gerados por `process_inmet_year`. |
| `src/main.py` | **Possível** | Pode chamar `get_logger` para logger global da aplicação. |

> **Nota:** Os links reais não foram fornecidos; substitua pelos caminhos corretos do repositório.  

---  

## 8️⃣ Pontos de atenção, riscos e melhorias recomendadas  

| Item | Risco / Observação | Recomendações |
|------|--------------------|---------------|
| **Cache de configuração** | Não há mecanismo de invalidação; alterações em `config.yaml` exigem reinício do processo. | Implementar `reload_config()` opcional que limpa `_CONFIG_CACHE`. |
| **Dependência de `zoneinfo`** | Em ambientes < 3.9 a timezone pode ficar implícita (local). | Documentar claramente a necessidade de Python ≥ 3.9 ou exigir `tzdata` no Docker. |
| **Uso de `yaml.safe_load`** | Se o arquivo contiver tags customizadas pode gerar `ConstructorError`. | Validar esquema de configuração (ex.: com `cerberus` ou `pydantic`). |
| **`stream_download`** – tamanho de chunk fixo | Pode ser ineficiente para conexões de alta latência ou arquivos muito pequenos. | Tornar `chunk_size` configurável via `config.yaml["io"]["chunk_size"]`. |
| **`unzip_file`** – `skip_if_exists=True` | Se o diretório existir mas estiver incompleto, a extração é ignorada silenciosamente. | Oferecer parâmetro `force` ou checagem de integridade (ex.: checksum). |
| **`process_inmet_year`** – leitura de cabeçalho rígida | Arquivos que mudem a posição do cabeçalho quebram o parser. | Detectar dinamicamente a linha de cabeçalho (ex.: buscar linha que contenha todas as colunas esperadas). |
| **Logging rotativo** – `max_bytes` e `backup_count` fixos | Em ambientes de alta rotatividade pode gerar perda de logs antigos. | Expor esses parâmetros no `config.yaml` (já feito) e validar limites razoáveis. |
| **Importação tardia de BeautifulSoup** | Falha silenciosa se `beautifulsoup4` não estiver instalado. | Documentar dependência opcional e incluir no `requirements.txt`. |
| **Tipagem** | Algumas funções retornam `list[Path]` mas anotam `List[Path]`; mistura de sintaxe Python 3.9+ e `typing`. | Uniformizar para `list[Path]` ou `List[Path]` conforme versão mínima suportada. |
| **Testabilidade** | Funções de I/O (download, unzip) não têm mocks internos. | Criar wrappers ou injetar dependências para facilitar testes unitários. |

---  

*Documentação gerada em 13/01/2026.*
