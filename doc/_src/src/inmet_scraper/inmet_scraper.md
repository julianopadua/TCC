# inmet_scraper.py – Documentação Técnica Interna  

---  

## 1. Visão geral e responsabilidade  

`inmet_scraper.py` implementa o **pipeline de coleta, download e preparação** dos dados históricos do INMET (Instituto Nacional de Meteorologia).  
Ele realiza, sequencialmente:  

1. **Descoberta** de arquivos ZIP disponíveis na página de dados históricos.  
2. **Download** dos ZIPs para o diretório *raw/INMET*.  
3. **Extração** dos arquivos ZIP para *raw/INMET/csv*.  
4. **Consolidação** dos CSVs extraídos em arquivos anuais no diretório *processed/INMET*.  

O módulo também contém uma função auxiliar para extrair arquivos ZIP de outro provedor (*BDQueimadas*), embora seu uso seja opcional.

---  

## 2. Posicionamento na arquitetura  

| Camada / Domínio | Descrição |
|------------------|-----------|
| **Data Ingestion** (Coleta) | Responsável por obter dados brutos de fontes externas. |
| **Utilitário** | Depende exclusivamente de funções genéricas definidas em `utils.py`. |
| **Sem UI** | Não expõe interface de usuário; é acionado via linha de comando (`python -m src.inmet_scraper`) ou importado por scripts de orquestração. |

---  

## 3. Interfaces e exports  

O módulo **não** define classes nem exporta objetos além das funções de alto nível que podem ser reutilizadas por outros scripts:

| Nome | Tipo | Descrição |
|------|------|-----------|
| `discover_inmet_zip_links` | `def(base_url: str = INMET_BASE_URL) -> list[str]` | Retorna URLs absolutas de arquivos `.zip` encontrados na página do INMET. |
| `download_inmet_archives` | `def(links: list[str]) -> None` | Faz download dos ZIPs para `INMET_RAW_DIR`, ignorando arquivos já presentes. |
| `extract_inmet_archives` | `def() -> None` | Descompacta todos os ZIPs de `INMET_RAW_DIR` para `INMET_CSV_DIR`, criando sub‑diretórios por nome de ZIP. |
| `extract_bdqueimadas_archives` | `def() -> None` | Descompacta ZIPs de `BDQUEIMADAS` (opcional). |
| `consolidate_inmet_after_extract` | `def(years: list[int] | None = None, overwrite: bool = False) -> None` | Consolida CSVs extraídos em arquivos anuais no diretório *processed*. |
| `main` | `def() -> None` | Orquestra o fluxo completo (cria diretórios, executa as etapas acima). |

---  

## 4. Dependências e acoplamentos  

| Tipo | Módulo / Biblioteca | Motivo da dependência |
|------|----------------------|-----------------------|
| **Externa** | `requests` | Sessões HTTP reutilizáveis (`get_requests_session`). |
| **Externa** | `beautifulsoup4` (opcional `lxml`) | Parsing HTML para localizar links de ZIP (`list_zip_links_from_page`). |
| **Interna** | `utils.py` | Funções auxiliares de configuração, logging, I/O e processamento (`loadConfig`, `get_logger`, `ensure_dir`, etc.). |
| **Interna** | `process_inmet_years` (importado de `utils`) | Consolidção de CSVs por ano. |

O módulo **não** importa nenhum outro componente da aplicação, mantendo um acoplamento baixo ao restante do código‑base.

---  

## 5. Leitura guiada do código (top‑down)  

1. **Configuração inicial**  
   ```python
   cfg = loadConfig()
   log = get_logger("inmet.scraper", kind="scraper", per_run_file=True)
   INMET_BASE_URL = cfg.get("inmet", {}).get("base_url") or "https://portal.inmet.gov.br/dadoshistoricos"
   INMET_RAW_DIR, INMET_CSV_DIR = get_inmet_paths()
   ```  
   - Carrega `config.yaml`.  
   - Cria logger dedicado (`inmet.scraper`).  
   - Define URL base com *fallback* estático.  
   - Obtém caminhos de diretórios configurados via `utils`.  

2. **`discover_inmet_zip_links`**  
   - Cria sessão HTTP (`get_requests_session`).  
   - Usa `list_zip_links_from_page` para extrair links que terminam em `.zip`.  
   - Loga a quantidade encontrada.  

3. **`download_inmet_archives`**  
   - Itera sobre a lista de URLs.  
   - Deriva o nome do arquivo a partir da URL (`os.path.basename`).  
   - Verifica existência prévia para evitar downloads redundantes.  
   - Chama `stream_download` (download em blocos) com tratamento de exceções genéricas (log de erro).  

4. **`extract_inmet_archives`**  
   - Delegação direta a `unzip_all_in_dir`, que descompacta cada ZIP e cria sub‑diretório com o nome do arquivo ZIP (`make_subdir_from_zip=True`).  

5. **`extract_bdqueimadas_archives`** (opcional)  
   - Recupera caminhos específicos de *BDQueimadas* via `get_bdqueimadas_paths`.  
   - Reutiliza a mesma lógica de descompactação.  

6. **`consolidate_inmet_after_extract`**  
   - Determina os anos a processar:  
     - Usa parâmetro explícito ou `config.yaml`.  
     - Caso ausente, tenta inferir a partir de sub‑pastas numéricas em `INMET_CSV_DIR`.  
   - Se nenhum ano for encontrado, emite *warning* e aborta.  
   - Invoca `process_inmet_years` (de `utils`) para gerar arquivos `processed/INMET/inmet_{year}.csv`.  

7. **`main`**  
   - Garante existência dos diretórios *raw* e *csv*.  
   - Executa as etapas de descoberta, download, extração e consolidação.  
   - Comentário indica possibilidade de chamar `extract_bdqueimadas_archives`.  

8. **Entrypoint**  
   ```python
   if __name__ == "__main__":
       main()
   ```  
   Permite execução direta como script.

---  

## 6. Fluxo de dados / estado  

```
config.yaml ──► cfg (INMET_BASE_URL, INMET_RAW_DIR, INMET_CSV_DIR, years)
      │
      ▼
discover_inmet_zip_links ──► lista de URLs .zip
      │
      ▼
download_inmet_archives ──► arquivos .zip em INMET_RAW_DIR
      │
      ▼
extract_inmet_archives ──► CSVs em INMET_CSV_DIR/<nome_zip>/
      │
      ▼
consolidate_inmet_after_extract ──► processed/INMET/inmet_{year}.csv
```

O estado interno é mantido exclusivamente no sistema de arquivos; não há objetos mutáveis compartilhados entre funções.

---  

## 7. Conexões com outros arquivos do projeto  

| Módulo | Tipo de vínculo | Comentário |
|--------|----------------|------------|
| `utils.py` | **Importação direta** (`from utils import …`) | Fornece todas as funções auxiliares (config, logging, I/O, processamento). |
| `config.yaml` (não‑code) | **Leitura** via `loadConfig` | Define URL base, diretórios e lista de anos. |
| `process_inmet_years` (em `utils.py`) | **Chamada** na consolidação | Responsável por combinar CSVs por ano. |

> **Nota:** Não há importações externas adicionais nem dependências circulares.

---  

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Impacto | Recomendações |
|------|---------|---------------|
| **Tratamento genérico de exceções** (`except Exception as e`) | Pode mascarar erros críticos (ex.: falha de rede permanente). | Substituir por exceções específicas (`requests.exceptions.RequestException`, `OSError`). |
| **Ausência de verificação de integridade dos ZIPs** | Arquivos corrompidos podem gerar falhas silenciosas na extração. | Validar checksum (ex.: SHA‑256) ou usar `zipfile.is_zipfile` antes de descompactar. |
| **Dependência implícita de `beautifulsoup4`** | Caso a biblioteca não esteja instalada, a descoberta falha sem mensagem clara. | Documentar requisito no `requirements.txt` e capturar `ImportError` com log explicativo. |
| **Hard‑coded fallback de URL** | Se a página mudar, o script continuará usando a URL antiga sem aviso. | Expor fallback via variável de ambiente ou parâmetro de função. |
| **Consolidação automática de anos** | Inferir anos a partir de nomes de diretórios pode gerar falsos positivos/negativos. | Validar que cada diretório contém arquivos CSV antes de incluí‑lo na lista de anos. |
| **Uso de `os.path.basename(url.split("?")[0])`** | Não cobre URLs com caminhos complexos ou codificação de caracteres. | Utilizar `urllib.parse.urlparse` + `os.path.basename` para maior robustez. |
| **Ausência de testes unitários** (não visível no código) | Dificulta a garantia de corretude em mudanças futuras. | Implementar testes de integração que mockem `requests` e `utils` para cada etapa. |

---  

*Esta documentação foi gerada com base no código-fonte disponível e nas importações explícitas. Qualquer comportamento adicional presente em `utils.py` ou em arquivos de configuração externos não está descrito aqui.*
