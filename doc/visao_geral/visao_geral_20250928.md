# Visão geral do fluxo (INMET)

1. `inmet_scraper.py` (pipeline alto nível)

* `discover_inmet_zip_links()` → varre a página base e retorna URLs absolutas de `.zip`.
* `download_inmet_archives(links)` → baixa cada `.zip` para `raw/INMET`.
* `extract_inmet_archives()` → extrai todos os `.zip` de `raw/INMET` em `raw/INMET/csv/<nome_zip>`.
* `consolidate_inmet_after_extract()` → chama `utils.process_inmet_years(...)` para consolidar CSVs extraídos em arquivos anuais em `processed/INMET`.
* `main()` amarra tudo acima.

2. `utils.py` (núcleo de infra + consolidação)

* Config/paths: `loadConfig`, `_resolve_paths`, `get_path`, `get_inmet_paths`, `get_bdqueimadas_paths`.
* Logging: `get_logger` com `RotatingFileHandler`.
* HTTP: `get_requests_session` (retries/backoff), parsing HTML: `_soup`, descoberta de `.zip`: `list_zip_links_from_page`, download robusto: `stream_download`.
* Zip: `unzip_file`, `unzip_all_in_dir`.
* Consolidação INMET: `_INMET_DROP_COLS` (lista de colunas a remover), `_inmet_year_dir` (descoberta de pasta do ano), `_parse_inmet_header` (lê metadados da estação), `process_inmet_year` (consolida todas as estações de um ano), `process_inmet_years`.

3. `load_inmet_csv_data.py` (script “antigo”/paralelo)

* Lê `config.yaml` manualmente com `yaml.safe_load`.
* Caminhos montados com `script_dir` + chaves de `config`.
* Rotina interativa por ano: `processar_ano(year_int)` (lógica de leitura/limpeza MUITO similar a `utils.process_inmet_year`) e `load_and_process_by_year()` (loop 2000–2025 com `input()`).

Resumo: hoje existem DOIS caminhos para consolidar INMET pós-extração — o “novo” canônico via `utils.process_inmet_year(s)` (chamado por `inmet_scraper.consolidate_inmet_after_extract`) e o “antigo” via `load_inmet_csv_data.py`. Ambos funcionam, mas duplicam lógica.

---

# Leitura peça-a-peça e observações

## inmet_scraper.py

* Config:

  * `cfg = loadConfig()` e `log = get_logger("inmet.scraper")`: aderente ao core de `utils`.
  * `INMET_BASE_URL`: usa `config.yaml` com fallback padrão — ok.
  * `INMET_RAW_DIR, INMET_CSV_DIR = get_inmet_paths()`: centraliza paths — bom.

* `discover_inmet_zip_links`:

  * Usa `list_zip_links_from_page` (que já normaliza links para absolutos e filtra `.zip`). Boa separação de responsabilidades.
  * Log informativo com total encontrado.

* `download_inmet_archives`:

  * Deriva `fname` de `url` com strip de query string – prático.
  * Skip se arquivo já existe. Download com `stream_download` e logs; captura exceptions e só registra erro — tolerante a falhas de um arquivo sem parar o lote.

* `extract_inmet_archives`:

  * `unzip_all_in_dir(..., make_subdir_from_zip=True)`: cria pasta por zip — ajuda a organizar.

* `extract_bdqueimadas_archives`:

  * Simétrico ao INMET, usando `get_bdqueimadas_paths`.

* “Seção 2.5 – Consolidação”:

  * `from utils import process_inmet_years` está no meio do arquivo (funciona, mas estilisticamente costuma ir ao topo).
  * `consolidate_inmet_after_extract(years=None, overwrite=False)`:

    * Pega `years` do `config.yaml` ou infere por subpastas numéricas em `INMET_CSV_DIR`.
    * Chama `process_inmet_years(yrs, overwrite=...)`.
    * Boa automação: roda “sem perguntar nada”.

* `main`:

  * Garante diretórios, roda 4 passos (descobrir→baixar→extrair→consolidar) — pipeline claro e idempotente (skips).

❒ Pontos fortes

* Pipeline claro e estável, com logs e idempotência (skip se já existe).
* Responsabilidades bem divididas (descoberta, download, extração, consolidação).
* Usa `utils` para paths, sessão HTTP, unzip e consolidação.

⚑ Riscos/arestas

* `unzip_all_in_dir` só olha `*.zip` diretamente sob `raw/INMET` (não recursivo). Se algum zip for salvo em subpasta, não será pego.
* `skip_if_exists=True` em `unzip_file`: se uma extração anterior foi parcial (ex.: falha), a existência do diretório destino faz pular. Normalmente ok, mas pode mascarar meia-extração.
* Import localizado no meio do arquivo (não quebra, só estilo).

## utils.py

* Config:

  * Cache de config e raiz — bom para performance.
  * `_resolve_paths` expande strings que **contêm “/”** ou começam com “.”; isso cobre a maioria dos casos, mas se alguém colocar um valor simples sem “/” (ex.: `"raw"`) ele **não** é resolvido relativo à raiz. Geralmente você usa subcaminhos (com “/”), então na prática passa.
  * Criação de diretórios opcional baseada em `io.create_missing_dirs` — útil.

* Logging:

  * Evita handlers duplicados se o logger já tiver handlers — ótimo.
  * Suporte a arquivo rotativo com limites configuráveis — profissional.

* HTTP/scraping:

  * `get_requests_session` com `Retry` para GET/POST, `status_forcelist` correto, `backoff_factor` — robusto.
  * `_soup` tenta `lxml` e faz fallback — ótimo.
  * `list_zip_links_from_page`:

    * Filtra por `.zip` case-insensitive (`lower().endswith(".zip")`), normaliza com `urljoin`, `sorted(set(...))` — limpo.

* Download:

  * `stream_download` com `.part` e replace atômico; mostra progresso em `DEBUG` quando há `Content-Length`. Excelente prática.

* ZIP:

  * `unzip_file`: skip se destino existe (com a ressalva acima), trata `BadZipFile` com log/raise.
  * `unzip_all_in_dir`: itera sobre `*.zip` (não recursivo), extrai em `<extract_root>/<nome_zip>` se `make_subdir_from_zip=True`.

* INMET (consolidação):

  * `_INMET_DROP_COLS`: lista centralizada — bom.
  * `_inmet_year_dir`: tenta `<csv>/<year>/<year>`, depois `<csv>/<year>`, e por fim busca profunda por pasta chamada `<year>` — resiliente a variações de extração.
  * `_parse_inmet_header`: lê linhas 3/5/6 para cidade/lat/lon e linha 9 (idx 8) para header; tolerante a arquivo curto; `errors="ignore"` — resiliente.
  * `process_inmet_year`:

    * Usa `_inmet_year_dir` + coleta `*.CSV` e `*.csv`.
    * Lê com `sep=";"`, `skiprows=9`, `latin1`, `engine="python"`, `on_bad_lines="skip"`.
    * Ajusta desalinhamento entre header e dados (cria `COLUNA_EXTRA_n` ou “pulando” quando há menos colunas de dados que de header).
    * Anexa metadados (`ANO`, `CIDADE`, `LATITUDE`, `LONGITUDE`), remove colunas de `_INMET_DROP_COLS`, remove `COLUNA_EXTRA*`.
    * Concatena tudo e grava `processed/INMET/inmet_{year}.csv` (ou padrão do `config`).
  * `process_inmet_years`: simples loop.

❒ Pontos fortes

* Núcleo bem desenhado, coeso e reaproveitável.
* “Tolerância a ruído” dos CSVs do INMET (desalinhamento, linhas ruins, encodings).
* Logs granulares em pontos críticos.

⚑ Riscos/arestas

* Performance: `engine="python"` + `on_bad_lines="skip"` é robusto, mas costuma ser mais lento. Não necessariamente um problema, mas é um trade-off.
* Memória: concat final em memória; ok para volumes normais, mas pode pesar para anos com muitas estações/linhas.
* `_resolve_paths`: strings simples sem “/” não são resolvidas; só atenção ao estilo do `config.yaml`.
* `_find_project_root` assume `utils.py` em `<root>/src/` — se mover a estrutura, quebra a detecção.

## load_inmet_csv_data.py

* Lê `config.yaml` “na unha” e monta paths com `script_dir` + chaves de `config`.

* Reimplementa praticamente toda a lógica de `utils.process_inmet_year`:

  * Lê header na linha 9.
  * Extrai cidade/lat/lon de linhas 3/5/6.
  * Lê CSV com os mesmos parâmetros (`latin1`, `;`, `skiprows=9`, `engine="python"`, `on_bad_lines='skip'`).
  * Resolve desalinhamento gerando `COLUNA_EXTRA_n`.
  * Adiciona metadados (ANO/CIDADE/LATITUDE/LONGITUDE).
  * Remove a MESMA lista de colunas (duplicada aqui).
  * Concatena e salva em `processed/INMET/inmet_{ano}.csv`.

* Diferenças relevantes:

  * Interatividade: pergunta “s/n” por ano no loop 2000–2025. O pipeline atual (via `inmet_scraper`) é não-interativo.
  * Paths: usa `script_dir` + `config["paths"]["data_raw"]` etc. Se o `config` já tiver caminhos absolutos, concatenar com `script_dir` pode produzir caminhos incorretos (potencial armadilha).
  * Define localmente `colunas_remover` (duplicação de `_INMET_DROP_COLS`).

❒ Pontos fortes

* Funciona de forma independente do `utils` (útil historicamente).
* Dá controle manual ano a ano.

⚑ Riscos/arestas

* Redundância com risco de drift: a lógica é um “clone” de `utils.process_inmet_year`, mas agora existem duas fontes de verdade.
* Inconsistência de paths: usa uma política de paths diferente do `utils` (pode conflitar).
* Interatividade quebra automação e idempotência do pipeline principal.

---

# Redundâncias (onde há “duas versões” do mesmo conceito)

1. Consolidação por ano

* `load_inmet_csv_data.processar_ano` ≈ `utils.process_inmet_year` (quase a mesma função com nomes/caminhos diferentes).

2. Lista de colunas a descartar

* `colunas_remover` em `load_inmet_csv_data.py` ≈ `_INMET_DROP_COLS` em `utils.py`.

3. Leitura de `config.yaml` e montagem de caminhos

* `load_inmet_csv_data.py` tem sua própria leitura (com `yaml` puro) e estratégia de paths, enquanto o resto do projeto usa `utils.loadConfig` + `get_path` + helpers (`get_inmet_paths`, etc.).

4. Descoberta de header/linhas de metadados

* Código duplicado: `_parse_inmet_header` em `utils` e leitura manual no script antigo.

Reflexão (manter ou não?):

* Faz sentido convergir tudo para o “core” em `utils` e deixar `load_inmet_csv_data.py` como um **wrapper fino** (ou descontinuado) para evitar drift. Hoje, mantendo “duas fontes” você precisa lembrar de atualizar as duas sempre que o INMET mudar algo.

---

# Oportunidades de otimização (apenas reflexão, prós/contras)

1. Unificação do caminho “pós-extração”

* Ideia: adotar **apenas** `utils.process_inmet_year(s)` como fonte canônica de consolidação; se quiser interface “manual por ano”, criar um pequeno CLI que **chame** `utils.process_inmet_year(ano)` em vez de duplicar a lógica.
* Por quê: elimina redundância (itens 1–4 acima), reduz chance de divergência e erros de caminho.

2. Estratégia de paths (consistência)

* Ideia: usar sempre `utils.loadConfig` + `get_path`/helpers.
* Risco atual: `load_inmet_csv_data.py` soma `script_dir` a valores de `config`. Se `config` trouxer absolutos, isso vira caminho inválido.
* Benefício: “uma única verdade” para paths e criação de diretórios.

3. Performance de leitura CSV

* Hoje: `engine="python"` + `on_bad_lines="skip"` (robustez > velocidade).
* Possível: tentar `engine="c"` quando possível (mais rápido) e só cair para `python` quando detectar problema; ou usar `usecols` para já omitir as colunas a descartar — se o header estiver limpo.
* Trade-off: complexifica o código/ramificações; pode não valer se os tempos atuais já são aceitáveis.

4. Memória na consolidação

* Hoje: acumula `dfs` numa lista e faz `pd.concat` no final.
* Alternativas: “streamar” para disco (append em CSV ou `pyarrow`/parquet), ou concatenar em blocos.
* Quando vale: anos muito grandes (muitas estações/linhas). Se não enfrenta OOM, manter simples é melhor.

5. Detecção de partial-unzip

* Hoje: `skip_if_exists=True` evita reprocessar, mas também “confia” que a extração passada foi completa.
* Possível: gravar um “.done” marker após extração completa ou checar contagem de arquivos vs `ZipFile.namelist()`.
* Trade-off: mais I/O e estado, ganha robustez em cenários de falha no meio da extração.

6. Descoberta de zips a extrair

* `unzip_all_in_dir` usa `zip_root.glob("*.zip")` (não recursivo).
* Se você eventualmente organizar subpastas sob `raw/INMET`, considerar uma varredura recursiva — só se esse caso realmente aparecer.

7. Paralelismo controlado

* Downloads (e até unzip) poderiam rodar com um pool de threads/processos para reduzir tempo total.
* Trade-off: aumenta complexidade (limites de cortesia do servidor, logs intercalados, etc.). Avaliar “deve” vs “pode”.

8. Tipos/dtypes e normalização precoce

* Se no consumo posterior você já sabe dtypes (datas, numéricos), declarar `dtype`/`parse_dates` pode evitar custo posterior e inconsistências.
* Trade-off: exige lidar com inconsistências das estações (valores fora de padrão).

9. `_resolve_paths` e valores simples

* Se alguém colocar `paths.data_raw: raw` (sem “/”), hoje não será resolvido relativo à raiz.
* Pode ficar como está (disciplina no `config`) ou ampliar a heurística.
* Trade-off: mudar heurística pode ter efeitos colaterais; talvez só documentar a “regra do slash”.

10. Logs

* Você já tem bons logs. Se quiser métricas de throughput (MB/s por download, tempo por ano, nº de linhas por ano), dá para enriquecer sem mudar a lógica. Só vale se isso ajudar sua monitoração.

---

# “Deve” x “Pode” (priorização sugerida)

* **Deve (alto valor, baixo risco)**

  * Eliminar a duplicação funcional entre `load_inmet_csv_data.py` e `utils.process_inmet_year(s)` (manter apenas o caminho canônico em `utils` e transformar o script antigo num wrapper fino ou aposentar).
  * Unificar a fonte da lista de colunas a remover (usar `_INMET_DROP_COLS` como verdade única).
  * Padronizar leitura de config/paths via `utils.loadConfig`/helpers para todo o projeto.

* **Pode (valor situacional)**

  * Marcação “.done” na extração para detectar partial-unzip.
  * Varredura recursiva de zips, se o layout exigir.
  * Otimizações de performance (engine “c”, `usecols`, dtypes, escrita incremental) se o volume/tempo atual estiverem incômodos.
  * Paralelismo controlado (se o gargalo for significativo e o servidor permitir).

---

# Pequenos detalhes/caveats notáveis

* O import tardio `from utils import process_inmet_years` dentro do `inmet_scraper.py` funciona, mas estilisticamente costuma ir ao topo (sem urgência).
* `_inmet_year_dir` é bem resiliente a variações de profundidade de pasta — bom, dado que zips diferentes podem extrair com layouts levemente distintos.
* `on_bad_lines='skip'` é uma salvaguarda importante para dados ruidosos do INMET; qualquer otimização deve preservá-la (ou substituí-la por validação equivalente).
* `list_zip_links_from_page` pega **apenas** `.zip` — se o INMET mudar para `.7z`/`.rar`/links via JS, seria necessário ajustar (só para ficar no radar).
* `load_inmet_csv_data.py` exige `input()` — perfeito para uso manual, mas foge da automação idempotente do pipeline novo.

---

