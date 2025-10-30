# INMET Scraper: análise técnica aprofundada

Arquivo: `src/inmet_scraper.py`
Função: baixar zips históricos do INMET, extrair CSVs e consolidar por ano.
Convencões: trechos citados mantêm numeração e conteúdo originais.

---

## Sumário

* [1. Modelagem do problema e hipóteses](#1-modelagem-do-problema-e-hipóteses)
* [2. Configuração e inicialização](#2-configuração-e-inicialização)
* [3. Descoberta de links de dados](#3-descoberta-de-links-de-dados)
* [4. Download com idempotência e atomicidade](#4-download-com-idempotência-e-atomicidade)
* [5. Extração com isolamento por pacote](#5-extração-com-isolamento-por-pacote)
* [6. Consolidação pós extração](#6-consolidação-pós-extracao)
* [7. Orquestração do pipeline](#7-orquestração-do-pipeline)
* [8. Propriedades formais: determinismo, idempotência e complexidade](#8-propriedades-formais-determinismo-idempotência-e-complexidade)
* [9. Observabilidade e manejo de falhas](#9-observabilidade-e-manejo-de-falhas)
* [10. Considerações de desempenho e I/O](#10-considerações-de-desempenho-e-io)
* [11. Segurança e integridade de dados](#11-segurança-e-integridade-de-dados)
* [12. Extensões recomendadas](#12-extensões-recomendadas)

---

## 1. Modelagem do problema e hipóteses

O módulo implementa um pipeline ETL offline, orientado a arquivos, com três transformações principais: descoberta de fontes, aquisição robusta e materialização de CSVs ano a ano. Supõe-se: página de índice do INMET listando zips públicos, rede confiável com falhas transitórias, e formato estável de zips contendo CSVs estruturados por ano.

---

## 2. Configuração e inicialização

Trechos relevantes:

```python
27  cfg = loadConfig()
28  log = get_logger("inmet.scraper", kind="scraper", per_run_file=True)
31  INMET_BASE_URL = (
32      cfg.get("inmet", {}).get("base_url")
33      or "https://portal.inmet.gov.br/dadoshistoricos"
34  )
37  INMET_RAW_DIR, INMET_CSV_DIR = get_inmet_paths()
```

Leitura técnica:

* Linhas 27 e 28: inicializam contexto determinístico de execução. `loadConfig()` resolve caminhos absolutos e, se configurado, cria diretórios. `get_logger(..., per_run_file=True)` garante trilha temporal por execução, útil para auditoria e depuração.
* Linhas 31 a 34: seleção da fonte primária com fallback estático assegura continuidade operacional quando o `config.yaml` não define `inmet.base_url`.
* Linha 37: extração de diretórios canônicos para INMET via `utils` promove acoplamento fraco ao layout de paths.

Invariante: todos os caminhos manipulados devem ser absolutos a partir da raiz do projeto, preservando reprodutibilidade entre ambientes.

---

## 3. Descoberta de links de dados

```python
42  def discover_inmet_zip_links(base_url: str = INMET_BASE_URL) -> list[str]:
46      session = get_requests_session()
47      links = list_zip_links_from_page(base_url, session=session)
48      log.info(f"{len(links)} arquivos .zip encontrados em {base_url}")
49      return links
```

Análise algorítmica:

* `get_requests_session()` deve incorporar política de retry com backoff e timeouts conservadores, reduzindo P(falha transitória).
* `list_zip_links_from_page` encapsula o parsing do HTML. Complexidade temporal O(S) no tamanho do HTML. Complexidade espacial O(L) no número de links encontrados.
* Propriedade de contrato: retorna apenas URLs absolutas de zips. Mudanças no DOM da página devem ser absorvidas internamente a esta função, mantendo a assinatura e reduzindo difusão de mudanças.

Risco controlado: acoplamento estrutural ao HTML da origem. Mitigação recomendada na Seção 12.

---

## 4. Download com idempotência e atomicidade

```python
51  def download_inmet_archives(links: list[str]) -> None:
55      session = get_requests_session()
57      fname = os.path.basename(url.split("?")[0])
58      dest = Path(INMET_RAW_DIR) / fname
59      if dest.exists():
60          log.info(f"[SKIP] {fname} já existe em {dest}")
61          continue
64      stream_download(url, dest, session=session, log=log)
66      log.error(f"[ERROR] Falha ao baixar {fname}: {e}")
```

Propriedades:

* Idempotência: o teste em 59 a 61 evita retransferência, reduzindo custo de rede e risco de inconsistência.
* Atomicidade esperada: `stream_download` deve gravar em arquivo temporário e realizar rename atômico ao final. Isso evita artefatos truncados se houver falhas no meio da escrita.
* Complexidade: O(T) no total de bytes transferidos. Latência dominada por RTT e largura de banda. Paralelização é possível, mas fora do escopo atual.

Fail-fast por item: a exceção é capturada por arquivo, mantendo o progresso global. A métrica de sucesso parcial fica registrada em log.

---

## 5. Extração com isolamento por pacote

```python
68  def extract_inmet_archives() -> None:
72      unzip_all_in_dir(INMET_RAW_DIR, INMET_CSV_DIR, make_subdir_from_zip=True, log=log)
```

Racional:

* `make_subdir_from_zip=True` impõe partição por pacote, eliminando colisões de nome e preservando rastreabilidade. Essa decisão é equivalente a impor uma função de hashing consistente do nome do zip para um subdiretório, mas mantendo legibilidade humana.
* Complexidade: O(U) no total de bytes descompactados. I/O bound. Vantajoso direcionar para disco local com throughput adequado.

---

## 6. Consolidação pós extração

```python
85  from utils import process_inmet_years
87  def consolidate_inmet_after_extract(years: list[int] | None = None, overwrite: bool = False) -> None:
92      yrs = years or cfg.get("inmet", {}).get("years")
95      yrs = sorted({int(p.name) for p in INMET_CSV_DIR.iterdir() if p.is_dir() and p.name.isdigit()})
97          log.warning("[WARN] Não foi possível inferir anos a partir de INMET/csv.")
100     process_inmet_years(yrs, overwrite=overwrite)
```

Comportamento:

* Seleção de anos segue precedência formal: parâmetro explícito, configuração, inferência por diretórios numéricos. Essa hierarquia maximiza reprodutibilidade quando desejada e adaptabilidade quando a fonte evolui.
* `process_inmet_years` é um mapeamento de alto nível Ano → `processed/INMET/inmet_{YYYY}.csv`, idealmente streaming e tolerante a CSVs volumosos. Espera-se que preserve schema e normalizações acordadas (ex. datas).

Complexidade: O(Ny * Ry), onde Ny é número de anos e Ry é número de linhas por ano. Deve operar com memória O(1) em relação ao tamanho do dataset, via leitura linha a linha ou chunked.

---

## 7. Orquestração do pipeline

```python
105 def main() -> None:
106     ensure_dir(INMET_RAW_DIR)
107     ensure_dir(INMET_CSV_DIR)
109     links = discover_inmet_zip_links()
110     download_inmet_archives(links)
111     extract_inmet_archives()
114     consolidate_inmet_after_extract(overwrite=False)

119 if __name__ == "__main__":
120     main()
```

Semântica:

* Preparação determinística de diretórios (106, 107) estabelece pré-condições do ETL.
* Sequência de operações impõe ordem parcial adequada: descoberta precede download, que precede extração, que precede consolidação. Essa DAG lineariza dependências e previne condições de corrida.
* `overwrite=False` na consolidação preserva artefatos existentes, reforçando idempotência global.

---

## 8. Propriedades formais: determinismo, idempotência e complexidade

* Determinismo: para uma versão fixada da página fonte e conjunto de zips, a sequência de artefatos gerados é determinística. Variabilidade temporal surge apenas de ordens diferentes na iteração dos links, sem afetar resultado final.
* Idempotência: download e extração são idempotentes por construção; consolidação deve ser idempotente quando parametrizada com `overwrite=False` e seleção de anos estável.
* Complexidade total: O(S + T + U + Ny*Ry), dominada por rede e I/O de disco. Memória deve permanecer O(1) ao operar streaming nas fases intensivas.

---

## 9. Observabilidade e manejo de falhas

Pontos de medição:

```python
48  log.info(f"{len(links)} arquivos .zip encontrados em {base_url}")
60  log.info(f"[SKIP] {fname} já existe em {dest}")
64  stream_download(..., log=log)
66  log.error(f"[ERROR] Falha ao baixar {fname}: {e}")
97  log.warning("[WARN] Não foi possível inferir anos a partir de INMET/csv.")
99  log.info(f"[CONSOLIDATE] Anos: {yrs}")
```

Diretrizes:

* Registros de cardinalidade e eventos de skip quantificam ganhos de idempotência.
* Erros por item não abortam o job inteiro, mas devem ser contabilizados para alçadas de retry ou intervenção.
* Recomendável padronizar contadores no logger (ex. total, sucesso, falha) e granularidade de níveis (info, warn, error).

---

## 10. Considerações de desempenho e I/O

* Download: throughput limitado pela banda; sugerem-se conexões paralelas com janela de 3 a 6 workers, respeitando limites de cortesia do servidor.
* Extração: gargalo de disco. Usar SSD local, desabilitar antivírus em diretórios temporários se apropriado.
* Consolidação: ler e escrever em blocos grandes (buffered I/O), preservar CSVs sem reparse desnecessário quando possível.

---

## 11. Segurança e integridade de dados

* Integridade: `stream_download` deve validar tamanho ou hash quando metadados estiverem disponíveis. Na ausência, validação parcial por cabeçalhos ZIP e tentativa de listagem após download.
* Segurança de caminho: sanitizar `fname` derivado da URL evita path traversal. O split usado em 57 combinado com `os.path.basename` mitiga cenários triviais.
* Resiliência a HTML hostil: `list_zip_links_from_page` não deve executar conteúdo ativo, apenas parsing estático.

---

## 12. Extensões recomendadas

* CLI granular: flags `--skip-download`, `--skip-extract`, `--only-consolidate`, `--years 2000 2001`.
* Persistência de índice: salvar snapshot dos links com timestamp e diff na rodada seguinte.
* Paralelização controlada: `concurrent.futures.ThreadPoolExecutor` para downloads, com retry per task.
* Verificação de integridade: armazenar checksums após download e revalidar antes de consolidar.
* Telemetria leve: sumarizar em JSON por execução o conjunto de arquivos baixados, extraídos e anos consolidados.

---