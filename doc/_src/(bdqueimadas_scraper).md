# BDQueimadas Scraper: análise técnica aprofundada

Arquivo: `src/bdqueimadas_scraper.py`
Função: descobrir, baixar e extrair os zips anuais `focos_br_ref_YYYY.zip` do COIDS INPE, particionando saídas por pacote.
Convenções: trechos citados mantêm numeração e conteúdo preservados para referência.

---

## Sumário

* [1. Modelagem do problema e hipóteses](#1-modelagem-do-problema-e-hipóteses)
* [2. Configuração e inicialização](#2-configuração-e-inicialização)
* [3. Descoberta de links de dados](#3-descoberta-de-links-de-dados)
* [4. Filtragem por ano](#4-filtragem-por-ano)
* [5. Download com idempotência e atomicidade](#5-download-com-idempotência-e-atomicidade)
* [6. Extração com isolamento por pacote](#6-extração-com-isolamento-por-pacote)
* [7. Layout de diretórios e desacoplamento de paths](#7-layout-de-diretórios-e-desacoplamento-de-paths)
* [8. Orquestração e CLI](#8-orquestração-e-cli)
* [9. Propriedades formais e complexidade](#9-propriedades-formais-e-complexidade)
* [10. Observabilidade e manejo de falhas](#10-observabilidade-e-manejo-de-falhas)
* [11. Considerações de desempenho e I O](#11-considerações-de-desempenho-e-i-o)
* [12. Segurança, integridade e conformidade](#12-segurança-integridade-e-conformidade)
* [13. Extensões e melhorias recomendadas](#13-extensões-e-melhorias-recomendadas)

---

## 1. Modelagem do problema e hipóteses

O módulo implementa um ETL offline, orientado a arquivos, para a coleção anual Brasil sat ref do BDQueimadas. O fluxo é linear: descoberta de URLs na página índice, filtro opcional por anos, download com política idempotente, e extração para uma área processada sob particionamento por zip. Suposições: a listagem HTML contém âncoras para `focos_br_ref_YYYY.zip`, as respostas HTTP são estáveis com eventuais falhas transitórias, e os zips embalam CSVs padronizados.

---

## 2. Configuração e inicialização

```python
01  from utils import loadConfig, get_logger, get_path, ensure_dir, ...
05  cfg = loadConfig()
06  log = get_logger("bdqueimadas.scraper", kind="scraper", per_run_file=True)
09  BDQ_BASE_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/"
11  DEFAULT_FOLDER = "ID_BDQUEIMADAS"
```

Leitura técnica:

* Linhas 05 e 06 configuram contexto determinístico e trilha de auditoria por execução. O logger com `per_run_file=True` preserva cronologia do job.
* Linha 09 fixa a origem canônica com barra final para composição segura via `urljoin`.
* Linha 11 define convenção de pasta default sob `data/raw` e `data/processed`, útil para repetibilidade.

---

## 3. Descoberta de links de dados

```python
20  def discover_bdq_zip_links(base_url: str = BDQ_BASE_URL) -> List[str]:
22      session = get_requests_session()
23      links = list_zip_links_from_page(base_url, session=session)
26      out = []
27      for h in links:
28          abs_url = urljoin(base_url, h)
29          name = os.path.basename(abs_url.split("?")[0]).lower()
30          if name.endswith(".zip") and name.startswith("focos_br_ref_"):
31              out.append(abs_url)
32      out = sorted(set(out))
33      log.info(f"{len(out)} .zip detectados em {base_url}")
34      return out
```

Análise:

* `get_requests_session` deve embutir timeouts e retry com backoff para reduzir falhas intermitentes.
* Filtro duplo por prefixo e sufixo faz saneamento de coleção e evita zips irrelevantes.
* `sorted(set(...))` impõe ordem total e remove duplicatas, reduzindo ruído a jusante.
* Complexidade O(S) no tamanho do HTML para parsing e O(L log L) para ordenar L links.

Risco controlado: mudanças no DOM da página podem quebrar `list_zip_links_from_page`. Encapsular o parser permite adaptação local sem alterar as chamadas externas.

---

## 4. Filtragem por ano

```python
37  def filter_links_by_year(links: Iterable[str], years: Optional[Iterable[int]]) -> List[str]:
39      if not years:
40          return list(links)
41      wanted = {str(int(y)) for y in years}
43      out = []
44      for url in links:
45          fname = os.path.basename(url.split("?")[0]).lower()
47          try:
48              stem = fname.rsplit(".", 1)[0]
49              year = stem.split("_")[-1]
50          except Exception:
51              continue
52          if year in wanted:
53              out.append(url)
54      return sorted(out)
```

Leitura técnica:

* O recorte por ano é puramente sintático sobre o nome do arquivo. Não há dependência de metadados externos.
* O bloco `try except` absorve nomes atípicos sem abortar o job.
* Ordenação final garante determinismo na sequência de downloads.

---

## 5. Download com idempotência e atomicidade

```python
58  def download_bdq_archives(folder_name: str = DEFAULT_FOLDER, years: Optional[Iterable[int]] = None, overwrite: bool = False) -> List[Path]:
63      target_dir = get_target_raw_dir(folder_name)
64      session = get_requests_session()
66      all_links = discover_bdq_zip_links(BDQ_BASE_URL)
67      sel_links = filter_links_by_year(all_links, years)
70      if not sel_links:
71          log.warning("[WARN] Nenhum link selecionado para download.")
72          return []
74      downloaded: List[Path] = []
75      for url in sel_links:
76          fname = os.path.basename(url.split("?")[0])
77          dest = target_dir / fname
78          if dest.exists() and not overwrite:
79              log.info(f"[SKIP] {fname} já existe.")
80              downloaded.append(dest)
81              continue
83          try:
84              log.info(f"[DOWNLOADING] {fname}")
85              stream_download(url, dest, session=session, log=log)
86              downloaded.append(dest)
87          except Exception as e:
88              log.error(f"[ERROR] Falha ao baixar {fname}: {e}")
89      return downloaded
```

Propriedades:

* Idempotência: o teste de existência do artefato evita retransferência. A flag `overwrite` permite forçar revalidação quando necessário.
* Atomicidade esperada: `stream_download` deve escrever em arquivo temporário e renomear ao final para impedir arquivos truncados. Recomendação explícita na seção 12 para garantir esse contrato.
* Granularidade de falha por item: exceções são capturadas por arquivo mantendo progresso parcial e evidência em log.
* Complexidade O(T) no volume total transferido. Latência dominada por rede.

---

## 6. Extração com isolamento por pacote

```python
92  def extract_downloaded_archives(folder_name: str = DEFAULT_FOLDER) -> Path:
95      source_zip_dir = get_target_raw_dir(folder_name)
97      out_processed_dir = get_target_processed_dir(folder_name)
99      unzip_all_in_dir(source_zip_dir, out_processed_dir, make_subdir_from_zip=True, log=log)
100     return out_processed_dir
```

Racional:

* `make_subdir_from_zip=True` impõe particionamento 1 para 1 por pacote, prevenindo colisões de nomes e preservando rastreabilidade entre zip e CSVs produzidos.
* Complexidade O(U) no total descompactado. Operação I O bound.

---

## 7. Layout de diretórios e desacoplamento de paths

```python
14  def get_target_processed_dir(folder_name: str = DEFAULT_FOLDER) -> Path:
16      base = get_path("paths", "data", "processed")
17      target = Path(base) / (folder_name or DEFAULT_FOLDER)
18      return ensure_dir(target)

28  def get_target_raw_dir(folder_name: str = DEFAULT_FOLDER) -> Path:
30      raw_root = get_path("paths", "data", "raw")
31      target = Path(raw_root) / (folder_name or DEFAULT_FOLDER)
32      return ensure_dir(target)
```

Diretrizes:

* O módulo desacopla os diretórios efetivos do `config.yaml` via `get_path`, mas mantém uma convenção explícita de subpasta por `folder_name`.
* `ensure_dir` materializa precondições de existência em tempo de execução e reduz erros de E N O E N T.

Invariante: todos os caminhos devem ser absolutos a partir da raiz do projeto para garantir reprodutibilidade entre ambientes.

---

## 8. Orquestração e CLI

```python
104 if __name__ == "__main__":
106     p = argparse.ArgumentParser(...)
111     p.add_argument("--folder", required=False, default=DEFAULT_FOLDER, ...)
116     p.add_argument("--years", nargs="*", type=int, default=None, ...)
121     p.add_argument("--overwrite", action="store_true", ...)
125     p.add_argument("--no-extract", action="store_true", ...)
130     args = p.parse_args()
133     folder = args.folder or DEFAULT_FOLDER
134     log.info(f"[TARGET] data/raw/{folder}")
135     paths = download_bdq_archives(folder_name=folder, years=args.years, overwrite=args.overwrite)
136     log.info(f"[OK] {len(paths)} arquivo(s) disponíveis em data/raw/{folder}")
138     if not args.no_extract:
139         out_dir = extract_downloaded_archives(folder)
140         log.info(f"[EXTRACTED] Arquivos extraídos em: {out_dir}")
```

Semântica:

* A CLI expõe os pontos de variação relevantes: filtro temporal, política de overwrite e controle de extração.
* A ordem operacional é linear e segura: download precede extração.
* O padrão `--no-extract` permite staging apenas dos zips em ambientes com recursos restritos.

---

## 9. Propriedades formais e complexidade

* Determinismo: dada uma página índice fixa, a lista final de URLs é determinística pós ordenação. A sequência de artefatos locais é reproduzível.
* Idempotência: garantida no download quando `overwrite=False`. A extração é idempotente sob `make_subdir_from_zip=True` desde que a função de descompactação seja sobregravável de forma segura.
* Complexidade total: O(S + L log L + T + U), onde S é tamanho do HTML, L a contagem de links, T os bytes transferidos e U os bytes descompactados. Memória deve permanecer O(1) ao operar stream de rede e descompressão incremental.

---

## 10. Observabilidade e manejo de falhas

Pontos de medição destacados:

```python
33  log.info(f"{len(out)} .zip detectados em {base_url}")
71  log.warning("[WARN] Nenhum link selecionado para download.")
79  log.info(f"[SKIP] {fname} já existe.")
84  log.info(f"[DOWNLOADING] {fname}")
88  log.error(f"[ERROR] Falha ao baixar {fname}: {e}")
139 log.info(f"[EXTRACTED] Arquivos extraídos em: {out_dir}")
```

Boas práticas:

* Contabilizar sucessos, skips e erros por execução com totais agregados no rodapé do log.
* Emitir métricas de latência por arquivo e throughput médio por fase para orientar ajustes de paralelismo.
* Elevar para `warning` quando a seleção de anos resultar vazia por conflito de filtro.

---

## 11. Considerações de desempenho e I O

* Rede: aplicar janela de conexões concorrentes moderada no download, respeitando limites de cortesia do provedor. Sugestão prática futura em seção 13.
* Disco: direcionar extração para SSD local. Evitar antivírus em tempo real no diretório de staging quando apropriado e autorizado.
* Bufferização: `stream_download` deve utilizar chunks grandes e verificação de progresso para melhor throughput.
* Multiprocessamento: não necessário para volumes médios. Em volumes altos, considerar pipeline produtor consumidor com fila limitada.

---

## 12. Segurança, integridade e conformidade

* Sanitização de nomes: uso de `os.path.basename` mitiga path traversal. Recomendado validar o padrão `focos_br_ref_YYYY.zip` antes de gravar.
* Atomicidade: `stream_download` deve escrever em arquivo temporário no mesmo filesystem e realizar `rename` ao final. Em caso de exceção, remover o temporário.
* Verificação de integridade: quando possível, validar o zip via listagem pós download. Opcionalmente calcular checksum e persistir manifesto por execução.
* Resiliência HTTP: configurar timeouts prudentes e limitar redirecionamentos. Retentar somente métodos idempotentes.
* Conformidade com origem: limitar concorrência e user agent identificável quando aplicável a políticas do servidor.

---

## 13. Extensões e melhorias recomendadas

1. CLI granular adicional
   Adicionar flags `--skip-download`, `--only-extract`, `--list-years` para inspeção rápida da página índice.

2. Paralelização controlada
   Introduzir `ThreadPoolExecutor(max_workers=k)` no loop de downloads com fila limitada e retry per task com jitter.

3. Persistência de índice
   Salvar snapshot em JSON com timestamp contendo `url`, `fname`, `year`, `status` e `sha256` quando calculado. Gerar diff entre execuções.

4. Telemetria leve
   Exportar métricas `n_links_detected`, `n_downloaded`, `n_skipped`, `n_failed`, `elapsed_seconds` por fase.

5. Validação de esquema
   Após extração, varrer CSVs e validar colunas mínimas esperadas. Emitir relatório por ano.

6. Tolerância a mudanças de DOM
   Tornar `list_zip_links_from_page` polimórfica com detectores de seletores alternativos e fallback por heurística.

7. Integração com consolidação a jusante
   Encadear etapa opcional `consolidate_bdqueimadas_years(years, overwrite=False)` para produzir artefatos em `data/processed` padronizados por ano.

---

### Anexo A. Exemplos de uso

Baixar todos os anos para a pasta default e extrair:

```bash
python -m src.bdqueimadas_scraper
```

Baixar apenas 2019 e 2020, sem extração:

```bash
python -m src.bdqueimadas_scraper --years 2019 2020 --no-extract
```

Forçar rebaixar arquivos existentes e extrair:

```bash
python -m src.bdqueimadas_scraper --overwrite
```
