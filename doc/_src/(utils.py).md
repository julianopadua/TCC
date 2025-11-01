# Utils: análise técnica aprofundada

Arquivo: `src/utils.py`
Função: utilitários transversais do projeto TCC para configuração, logging, filesystem, HTTP, parsing de HTML, download com streaming, extração ZIP e rotinas de leitura e consolidação do INMET.
Convenções: sempre que um trecho de código for citado, ele virá com numeração local de linhas. As referências textuais usam esses números.

---

## Sumário

* [1. Modelagem do problema e hipóteses](#1-modelagem-do-problema-e-hipóteses)
* [2. Configuração, raiz do projeto e resolução de caminhos](#2-configuração-raiz-do-projeto-e-resolução-de-caminhos)
* [3. Logging estruturado e rotação de arquivos](#3-logging-estruturado-e-rotação-de-arquivos)
* [4. Filesystem helpers](#4-filesystem-helpers)
* [5. HTTP resiliente, parsing HTML e download atômico](#5-http-resiliente-parsing-html-e-download-atômico)
* [6. Extração ZIP e varredura em lote](#6-extração-zip-e-varredura-em-lote)
* [7. Helpers de provedores e layout de paths](#7-helpers-de-provedores-e-layout-de-paths)
* [8. INMET: detecção de pastas, parsing de cabeçalhos e consolidação anual](#8-inmet-detecção-de-pastas-parsing-de-cabeçalhos-e-consolidação-anual)
* [9. Main de teste e orquestração mínima](#9-main-de-teste-e-orquestração-mínima)
* [10. Propriedades formais e complexidade](#10-propriedades-formais-e-complexidade)
* [11. Observabilidade e tratamento de falhas](#11-observabilidade-e-tratamento-de-falhas)
* [12. Considerações de desempenho e I/O](#12-considerações-de-desempenho-e-io)
* [13. Segurança, integridade e invariantes](#13-segurança-integridade-e-invariantes)
* [14. Extensões recomendadas](#14-extensões-recomendadas)

---

## 1. Modelagem do problema e hipóteses

O módulo agrega utilitários fundamentais para um pipeline ETL offline baseado em arquivos. Ele centraliza: resolução determinística de caminhos a partir da raiz do projeto, carregamento de configurações com expansão de variáveis, criação de diretórios idempotente, logger com rotação e arquivo por execução, sessão HTTP com retries exponenciais, parsing HTML robusto, download com gravação temporária e rename atômico, extração ZIP segura e, para o provedor INMET, detecção tolerante de pastas por ano e consolidação de CSVs com saneamento de cabeçalhos.

Hipóteses: existência de `config.yaml` na raiz ou definida por `PROJECT_CONFIG`, estrutura de diretórios coerente com `paths` no config, e dados do INMET em CSV latin1 com cabeçalhos posicionais.

---

## 2. Configuração, raiz do projeto e resolução de caminhos

### Descoberta da raiz e caches

```python
1  _CONFIG_CACHE: Dict[str, Any] | None = None
2  _ROOT_CACHE: Path | None = None

3  def _find_project_root() -> Path:
4      return Path(__file__).resolve().parents[1]
```

Leitura: caches globais (1 a 2) evitam reprocessos; a raiz é inferida supondo `src/utils.py` em `<root>/src` (3 a 4).

### Expansão e resolução recursiva de paths

```python
1  def _expand_path(value: str, base: Path) -> str:
2      expanded = os.path.expanduser(os.path.expandvars(value))
3      p = Path(expanded)
4      if not p.is_absolute():
5          p = base / p
6      return str(p.resolve())
```

Semântica: expande `~` e variáveis de ambiente, torna absoluto relativo a `base` e normaliza via `resolve()` (1 a 6).

```python
1  def _resolve_paths(node: Any, base: Path) -> Any:
2      if isinstance(node, dict):
3          return {k: _resolve_paths(v, base) for k, v in node.items()}
4      if isinstance(node, list):
5          return [_resolve_paths(v, base) for v in node]
6      if isinstance(node, str) and ("/" in node or node.startswith(".")):
7          return _expand_path(node, base)
8      return node
```

Semântica: caminha a estrutura e aplica `_expand_path` a strings com cara de caminho (6 a 7), mantendo outros nós intactos.

### Carregamento do config com criação opcional de diretórios

```python
1  def loadConfig(config_path: str | Path | None = None, create_dirs: bool | None = None) -> Dict[str, Any]:
2      global _CONFIG_CACHE, _ROOT_CACHE
3      if _CONFIG_CACHE is not None:
4          return _CONFIG_CACHE
5      _ROOT_CACHE = _find_project_root()
6      cfg_path = Path(config_path) if config_path else None
7      if cfg_path is None:
8          env_cfg = os.getenv("PROJECT_CONFIG", "")
9          cfg_path = Path(env_cfg) if env_cfg else (_ROOT_CACHE / "config.yaml")
10     if not cfg_path.exists():
11         raise FileNotFoundError(f"Config file not found: {cfg_path}")
12     with cfg_path.open("r", encoding="utf-8") as f:
13         cfg = yaml.safe_load(f) or {}
14     cfg.setdefault("paths", {})
15     cfg["paths"]["root"] = str(_ROOT_CACHE)
16     cfg["paths"] = _resolve_paths(cfg.get("paths", {}), _ROOT_CACHE)
17     make_dirs = cfg.get("io", {}).get("create_missing_dirs", False)
18     if create_dirs is not None:
19         make_dirs = create_dirs
20     if make_dirs:
21         _create_all_paths(cfg)
22     _CONFIG_CACHE = cfg
23     return cfg
```

Propriedades: prioridade de fonte de config em 6 a 9; injeção de `paths.root` (15) e normalização recursiva (16). Idempotência via cache (3 a 4). A criação de diretórios é controlada por `io.create_missing_dirs` ou argumento explícito (17 a 21).

### Acesso a chaves aninhadas e criação em massa

```python
1  def get_path(*keys: str) -> Path:
2      cfg = loadConfig()
3      node: Any = cfg
4      for k in keys:
5          node = node[k]
6      return Path(node)
```

Uso: `get_path('paths', 'providers', 'bdqueimadas', 'raw')`.

```python
1  def _create_all_paths(cfg: Dict[str, Any]) -> None:
2      def collect_dirs(node: Any) -> list[Path]:
3          acc: list[Path] = []
4          if isinstance(node, dict):
5              for v in node.values():
6                  acc.extend(collect_dirs(v))
7          elif isinstance(node, list):
8              for v in node:
9                  acc.extend(collect_dirs(v))
10         elif isinstance(node, str):
11             acc.append(Path(node))
12         return acc
13     paths_node = cfg.get("paths", {})
14     dirs = collect_dirs(paths_node)
15     log_file = cfg.get("logging", {}).get("file")
16     if log_file:
17         dirs.append(Path(log_file).parent)
18     for d in dirs:
19         try:
20             Path(d).mkdir(parents=True, exist_ok=True)
21         except Exception as e:
22             print(f"[WARN] Falha ao criar diretório: {d} -> {e}", file=sys.stderr)
```

Observação: cria também o diretório do arquivo de log se configurado (15 a 17).

### Tempo com fuso do projeto e arquivo de log por execução

```python
1  def _now_tz():
2      cfg = loadConfig()
3      tzname = (cfg.get("project", {}) or {}).get("timezone")
4      tz = ZoneInfo(tzname) if (ZoneInfo and tzname) else None
5      return datetime.now(tz) if tz else datetime.now()
```

```python
1  def _build_daily_log_file(kind: str) -> Path:
2      base_logs = get_path("paths", "logs")
3      now = _now_tz()
4      day_dir = ensure_dir(Path(base_logs) / f"log_{now:%Y%m%d}")
5      return day_dir / f"{kind}_{now:%H%M%S}.log"
```

Invariante: se `project.timezone` não existir ou `zoneinfo` indisponível, cai no tempo local.

---

## 3. Logging estruturado e rotação de arquivos

```python
1  def get_logger(name: str = "app", *, kind: str | None = None, per_run_file: bool = False) -> logging.Logger:
2      cfg = loadConfig()
3      log_cfg = cfg.get("logging", {})
4      level_name = log_cfg.get("level", "INFO").upper()
5      level = getattr(logging, level_name, logging.INFO)
6      logger = logging.getLogger(name)
7      if logger.handlers:
8          return logger
9      logger.setLevel(level)
10     fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
11     sh = logging.StreamHandler()
12     sh.setLevel(level)
13     sh.setFormatter(fmt)
14     logger.addHandler(sh)
15     log_file_cfg = (cfg.get("logging", {}) or {}).get("file")
16     if per_run_file and kind:
17         log_path = _build_daily_log_file(kind)
18     else:
19         log_path = Path(log_file_cfg) if log_file_cfg else None
20     if log_path:
21         max_bytes = int((cfg.get("logging", {}) or {}).get("max_bytes", 5_000_000))
22         backup = int((cfg.get("logging", {}) or {}).get("backup_count", 5))
23         fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup, encoding="utf-8")
24         fh.setLevel(level)
25         fh.setFormatter(fmt)
26         logger.addHandler(fh)
27     return logger
```

Decisões: um único logger por `name` é configurado uma vez (6 a 8); saída no console sempre presente; arquivo pode ser rotativo por caminho fixo do config (15, 19 a 26) ou único por execução com prefixo `kind` e carimbo temporal (16 a 18).

---

## 4. Filesystem helpers

```python
1  def ensure_dir(path: Path | str) -> Path:
2      p = Path(path).resolve()
3      p.mkdir(parents=True, exist_ok=True)
4      return p
```

```python
1  def list_files(root: Path | str, patterns: Iterable[str]) -> List[Path]:
2      root = Path(root)
3      out: List[Path] = []
4      for pat in patterns:
5          out.extend(root.rglob(pat))
6      return sorted(set(out))
```

Normalização semântica de chaves textuais para matching robusto:

```python
1  def normalize_key(s: str | None) -> str:
2      if s is None:
3          return ""
4      s = str(s).strip()
5      if not s:
6          return ""
7      s = unicodedata.normalize("NFKD", s)
8      s = "".join(ch for ch in s if not unicodedata.combining(ch))
9      s = s.replace("’", "'").replace("`", "'")
10     s = re.sub(r"[^A-Za-z0-9]+", " ", s)
11     s = s.casefold().strip()
12     s = re.sub(r"\s+", " ", s)
13     return s
```

Efeito: remove diacríticos e símbolos, padroniza aspas, colapsa espaços e faz `casefold`.

---

## 5. HTTP resiliente, parsing HTML e download atômico

### Sessão com retries exponenciais

```python
1  def get_requests_session(retries: int = 3, backoff: float = 0.5):
2      s = requests.Session()
3      retry = Retry(
4          total=retries,
5          connect=retries,
6          read=retries,
7          backoff_factor=backoff,
8          status_forcelist=(429, 500, 502, 503, 504),
9          allowed_methods=frozenset({"GET", "POST"}),
10         raise_on_status=False,
11     )
12     adapter = HTTPAdapter(max_retries=retry)
13     s.mount("http://", adapter)
14     s.mount("https://", adapter)
15     return s
```

Propriedades: reintentos em erros transitórios HTTP e de rede com backoff (3 a 10), aplicados a ambos os esquemas (13 a 14).

### BeautifulSoup com parser preferencial

```python
1  def _soup(html_text: str):
2      try:
3          from bs4 import BeautifulSoup
4      except ImportError as e:
5          raise RuntimeError("beautifulsoup4 não instalado. `pip install beautifulsoup4 lxml`") from e
6      parser = "lxml"
7      try:
8          import lxml  # noqa: F401
9      except Exception:
10         parser = "html.parser"
11     return BeautifulSoup(html_text, parser)
```

Fallback para `html.parser` quando `lxml` não está disponível.

### Descoberta de links .zip em uma página

```python
1  def list_zip_links_from_page(url: str, session=None) -> List[str]:
2      session = session or get_requests_session()
3      resp = session.get(url, timeout=60)
4      resp.raise_for_status()
5      soup = _soup(resp.text)
6      hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
7      hrefs = [h for h in hrefs if h and h.lower().endswith(".zip")]
8      return sorted(set(urljoin(url, h) for h in hrefs))
```

Notas: filtra por sufixo `.zip` e normaliza para URL absoluta com `urljoin` (8).

### Download com streaming e rename atômico

```python
1  def stream_download(url: str, dest: Path | str, session=None, chunk_size: int = 1024 * 256, log: Optional[logging.Logger] = None) -> Path:
2      session = session or get_requests_session()
3      dest = Path(dest)
4      ensure_dir(dest.parent)
5      tmp = dest.with_suffix(dest.suffix + ".part")
6      r = session.get(url, stream=True, timeout=120)
7      r.raise_for_status()
8      total = int(r.headers.get("Content-Length", 0))
9      written = 0
10     with tmp.open("wb") as f:
11         for chunk in r.iter_content(chunk_size=chunk_size):
12             if chunk:
13                 f.write(chunk)
14                 written += len(chunk)
15                 if log and total:
16                     pct = (written / total) * 100
17                     log.debug(f"Baixando {dest.name}: {pct:5.1f}%")
18     tmp.replace(dest)
19     if log:
20         log.info(f"[DOWNLOADED] {dest} ({written/1e6:.2f} MB)")
21     return dest
```

Propriedades: grava em arquivo `.part` e finaliza com `replace` atômico (5, 18). Emite progresso se `Content-Length` disponível (15 a 17).

---

## 6. Extração ZIP e varredura em lote

```python
1  def unzip_file(zip_path: Path | str, out_dir: Path | str, skip_if_exists: bool = True, log: Optional[logging.Logger] = None) -> List[Path]:
2      zip_path = Path(zip_path)
3      out_dir = Path(out_dir)
4      if skip_if_exists and out_dir.exists():
5          if log:
6              log.info(f"[SKIP] {zip_path.name} já extraído em {out_dir}")
7          return list(out_dir.rglob("*"))
8      ensure_dir(out_dir)
9      try:
10         with zipfile.ZipFile(zip_path, "r") as zf:
11             zf.extractall(out_dir)
12         if log:
13             log.info(f"[UNZIP] {zip_path.name} -> {out_dir}")
14     except zipfile.BadZipFile:
15         if log:
16             log.error(f"[ERROR] {zip_path} corrompido ou não é ZIP válido.")
17         raise
18     return list(out_dir.rglob("*"))
```

```python
1  def unzip_all_in_dir(zip_root: Path | str, extract_root: Path | str, make_subdir_from_zip: bool = True, log: Optional[logging.Logger] = None) -> None:
2      zip_root = Path(zip_root)
3      extract_root = Path(extract_root)
4      for z in sorted(zip_root.glob("*.zip")):
5          target = extract_root / z.stem if make_subdir_from_zip else extract_root
6          unzip_file(z, target, skip_if_exists=True, log=log)
```

Decisão: segregação por nome do zip quando `make_subdir_from_zip=True` (5) previne colisões.

---

## 7. Helpers de provedores e layout de paths

```python
1  def get_inmet_paths() -> Tuple[Path, Path]:
2      raw = get_path("paths", "providers", "inmet", "raw")
3      csv_dir = ensure_dir(Path(raw) / "csv")
4      return Path(raw), csv_dir
```

```python
1  def get_bdqueimadas_paths() -> Tuple[Path, Path]:
2      raw = get_path("paths", "providers", "bdqueimadas", "raw")
3      csv_dir = ensure_dir(Path(raw) / "csv")
4      return Path(raw), csv_dir
```

Invariante: `csv` sempre como subpasta de `raw`.

---

## 8. INMET: detecção de pastas, parsing de cabeçalhos e consolidação anual

### Pastas anuais tolerantes a variações

```python
1  def _inmet_year_dir(year: int) -> Optional[Path]:
2      _, csv_root = get_inmet_paths()
3      y = str(year)
4      cand1 = csv_root / y / y
5      cand2 = csv_root / y
6      if cand1.is_dir():
7          return cand1
8      if cand2.is_dir():
9          return cand2
10     for p in csv_root.rglob("*"):
11         if p.is_dir() and p.name == y:
12             return p
13     return None
```

Busca por `<csv>/<ano>/<ano>` ou `<csv>/<ano>` e, como fallback, varredura profunda.

### Parsing do cabeçalho e metadados de estação

```python
1  def _parse_inmet_header(fp: Path):
2      header, cidade, lat, lon = [], None, None, None
3      try:
4          with fp.open("r", encoding="latin1", errors="ignore") as f:
5              lines = f.readlines()
6          if len(lines) > 8:
7              header_line = lines[8].strip()
8              header = [h.strip() for h in header_line.split(";") if h.strip() != ""]
9          if len(lines) > 2:
10             parts = [p.strip() for p in lines[2].split(";")]
11             if len(parts) > 1:
12                 cidade = parts[1] or None
13         if len(lines) > 4:
14             parts = [p.strip() for p in lines[4].split(";")]
15             if len(parts) > 1:
16                 lat = parts[1] or None
17         if len(lines) > 5:
18             parts = [p.strip() for p in lines[5].split(";")]
19             if len(parts) > 1:
20                 lon = parts[1] or None
21     except Exception as e:
22         get_logger("inmet.load", kind="load", per_run_file=True).warning(f"[WARN] Falha lendo header de {fp}: {e}")
23     return header, cidade, lat, lon
```

Decisão: encoding `latin1` com `errors="ignore"` para tolerar bytes inválidos; linha 9 do arquivo original contém o cabeçalho lógico (6 a 8).

### Consolidação anual com saneamento de colunas

```python
1  _INMET_DROP_COLS = [
2      "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)",
3      "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)",
4      "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)",
5      "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)",
6      "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)",
7      "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)",
8      "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)",
9      "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)",
10 ]
```

```python
1  def process_inmet_year(year: int, drop_cols: Optional[list[str]] = None, overwrite: bool = False) -> Optional[Path]:
2      log = get_logger("inmet.load", kind="load", per_run_file=True)
3      drop_cols = drop_cols or _INMET_DROP_COLS
4      year_dir = _inmet_year_dir(year)
5      if not year_dir:
6          log.warning(f"[WARN] Pasta do ano {year} não encontrada em INMET/csv.")
7          return None
8      proc_dir = get_path("paths", "providers", "inmet", "processed")
9      ensure_dir(proc_dir)
10     cfg = loadConfig()
11     patt = (cfg.get("filenames", {})
12               .get("patterns", {})
13               .get("inmet_csv", "inmet_{year}.csv"))
14     out_path = Path(proc_dir) / patt.format(year=year)
15     if out_path.exists() and not overwrite:
16         log.info(f"[SKIP] {out_path.name} já existe.")
17         return out_path
18     files = sorted([*year_dir.glob("*.CSV"), *year_dir.glob("*.csv")])
19     if not files:
20         log.warning(f"[WARN] Nenhum .CSV encontrado em {year_dir}")
21         return None
22     dfs = []
23     for fp in files:
24         try:
25             header, cidade, lat, lon = _parse_inmet_header(fp)
26             df = pd.read_csv(
27                 fp, sep=";", skiprows=9, encoding="latin1",
28                 engine="python", on_bad_lines="skip"
29             )
30             if df.shape[1] > len(header):
31                 log.warning(f"[WARN] Colunas extras detectadas em: {fp.name}")
32                 while len(header) < df.shape[1]:
33                     header.append(f"COLUNA_EXTRA_{len(header)+1}")
34             elif df.shape[1] < len(header):
35                 log.warning(f"[WARN] Header maior que colunas de dados em {fp.name}. Pulando.")
36                 continue
37             df.columns = header
38             df["ANO"] = year
39             df["CIDADE"] = cidade
40             df["LATITUDE"] = lat
41             df["LONGITUDE"] = lon
42             cols_exist = [c for c in drop_cols if c in df.columns]
43             if cols_exist:
44                 df.drop(columns=cols_exist, inplace=True, errors="ignore")
45             df = df.loc[:, ~df.columns.str.startswith("COLUNA_EXTRA")]
46             dfs.append(df)
47             log.info(f"[READ] {fp.name}")
48         except Exception as e:
49             log.error(f"[ERROR] {fp} -> {e}")
50     if not dfs:
51         log.warning(f"[WARN] Nenhum dado válido para {year}.")
52         return None
53     final = pd.concat(dfs, ignore_index=True)
54     final.to_csv(out_path, index=False, encoding="utf-8")
55     log.info(f"[WRITE] {out_path}")
56     return out_path
```

Contratos importantes:

* Entrada: pasta anual inferida por `_inmet_year_dir` (4 a 7).
* Leitura: `skiprows=9` alinha dados ao cabeçalho lido na linha 9 do arquivo original (26 a 29).
* Saneamento: ajustes quando o número de colunas difere do cabeçalho (30 a 36), remoção de colunas extras artificiais e de colunas a descartar (42 a 46).
* Metadados: ANO, CIDADE, LATITUDE, LONGITUDE adicionados explicitamente (38 a 41).
* Saída: `paths.providers.inmet.processed/inmet_{year}.csv` por padrão, customizável via `filenames.patterns.inmet_csv` (10 a 14).

### Lote de anos

```python
1  def process_inmet_years(years: Iterable[int], overwrite: bool = False) -> list[Path]:
2      out: list[Path] = []
3      for y in years:
4          p = process_inmet_year(int(y), overwrite=overwrite)
5          if p:
6              out.append(p)
7      return out
```

---

## 9. Main de teste e orquestração mínima

```python
1  if __name__ == "__main__":
2      cfg = loadConfig()
3      log = get_logger("utils.test", kind="load", per_run_file=True)
4      years = cfg.get("inmet", {}).get("years", list(range(2000, 2026)))
5      log.info(f"Processando anos de teste: {years}")
6      process_inmet_years(years, overwrite=False)
```

Função: executar consolidação de anos definidos no config ou default 2000 a 2025, com log por execução.

---

## 10. Propriedades formais e complexidade

* Determinismo: dada uma versão fixa de arquivos CSV, `process_inmet_year` gera o mesmo CSV de saída para a mesma configuração. O único efeito temporal está no carimbo do arquivo de log por execução.
* Idempotência: `ensure_dir` é idempotente; `unzip_file` pode pular extração se a pasta já existe; `process_inmet_year` preserva saídas existentes quando `overwrite=False`.
* Complexidade:

  * HTTP: O(B) no total de bytes baixados, CPU irrelevante, latência de rede dominante.
  * Extração: O(U) nos bytes descompactados, I/O bound.
  * Consolidação: O(Nf + R) para cada ano, onde Nf é número de arquivos CSV e R é total de linhas; memória proporcional ao maior dataframe em memória por concatenação, mitigável por escrita incremental se necessário.

---

## 11. Observabilidade e tratamento de falhas

Pontos de log chave:

* Progresso de download com porcentagem quando `Content-Length` existe (stream_download 15 a 17).
* Skips e avisos na extração (unzip_file 4 a 7, 14 a 17).
* Sinais de desalinhamento de colunas e headers (process_inmet_year 30 a 36).
* Erros por arquivo CSV não derrubam o job inteiro, mas registram `[ERROR]` com caminho e exceção (48 a 49).
* Arquivo de log pode ser diário por execução com sufixo `kind` para facilitar auditoria.

---

## 12. Considerações de desempenho e I/O

* Download: `chunk_size` padrão 256 KiB equilibra syscalls e latência. Para redes rápidas, 512 KiB a 1 MiB podem reduzir overhead.
* Extração: preferir SSD local e diretórios de trabalho fora de antivírus.
* Consolidação: leitura via `engine="python"` tolera CSVs malformados, mas é mais lenta. Quando possível, migrar para `engine="c"` com pré-limpeza. A concatenação final pode ser substituída por escrita incremental com `mode="a"` para datasets muito grandes.

---

## 13. Segurança, integridade e invariantes

* Download atômico: gravação em `.part` seguida de `replace` garante que consumidores nunca leiam arquivos truncados.
* Path traversal: destinos derivam do config e não do servidor; para downloads arbitrários, manter `os.path.basename` ao formar nomes.
* ZIPs inválidos: captura `BadZipFile` e repropaga após log de erro, evitando estados parcialmente extraídos.
* Encoding: leitura latin1 com `errors="ignore"` evita abortar por bytes ruins, mas pode omitir símbolos. Para auditorias, registrar contagem de linhas lidas e descartadas via `on_bad_lines="skip"`.

---

## 14. Extensões recomendadas

1. `stream_download`: verificação de integridade com SHA256 opcional e validação de tamanho quando cabeçalho estiver presente.
2. `list_zip_links_from_page`: aceitar padrões adicionais e paginação, além de heurística para DOMs mutáveis.
3. `process_inmet_year`:

   * escrita incremental em parquet particionado por mês e cidade, reduzindo custo de reprocesso;
   * schema contract explícito com dtypes e coerção de datas;
   * contadores de qualidade por arquivo: linhas lidas, puladas, colunas extras detectadas.
4. CLI: adicionar subcomandos `utils http-test`, `inmet consolidate --years 2012 2013 --overwrite`, `zip extract --root <dir> --flat`.
5. Paralelismo: `ThreadPoolExecutor` para leitura de múltiplos CSVs por ano, respeitando limites de memória.
6. Telemetria: emissão de um resumo JSON por execução com entradas de entrada e artefatos gerados.

---

## Apêndice A — Exemplos de uso

### Carregar config, preparar logs e consolidar 2015 e 2016

```python
from src.utils import loadConfig, get_logger, process_inmet_years

cfg = loadConfig(create_dirs=True)
log = get_logger("job.inmet", kind="load", per_run_file=True)
paths = process_inmet_years([2015, 2016], overwrite=False)
log.info({"out": [str(p) for p in paths]})
```

### Baixar e extrair todos os zips listados em uma página

```python
from src.utils import get_requests_session, list_zip_links_from_page, stream_download, unzip_all_in_dir, ensure_dir
from pathlib import Path

session = get_requests_session()
links = list_zip_links_from_page("https://example.org/dados", session=session)
raw_dir = ensure_dir(Path("data/raw/inmet"))
for url in links:
    fname = Path(url.split("?")[0]).name
    stream_download(url, raw_dir / fname, session=session)
unzip_all_in_dir(raw_dir, raw_dir / "csv", make_subdir_from_zip=True)
```
