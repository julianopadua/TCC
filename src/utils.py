# src/utils.py
# =============================================================================
# UTILITÁRIOS — PROJETO TCC (CONFIG, LOG, FS, HTTP/SCRAPING, ZIP, PROVEDORES)
# =============================================================================
from __future__ import annotations

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Optional
from urllib.parse import urljoin
from typing import Iterable, Optional
import re
import unicodedata
import pandas as pd
import zipfile
from datetime import datetime
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None  # fallback: timezone local

import requests
from requests.adapters import HTTPAdapter
try:
    # urllib3 >= 2
    from urllib3.util.retry import Retry
except Exception:  # pragma: no cover
    # Fallback (deve existir)
    from urllib3.util.retry import Retry  # type: ignore

import yaml

# -----------------------------------------------------------------------------
# [SEÇÃO 0] CACHES GLOBAIS
# -----------------------------------------------------------------------------
_CONFIG_CACHE: Dict[str, Any] | None = None
_ROOT_CACHE: Path | None = None


# -----------------------------------------------------------------------------
# [SEÇÃO 1] RAIZ DO PROJETO E CONFIG
# -----------------------------------------------------------------------------
def _find_project_root() -> Path:
    """
    Descobre a raiz do projeto assumindo este arquivo em: <root>/src/utils.py
    """
    return Path(__file__).resolve().parents[1]


def _expand_path(value: str, base: Path) -> str:
    """
    Expande ~ e variáveis de ambiente e torna o caminho absoluto relativo à raiz do projeto.
    """
    expanded = os.path.expanduser(os.path.expandvars(value))
    p = Path(expanded)
    if not p.is_absolute():
        p = base / p
    return str(p.resolve())


def _resolve_paths(node: Any, base: Path) -> Any:
    """
    Resolve recursivamente os campos de caminho (strings com '/' ou que começam com '.').
    """
    if isinstance(node, dict):
        return {k: _resolve_paths(v, base) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_paths(v, base) for v in node]
    if isinstance(node, str) and ("/" in node or node.startswith(".")):
        return _expand_path(node, base)
    return node


def loadConfig(config_path: str | Path | None = None, create_dirs: bool | None = None) -> Dict[str, Any]:
    """
    Carrega config.yaml e:
      - injeta paths.root (raiz do projeto)
      - resolve todos os caminhos para absolutos
      - cria diretórios se io.create_missing_dirs = true (ou se create_dirs=True)
    Pode sobrescrever o caminho do arquivo via env: PROJECT_CONFIG.
    """
    global _CONFIG_CACHE, _ROOT_CACHE
    if _CONFIG_CACHE is not None:
        return _CONFIG_CACHE

    _ROOT_CACHE = _find_project_root()

    # Prioridade: argumento > env PROJECT_CONFIG > <root>/config.yaml
    cfg_path = Path(config_path) if config_path else None
    if cfg_path is None:
        env_cfg = os.getenv("PROJECT_CONFIG", "")
        cfg_path = Path(env_cfg) if env_cfg else (_ROOT_CACHE / "config.yaml")

    if not cfg_path.exists():
        raise FileNotFoundError(f"Config file not found: {cfg_path}")

    with cfg_path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    # Garante seção paths e injeta raiz absoluta
    cfg.setdefault("paths", {})
    cfg["paths"]["root"] = str(_ROOT_CACHE)

    # Resolve todos os caminhos relativos à raiz do projeto
    cfg["paths"] = _resolve_paths(cfg.get("paths", {}), _ROOT_CACHE)

    # Sinal para criação de diretórios
    make_dirs = cfg.get("io", {}).get("create_missing_dirs", False)
    if create_dirs is not None:
        make_dirs = create_dirs

    if make_dirs:
        _create_all_paths(cfg)

    _CONFIG_CACHE = cfg
    return cfg


def get_path(*keys: str) -> Path:
    """
    Helper para recuperar caminhos por chave aninhada.
    Ex.: get_path('paths', 'data', 'processed')
         get_path('paths', 'providers', 'bdqueimadas', 'processed')
    """
    cfg = loadConfig()
    node: Any = cfg
    for k in keys:
        node = node[k]
    return Path(node)


def _create_all_paths(cfg: Dict[str, Any]) -> None:
    """
    Cria todos os diretórios presentes em cfg['paths'] e o diretório do arquivo de log.
    """
    def collect_dirs(node: Any) -> list[Path]:
        acc: list[Path] = []
        if isinstance(node, dict):
            for v in node.values():
                acc.extend(collect_dirs(v))
        elif isinstance(node, list):
            for v in node:
                acc.extend(collect_dirs(v))
        elif isinstance(node, str):
            acc.append(Path(node))
        return acc

    paths_node = cfg.get("paths", {})
    dirs = collect_dirs(paths_node)

    log_file = cfg.get("logging", {}).get("file")
    if log_file:
        dirs.append(Path(log_file).parent)

    for d in dirs:
        try:
            Path(d).mkdir(parents=True, exist_ok=True)
        except Exception as e:
            print(f"[WARN] Falha ao criar diretório: {d} -> {e}", file=sys.stderr)

def _now_tz():
    """Agora no fuso do config.project.timezone (fallback: local)."""
    cfg = loadConfig()
    tzname = (cfg.get("project", {}) or {}).get("timezone")
    tz = ZoneInfo(tzname) if (ZoneInfo and tzname) else None
    return datetime.now(tz) if tz else datetime.now()

def _build_daily_log_file(kind: str) -> Path:
    """
    Retorna o caminho do arquivo de log do dia:
      logs/log_YYYYMMDD/{kind}_HHMMSS.log
    """
    base_logs = get_path("paths", "logs")  # usa paths.logs do config
    now = _now_tz()
    day_dir = ensure_dir(Path(base_logs) / f"log_{now:%Y%m%d}")
    return day_dir / f"{kind}_{now:%H%M%S}.log"


# -----------------------------------------------------------------------------
# [SEÇÃO 2] LOGGING
# -----------------------------------------------------------------------------
def get_logger(name: str = "app", *, kind: str | None = None, per_run_file: bool = False) -> logging.Logger:
    """
    Retorna um logger configurado conforme 'logging' no config.yaml.
    Cria handler de console e de arquivo (rotativo) se 'logging.file' existir.
    """
    cfg = loadConfig()
    log_cfg = cfg.get("logging", {})
    level_name = log_cfg.get("level", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # Console
    sh = logging.StreamHandler()
    sh.setLevel(level)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # Arquivo (rotativo ou diário por execução)
    log_file_cfg = (cfg.get("logging", {}) or {}).get("file")  # fallback do config
    if per_run_file and kind:
        log_path = _build_daily_log_file(kind)
    else:
        log_path = Path(log_file_cfg) if log_file_cfg else None

    if log_path:
        # mantém RotatingFileHandler (ok mesmo com filename único por execução)
        max_bytes = int((cfg.get("logging", {}) or {}).get("max_bytes", 5_000_000))
        backup = int((cfg.get("logging", {}) or {}).get("backup_count", 5))
        fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(fmt)
        logger.addHandler(fh)


    return logger


# -----------------------------------------------------------------------------
# [SEÇÃO 3] FILESYSTEM HELPERS
# -----------------------------------------------------------------------------
def ensure_dir(path: Path | str) -> Path:
    """Cria diretório (se necessário) e retorna Path absoluto."""
    p = Path(path).resolve()
    p.mkdir(parents=True, exist_ok=True)
    return p


def list_files(root: Path | str, patterns: Iterable[str]) -> List[Path]:
    """Lista arquivos por padrões (glob) recursivamente."""
    root = Path(root)
    out: List[Path] = []
    for pat in patterns:
        out.extend(root.rglob(pat))
    return sorted(set(out))

def normalize_key(s: str | None) -> str:
    """
    Normaliza strings para matching robusto:
      - converte None para ""
      - NFKD + remoção de diacríticos
      - unifica aspas
      - remove qualquer caractere não alfanumérico substituindo por espaço
      - casefold e colapsa espaços
    Ex.: "Brasília" -> "brasilia"; "D'Oeste" -> "d oeste"
    """
    if s is None:
        return ""
    s = str(s).strip()
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"[^A-Za-z0-9]+", " ", s)
    s = s.casefold().strip()
    s = re.sub(r"\s+", " ", s)
    return s

# -----------------------------------------------------------------------------
# [SEÇÃO 4] HTTP / SCRAPING
# -----------------------------------------------------------------------------
def get_requests_session(retries: int = 3, backoff: float = 0.5):
    """
    Cria uma sessão requests com retries exponenciais para GET/POST.
    """

    s = requests.Session()
    retry = Retry(
        total=retries,
        connect=retries,
        read=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "POST"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


def _soup(html_text: str):
    """Cria BeautifulSoup usando lxml se disponível; fallback para html.parser."""
    try:
        from bs4 import BeautifulSoup  # lazy import
    except ImportError as e:  # pragma: no cover
        raise RuntimeError("beautifulsoup4 não instalado. `pip install beautifulsoup4 lxml`") from e
    parser = "lxml"
    try:
        import lxml  # noqa: F401
    except Exception:
        parser = "html.parser"
    return BeautifulSoup(html_text, parser)


def list_zip_links_from_page(url: str, session=None) -> List[str]:
    """
    Baixa a página e retorna uma lista de URLs absolutas que terminam com .zip
    """
    session = session or get_requests_session()
    resp = session.get(url, timeout=60)
    resp.raise_for_status()
    soup = _soup(resp.text)
    hrefs = [a.get("href") for a in soup.find_all("a", href=True)]
    hrefs = [h for h in hrefs if h and h.lower().endswith(".zip")]
    # normaliza para URLs absolutas
    return sorted(set(urljoin(url, h) for h in hrefs))


def stream_download(url: str, dest: Path | str, session=None, chunk_size: int = 1024 * 256, log: Optional[logging.Logger] = None) -> Path:
    """
    Faz download com streaming para um arquivo temporário e depois move para 'dest'.
    """
    session = session or get_requests_session()
    dest = Path(dest)
    ensure_dir(dest.parent)
    tmp = dest.with_suffix(dest.suffix + ".part")

    r = session.get(url, stream=True, timeout=120)
    r.raise_for_status()
    total = int(r.headers.get("Content-Length", 0))
    written = 0

    with tmp.open("wb") as f:
        for chunk in r.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                written += len(chunk)
                if log and total:
                    pct = (written / total) * 100
                    log.debug(f"Baixando {dest.name}: {pct:5.1f}%")

    tmp.replace(dest)
    if log:
        log.info(f"[DOWNLOADED] {dest} ({written/1e6:.2f} MB)")
    return dest


# -----------------------------------------------------------------------------
# [SEÇÃO 5] ZIP / EXTRAÇÃO
# -----------------------------------------------------------------------------
def unzip_file(zip_path: Path | str, out_dir: Path | str, skip_if_exists: bool = True, log: Optional[logging.Logger] = None) -> List[Path]:
    """
    Extrai um .zip para um diretório específico. Retorna lista de arquivos extraídos.
    """

    zip_path = Path(zip_path)
    out_dir = Path(out_dir)
    if skip_if_exists and out_dir.exists():
        if log:
            log.info(f"[SKIP] {zip_path.name} já extraído em {out_dir}")
        return list(out_dir.rglob("*"))

    ensure_dir(out_dir)
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            zf.extractall(out_dir)
        if log:
            log.info(f"[UNZIP] {zip_path.name} -> {out_dir}")
    except zipfile.BadZipFile:
        if log:
            log.error(f"[ERROR] {zip_path} corrompido ou não é ZIP válido.")
        raise
    return list(out_dir.rglob("*"))


def unzip_all_in_dir(zip_root: Path | str, extract_root: Path | str, make_subdir_from_zip: bool = True, log: Optional[logging.Logger] = None) -> None:
    """
    Varre `zip_root` por *.zip e extrai em `extract_root`.
    Se make_subdir_from_zip=True, cria uma subpasta com o nome do zip (sem extensão).
    """
    zip_root = Path(zip_root)
    extract_root = Path(extract_root)
    for z in sorted(zip_root.glob("*.zip")):
        target = extract_root / z.stem if make_subdir_from_zip else extract_root
        unzip_file(z, target, skip_if_exists=True, log=log)


# -----------------------------------------------------------------------------
# [SEÇÃO 6] HELPERS ESPECÍFICOS DE PROVEDORES
# -----------------------------------------------------------------------------
def get_inmet_paths() -> Tuple[Path, Path]:
    """
    Retorna (raw_dir, csv_dir) para INMET conforme config.yaml.
    """
    raw = get_path("paths", "providers", "inmet", "raw")
    csv_dir = ensure_dir(Path(raw) / "csv")
    return Path(raw), csv_dir


def get_bdqueimadas_paths() -> Tuple[Path, Path]:
    """
    Retorna (raw_dir, csv_dir) para BDQUEIMADAS conforme config.yaml.
    """
    raw = get_path("paths", "providers", "bdqueimadas", "raw")
    csv_dir = ensure_dir(Path(raw) / "csv")
    return Path(raw), csv_dir


# -----------------------------------------------------------------------------
# [SEÇÃO 7] INMET — Leitura & Consolidação de CSVs (ZIP extraídos -> CSV anual)
# -----------------------------------------------------------------------------

# Colunas a remover (iguais às do script antigo)
_INMET_DROP_COLS = [
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)",
]

def _inmet_year_dir(year: int) -> Optional[Path]:
    """
    Detecta o diretório onde estão os CSVs daquele ano, tolerando variações:
      - <raw>/INMET/csv/<year>/<year>/
      - <raw>/INMET/csv/<year>/
      - ou uma subpasta profundamente aninhada que termine com .../<year>
    Retorna None se não encontrar.
    """
    _, csv_root = get_inmet_paths()
    y = str(year)
    cand1 = csv_root / y / y
    cand2 = csv_root / y
    if cand1.is_dir():
        return cand1
    if cand2.is_dir():
        return cand2
    # busca profunda
    for p in csv_root.rglob("*"):
        if p.is_dir() and p.name == y:
            return p
    return None


def _parse_inmet_header(fp: Path):
    """
    Lê as primeiras linhas do arquivo INMET para extrair:
      - linha de cabeçalho (linha 9 do arquivo original, index 8)
      - cidade (linha 3, index 2), latitude (5), longitude (6) — com tolerância
    Retorna (header_list, cidade, latitude, longitude)
    """
    header, cidade, lat, lon = [], None, None, None
    try:
        with fp.open("r", encoding="latin1", errors="ignore") as f:
            lines = f.readlines()
        # Proteção caso o arquivo seja curto
        if len(lines) > 8:
            header_line = lines[8].strip()
            header = [h.strip() for h in header_line.split(";") if h.strip() != ""]
        if len(lines) > 2:
            parts = [p.strip() for p in lines[2].split(";")]
            if len(parts) > 1:
                cidade = parts[1] or None
        if len(lines) > 4:
            parts = [p.strip() for p in lines[4].split(";")]
            if len(parts) > 1:
                lat = parts[1] or None
        if len(lines) > 5:
            parts = [p.strip() for p in lines[5].split(";")]
            if len(parts) > 1:
                lon = parts[1] or None
    except Exception as e:
        get_logger("inmet.load", kind="load", per_run_file=True).warning(f"[WARN] Falha lendo header de {fp}: {e}")
    return header, cidade, lat, lon


def process_inmet_year(year: int, drop_cols: Optional[list[str]] = None, overwrite: bool = False) -> Optional[Path]:
    """
    Consolida todos os CSVs das estações do ano `year` em um único CSV processado.
    Regras derivadas do script original:
      - lê cabeçalho na linha 9 (index 8)
      - adiciona colunas ANO, CIDADE, LATITUDE, LONGITUDE
      - remove colunas configuradas
      - lida com desalinhamento de colunas criando COLUNA_EXTRA_n ou pulando se faltar
    """

    log = get_logger("inmet.load", kind="load", per_run_file=True)
    drop_cols = drop_cols or _INMET_DROP_COLS

    year_dir = _inmet_year_dir(year)
    if not year_dir:
        log.warning(f"[WARN] Pasta do ano {year} não encontrada em INMET/csv.")
        return None

    proc_dir = get_path("paths", "providers", "inmet", "processed")
    ensure_dir(proc_dir)

    # Nome de saída a partir do config (fallback padrão)
    cfg = loadConfig()
    patt = (cfg.get("filenames", {})
              .get("patterns", {})
              .get("inmet_csv", "inmet_{year}.csv"))
    out_path = Path(proc_dir) / patt.format(year=year)

    if out_path.exists() and not overwrite:
        log.info(f"[SKIP] {out_path.name} já existe.")
        return out_path

    # Coleta de arquivos .CSV (case-insensitive)
    files = sorted([*year_dir.glob("*.CSV"), *year_dir.glob("*.csv")])
    if not files:
        log.warning(f"[WARN] Nenhum .CSV encontrado em {year_dir}")
        return None

    dfs = []
    for fp in files:
        try:
            header, cidade, lat, lon = _parse_inmet_header(fp)
            df = pd.read_csv(
                fp, sep=";", skiprows=9, encoding="latin1",
                engine="python", on_bad_lines="skip"
            )

            # Ajuste de desalinhamento entre header e dados
            if df.shape[1] > len(header):
                log.warning(f"[WARN] Colunas extras detectadas em: {fp.name}")
                while len(header) < df.shape[1]:
                    header.append(f"COLUNA_EXTRA_{len(header)+1}")
            elif df.shape[1] < len(header):
                log.warning(f"[WARN] Header maior que colunas de dados em {fp.name}. Pulando.")
                continue

            df.columns = header

            # Metadados fixos
            df["ANO"] = year
            df["CIDADE"] = cidade
            df["LATITUDE"] = lat
            df["LONGITUDE"] = lon

            # Remoções
            cols_exist = [c for c in drop_cols if c in df.columns]
            if cols_exist:
                df.drop(columns=cols_exist, inplace=True, errors="ignore")

            df = df.loc[:, ~df.columns.str.startswith("COLUNA_EXTRA")]

            dfs.append(df)
            log.info(f"[READ] {fp.name}")
        except Exception as e:
            log.error(f"[ERROR] {fp} -> {e}")

    if not dfs:
        log.warning(f"[WARN] Nenhum dado válido para {year}.")
        return None

    final = pd.concat(dfs, ignore_index=True)
    final.to_csv(out_path, index=False, encoding="utf-8")
    log.info(f"[WRITE] {out_path}")
    return out_path


def process_inmet_years(years: Iterable[int], overwrite: bool = False) -> list[Path]:
    """
    Processa múltiplos anos de uma vez, sem interação (sem input()).
    """
    out: list[Path] = []
    for y in years:
        p = process_inmet_year(int(y), overwrite=overwrite)
        if p:
            out.append(p)
    return out


# -----------------------------------------------------------------------------
# [SEÇÃO 8] MAIN DE TESTE (OPCIONAL)
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    cfg = loadConfig()
    log = get_logger("utils.test", kind="load", per_run_file=True)
    years = cfg.get("inmet", {}).get("years", list(range(2000, 2026)))
    log.info(f"Processando anos de teste: {years}")
    process_inmet_years(years, overwrite=False)

