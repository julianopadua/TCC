# src/inmet_scraper.py
# =============================================================================
# INMET SCRAPER — DOWNLOAD E EXTRAÇÃO DE HISTÓRICOS (ZIP -> CSV)
# Depende de: requests, beautifulsoup4 (opcional lxml), utils.py
# =============================================================================
from __future__ import annotations

from pathlib import Path
import os


from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    get_requests_session,
    list_zip_links_from_page,
    stream_download,
    unzip_all_in_dir,
    get_inmet_paths,
    get_bdqueimadas_paths,
)


# -----------------------------------------------------------------------------
# [SEÇÃO 1] CONFIG E CONSTANTES
# -----------------------------------------------------------------------------
cfg = loadConfig()
log = get_logger("inmet.scraper", kind="scraper", per_run_file=True)

# Base URL do INMET (usa fallback se não estiver no config.yaml)
INMET_BASE_URL = (
    cfg.get("inmet", {}).get("base_url")
    or "https://portal.inmet.gov.br/dadoshistoricos"
)

# Pastas (raw/csv) para INMET
INMET_RAW_DIR, INMET_CSV_DIR = get_inmet_paths()


# -----------------------------------------------------------------------------
# [SEÇÃO 2] FUNÇÕES DE ALTO NÍVEL (PIPELINE)
# -----------------------------------------------------------------------------
def discover_inmet_zip_links(base_url: str = INMET_BASE_URL) -> list[str]:
    """
    Lê a página de dados históricos do INMET e retorna links absolutos para .zip.
    """
    session = get_requests_session()
    links = list_zip_links_from_page(base_url, session=session)
    log.info(f"{len(links)} arquivos .zip encontrados em {base_url}")
    return links


def download_inmet_archives(links: list[str]) -> None:
    """
    Faz download dos .zip do INMET para o diretório raw configurado.
    """
    session = get_requests_session()
    for url in links:
        fname = os.path.basename(url.split("?")[0])
        dest = Path(INMET_RAW_DIR) / fname
        if dest.exists():
            log.info(f"[SKIP] {fname} já existe em {dest}")
            continue
        try:
            log.info(f"[DOWNLOADING] {fname}")
            stream_download(url, dest, session=session, log=log)
        except Exception as e:  # pragma: no cover
            log.error(f"[ERROR] Falha ao baixar {fname}: {e}")


def extract_inmet_archives() -> None:
    """
    Extrai todos os .zip do diretório raw/INMET para raw/INMET/csv/<nome_do_zip>.
    """
    unzip_all_in_dir(INMET_RAW_DIR, INMET_CSV_DIR, make_subdir_from_zip=True, log=log)


def extract_bdqueimadas_archives() -> None:
    """
    Procura zips em raw/BDQUEIMADAS e extrai para raw/BDQUEIMADAS/csv/<nome_do_zip>.
    Útil para o caso 'Brasil_sat_ref' ou quaisquer zips colocados no raw do provedor.
    """
    BDQ_RAW, BDQ_CSV = get_bdqueimadas_paths()
    unzip_all_in_dir(BDQ_RAW, BDQ_CSV, make_subdir_from_zip=True, log=log)

# -----------------------------------------------------------------------------
# [SEÇÃO 2.5] Consolidação pós-extração (CSV -> processed/INMET/inmet_{year}.csv)
# -----------------------------------------------------------------------------
from utils import process_inmet_years  # importe no topo do arquivo, junto com os demais

def consolidate_inmet_after_extract(years: list[int] | None = None, overwrite: bool = False) -> None:
    """
    Consolida os CSVs extraídos do INMET em um único CSV por ano no diretório processed.
    Se `years` não for fornecido, usa lista do config.yaml ou infere pelos diretórios em INMET_CSV_DIR.
    """
    yrs = years or cfg.get("inmet", {}).get("years")
    if not yrs:
        # tenta inferir por diretórios com nome numérico
        yrs = sorted({int(p.name) for p in INMET_CSV_DIR.iterdir() if p.is_dir() and p.name.isdigit()})
        if not yrs:
            log.warning("[WARN] Não foi possível inferir anos a partir de INMET/csv.")
            return
    log.info(f"[CONSOLIDATE] Anos: {yrs}")
    process_inmet_years(yrs, overwrite=overwrite)


# -----------------------------------------------------------------------------
# [SEÇÃO 3] MAIN
# -----------------------------------------------------------------------------
def main() -> None:
    ensure_dir(INMET_RAW_DIR)
    ensure_dir(INMET_CSV_DIR)

    links = discover_inmet_zip_links()
    download_inmet_archives(links)
    extract_inmet_archives()

    # Consolidação dos CSVs extraídos em processed/INMET
    consolidate_inmet_after_extract(overwrite=False)

    # Opcional: também extrair BDQueimadas, se desejar
    # extract_bdqueimadas_archives()


if __name__ == "__main__":
    main()
