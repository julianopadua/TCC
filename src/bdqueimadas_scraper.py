# src/bdqueimadas_scraper.py
# =============================================================================
# BDQUEIMADAS — Scraper dos .zip anuais (Brasil_sat_ref) no COIDS/INPE
# Baixa todos os arquivos focos_br_ref_YYYY.zip para data/raw/<folder_name>
# Opcional: extrai para data/raw/<folder_name>/csv
# Dep.: utils.py (loadConfig, get_logger, get_path, ensure_dir, requests helpers)
# =============================================================================
from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, Optional, List
from urllib.parse import urljoin

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    get_requests_session,
    list_zip_links_from_page,
    stream_download,
    unzip_all_in_dir,
)

# ----------------------------------------------------------------------------- 
# [SEÇÃO 1] CONFIG E CONSTANTES
# -----------------------------------------------------------------------------
cfg = loadConfig()
log = get_logger("bdqueimadas.scraper", kind="scraper", per_run_file=True)

# URL base fornecida (com barra final para join consistente)
BDQ_BASE_URL = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/"
# Pasta padrão pedida
DEFAULT_FOLDER = "ID_BDQUEIMADAS"

def get_target_processed_dir(folder_name: str = DEFAULT_FOLDER) -> Path:
    """
    Retorna o diretório de saída em data/processed/<folder_name>,
    ignorando o caminho fixo do config.yaml.
    """
    base = get_path("paths", "data", "processed")
    target = Path(base) / (folder_name or DEFAULT_FOLDER)
    return ensure_dir(target)


# ----------------------------------------------------------------------------- 
# [SEÇÃO 2] FUNÇÕES DE ALTO NÍVEL
# -----------------------------------------------------------------------------
def discover_bdq_zip_links(base_url: str = BDQ_BASE_URL) -> List[str]:
    """
    Retorna links absolutos para todos os .zip listados na página 'Brasil_sat_ref'.
    """
    session = get_requests_session()
    links = list_zip_links_from_page(base_url, session=session)
    # defesa extra: garantir absolutos e filtrar 'focos_br_ref_YYYY.zip'
    out = []
    for h in links:
        abs_url = urljoin(base_url, h)
        name = os.path.basename(abs_url.split("?")[0]).lower()
        if name.endswith(".zip") and name.startswith("focos_br_ref_"):
            out.append(abs_url)
    out = sorted(set(out))
    log.info(f"{len(out)} .zip detectados em {base_url}")
    return out


def filter_links_by_year(links: Iterable[str], years: Optional[Iterable[int]]) -> List[str]:
    """
    Se 'years' for fornecido, mantém apenas os .zip cujo sufixo de ano está na lista.
    """
    if not years:
        return list(links)
    wanted = {str(int(y)) for y in years}
    out = []
    for url in links:
        fname = os.path.basename(url.split("?")[0]).lower()  # focos_br_ref_YYYY.zip
        try:
            stem = fname.rsplit(".", 1)[0]
            year = stem.split("_")[-1]
        except Exception:
            continue
        if year in wanted:
            out.append(url)
    return sorted(out)


def get_target_raw_dir(folder_name: str = DEFAULT_FOLDER) -> Path:
    """
    Cria e retorna data/raw/<folder_name>.
    """
    raw_root = get_path("paths", "data", "raw")
    target = Path(raw_root) / (folder_name or DEFAULT_FOLDER)
    return ensure_dir(target)


def download_bdq_archives(
    folder_name: str = DEFAULT_FOLDER,
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
) -> List[Path]:
    """
    Baixa os .zip do BDQueimadas (Brasil_sat_ref) para data/raw/<folder_name>.
    - years: filtra por anos; se None, baixa todos os listados.
    - overwrite: True para rebaixar mesmo se o arquivo existir.
    Retorna a lista de caminhos baixados.
    """
    target_dir = get_target_raw_dir(folder_name)
    session = get_requests_session()

    all_links = discover_bdq_zip_links(BDQ_BASE_URL)
    sel_links = filter_links_by_year(all_links, years)

    if not sel_links:
        log.warning("[WARN] Nenhum link selecionado para download.")
        return []

    downloaded: List[Path] = []
    for url in sel_links:
        fname = os.path.basename(url.split("?")[0])
        dest = target_dir / fname
        if dest.exists() and not overwrite:
            log.info(f"[SKIP] {fname} já existe.")
            downloaded.append(dest)
            continue
        try:
            log.info(f"[DOWNLOADING] {fname}")
            stream_download(url, dest, session=session, log=log)
            downloaded.append(dest)
        except Exception as e:
            log.error(f"[ERROR] Falha ao baixar {fname}: {e}")
    return downloaded


def extract_downloaded_archives(folder_name: str = DEFAULT_FOLDER) -> Path:
    """
    Extrai todos os .zip de data/raw/<folder_name> para data/processed/<...>/<zip_stem>.
    Retorna o diretório final onde os CSVs foram depositados.
    """
    # origem (onde os .zip foram baixados)
    source_zip_dir = get_target_raw_dir(folder_name)
    # destino (onde queremos os CSVs extraídos)
    out_processed_dir = get_target_processed_dir(folder_name)
    # mantemos subpastas por zip para evitar colisões de nomes
    unzip_all_in_dir(source_zip_dir, out_processed_dir, make_subdir_from_zip=True, log=log)
    return out_processed_dir


# ----------------------------------------------------------------------------- 
# [SEÇÃO 3] MAIN/CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Scraper BDQueimadas (Brasil_sat_ref) -> data/raw/<folder>"
    )
    # Agora é OPCIONAL; default = ID_BDQUEIMADAS
    p.add_argument(
        "--folder",
        required=False,
        default=DEFAULT_FOLDER,
        help=f"Nome da pasta sob data/raw/ (default: {DEFAULT_FOLDER}).",
    )
    p.add_argument(
        "--years",
        nargs="*",
        type=int,
        default=None,
        help="Lista de anos a baixar (ex.: --years 2019 2020 2021). Se omitido, baixa todos.",
    )
    p.add_argument(
        "--overwrite",
        action="store_true",
        help="Se definido, rebaixa arquivos já existentes.",
    )
    p.add_argument(
        "--no-extract",
        action="store_true",
        help="Não extrair (por padrão, os zips são extraídos para data/processed).",
    )

    args = p.parse_args()

    folder = args.folder or DEFAULT_FOLDER
    log.info(f"[TARGET] data/raw/{folder}")
    paths = download_bdq_archives(
        folder_name=folder,
        years=args.years,
        overwrite=args.overwrite,
    )
    log.info(f"[OK] {len(paths)} arquivo(s) disponíveis em data/raw/{folder}")

    if not args.no_extract:
        out_dir = extract_downloaded_archives(folder)
        log.info(f"[EXTRACTED] Arquivos extraídos em: {out_dir}")

