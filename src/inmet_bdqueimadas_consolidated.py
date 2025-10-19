# src/inmet_bdqueimadas_consolidated.py
# =============================================================================
# Consolidação INMET (all_years) × BDQUEIMADAS (bdq_targets_all_years)
# Saída: data/consolidated/INMET_BDQ/inmet_bdq_hourly.csv (ou _YYYY-YYYY.csv)
# - Agrega BDQ por HORA + PAIS + ESTADO + MUNICIPIO
# - Casa com INMET por HORA + MUNICIPIO (valida UF quando disponível)
# - Mantém valores INMET como vieram (ex.: -999), sem filtrar aqui
# - INMET em streaming + merge e escrita incremental (sem estourar memória)
# - Suporta --years e --validation (amostra rápida)
# Dependências: utils.py (loadConfig, get_logger, get_path, ensure_dir)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Optional, Iterable, List, Tuple
import unicodedata
import pandas as pd
import numpy as np
import time
import re
import sys
import csv as _csv

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
)

cfg = loadConfig()
log = get_logger("inmet_bdq.consolidate", kind="load", per_run_file=True)

# -----------------------------------------------------------------------------
# PATHS (usa paths.data.external do config.yaml -> "./data/consolidated")
# -----------------------------------------------------------------------------
BASE_CONSOLIDATED = get_path("paths", "data", "external")
INMET_PATH = Path(BASE_CONSOLIDATED) / "INMET" / "inmet_all_years.csv"
BDQ_PATH   = Path(BASE_CONSOLIDATED) / "BDQUEIMADAS" / "bdq_targets_all_years.csv"
OUT_DIR    = ensure_dir(Path(BASE_CONSOLIDATED) / "INMET_BDQ")

# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------
_CTRL_RE = re.compile(r"[\x00-\x1F\x7F-\x9F]")
_WS_RE   = re.compile(r"[\u00A0\u200B\u200C\u200D\uFEFF]")  # NBSP, ZWSP, BOM etc.

def _strip_controls(x: str) -> str:
    if x is None:
        return ""
    s = _WS_RE.sub(" ", str(x))
    s = _CTRL_RE.sub("", s)
    return s

def _repair_mojibake(x: str) -> str:
    if x is None:
        return ""
    s = str(x)
    if any(t in s for t in ("Ã", "Â", "Ê", "Ô", "Õ", "", "¢", "§", "ã", "õ", "ç")):
        try:
            s = s.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
        except Exception:
            pass
    return s

def _norm_loc(x: str) -> str:
    # normaliza pt-BR p/ padronizar com INMET/BDQ: sem acento, caixa alta
    s = _repair_mojibake(x)
    s = _strip_controls(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return s.upper().strip()

def _log_phase(title: str):
    log.info(f"[PHASE] {title}")

def _read_csv_smart(path: Path, **kw) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False, on_bad_lines="skip", **kw)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="latin1", low_memory=False, on_bad_lines="skip", **kw)

def _bump_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            _csv.field_size_limit(limit)
            break
        except OverflowError:
            limit = int(limit // 10)
            if limit < 1_000_000:
                _csv.field_size_limit(1_000_000)
                break

# -----------------------------------------------------------------------------
# BDQ LOADER (full in-memory, mas com usecols e agregado por hora)
# -----------------------------------------------------------------------------
def load_bdq(
    bdq_path: Path,
    years: Optional[Iterable[int]] = None,
    id_preference: str = "foco",
    validation_limit: Optional[int] = None
) -> pd.DataFrame:
    if not bdq_path.exists():
        raise FileNotFoundError(f"BDQ não encontrado: {bdq_path}")

    _log_phase(f"Lendo BDQ: {bdq_path}")
    t0 = time.time()

    usecols = ["DATAHORA","PAIS","ESTADO","MUNICIPIO","FRP","RISCO_FOGO","FOCO_ID","ID_BDQ"]
    df = _read_csv_smart(bdq_path, usecols=usecols)
    log.info(f"  BDQ linhas: {len(df):,} | tempo={time.time()-t0:,.2f}s")

    # Localidade normalizada
    for col in ("PAIS", "ESTADO", "MUNICIPIO"):
        if col in df.columns:
            df[col] = df[col].map(_norm_loc)

    # Tempo -> hora cheia
    df["DATAHORA"] = pd.to_datetime(df["DATAHORA"], errors="coerce")
    df["HORA"] = df["DATAHORA"].dt.floor("h")
    df["ANO"] = df["HORA"].dt.year

    if years:
        years = sorted({int(y) for y in years})
        before = len(df)
        df = df[df["ANO"].isin(years)].copy()
        log.info(f"  BDQ filtro anos {years} | {before:,} -> {len(df):,}")

    if validation_limit:
        df = df.head(int(validation_limit)).copy()
        log.info(f"  [VALIDATION] BDQ limitado a {len(df):,} linhas")

    id_col = "FOCO_ID" if id_preference.lower() == "foco" else "ID_BDQ"
    if id_col not in df.columns:
        id_col = "FOCO_ID" if "FOCO_ID" in df.columns else "ID_BDQ"

    agg = {
        "FRP": "mean",
        "RISCO_FOGO": "mean",
        id_col: "first",
        "DATAHORA": "count",  # contagem de focos por hora
    }
    gb_cols = ["HORA", "PAIS", "ESTADO", "MUNICIPIO"]
    dfh = df.groupby(gb_cols, dropna=False).agg(agg).reset_index()
    dfh.rename(columns={"DATAHORA": "N_FOCOS", id_col: "ID_TARGET"}, inplace=True)
    dfh.sort_values(["HORA", "ESTADO", "MUNICIPIO"], kind="stable", inplace=True)
    log.info(f"  BDQ por hora: {len(dfh):,} linhas (agrupado) | tempo total={time.time()-t0:,.2f}s")
    return dfh

# -----------------------------------------------------------------------------
# INMET LOADER (STREAMING) — detecção robusta de colunas de DATA/HORA
# -----------------------------------------------------------------------------
def _normalize_colname(s: str) -> str:
    s0 = unicodedata.normalize("NFKD", s)
    s0 = "".join(ch for ch in s0 if not unicodedata.combining(ch))
    return s0.upper()

def _discover_inmet_columns(df_head: pd.DataFrame) -> dict:
    # normaliza nomes para decidir melhor
    norm_map = {c: _normalize_colname(c) for c in df_head.columns}

    # DATA: começa com "DATA"
    candidates_data = [c for c, n in norm_map.items() if n.startswith("DATA")]
    col_data = candidates_data[0] if candidates_data else "DATA"

    # HORA: prioriza exatamente "HORA" ou "HORA (UTC)" (evita "HORARIA")
    def is_hour_col(n: str) -> bool:
        return n == "HORA" or n.startswith("HORA ")

    candidates_hora = [c for c, n in norm_map.items() if is_hour_col(n)]
    if not candidates_hora:
        # fallback: contém "HORA" mas não "HORARIA"
        candidates_hora = [c for c, n in norm_map.items() if "HORA" in n and "HORARIA" not in n]
    col_hora = candidates_hora[0] if candidates_hora else "HORA"

    # CIDADE & ESTADO/UF
    candidates_city = [c for c, n in norm_map.items() if "CIDADE" in n]
    col_cidade = candidates_city[0] if candidates_city else "CIDADE"

    col_estado = None
    for c, n in norm_map.items():
        if n == "ESTADO" or n == "UF":
            col_estado = c
            break
    return {"DATA": col_data, "HORA": col_hora, "CIDADE": col_cidade, "ESTADO": col_estado}

def _open_inmet_reader(inmet_path: Path, engine: str, chunksize: int):
    return pd.read_csv(
        inmet_path,
        encoding="utf-8",
        low_memory=True,
        on_bad_lines="skip",
        dtype=str,
        chunksize=chunksize,
        engine=engine,
    )

def _iter_inmet_filtered_chunks(
    inmet_path: Path,
    years: Optional[Iterable[int]],
    chunksize: int
) -> Tuple[dict, Iterable[pd.DataFrame]]:
    """Abre reader com engine 'c' (fallback python) e itera chunks já com HORA/ANO calculados."""
    head = _read_csv_smart(inmet_path, nrows=50, dtype=str)
    cols = _discover_inmet_columns(head)
    col_data, col_hora, col_cidade, col_estado = cols["DATA"], cols["HORA"], cols["CIDADE"], cols["ESTADO"]

    engines = ["c", "python"]
    reader = None
    last_err = None
    for eng in engines:
        try:
            if eng == "python":
                _bump_csv_field_limit()
            reader = _open_inmet_reader(inmet_path, eng, chunksize)
            # sanity check: force first chunk
            _ = next(iter(reader))
            reader = _open_inmet_reader(inmet_path, eng, chunksize)
            log.info(f"  INMET usando engine='{eng}'")
            break
        except Exception as e:
            last_err = e
            log.warning(f"  engine='{eng}' falhou: {e}")
    if reader is None:
        raise RuntimeError(f"Falha ao abrir INMET em chunks. Último erro: {last_err}")

    years_set = {int(y) for y in years} if years else None

    def generator():
        chunks_read = 0
        kept_total = 0
        for chunk in reader:
            chunks_read += 1

            # Timestamp horário
            ts = pd.to_datetime(
                chunk[col_data].astype(str).str.strip() + " " + chunk[col_hora].astype(str).str.strip(),
                errors="coerce",
                utc=False
            )
            chunk["HORA"] = ts.dt.floor("h")
            chunk["ANO"] = chunk["HORA"].dt.year

            # Localidade normalizada
            chunk["PAIS"] = "BRASIL"
            chunk["MUNICIPIO"] = chunk[col_cidade].map(_norm_loc) if col_cidade in chunk.columns else np.nan
            chunk["ESTADO"]    = chunk[col_estado].map(_norm_loc) if col_estado else np.nan

            # Filtro de anos
            if years_set:
                chunk = chunk[chunk["ANO"].isin(years_set)]

            if len(chunk) == 0:
                continue

            kept_total += len(chunk)
            if chunks_read % 5 == 0:
                log.info(f"  [INMET] chunks lidos: {chunks_read} | acumulado pós-filtro: {kept_total:,}")

            yield chunk

    return cols, generator()

# -----------------------------------------------------------------------------
# MERGE E ESCRITA INCREMENTAL
# -----------------------------------------------------------------------------
def _merge_chunk_with_bdq(chunk_in: pd.DataFrame, bdq_hourly: pd.DataFrame) -> pd.DataFrame:
    # renomeia BDQ p/ evitar colisões
    bq = bdq_hourly.rename(columns={"PAIS":"PAIS_BDQ", "ESTADO":"ESTADO_BDQ"})
    m = chunk_in.merge(bq, on=["HORA", "MUNICIPIO"], how="left", suffixes=("_IN", "_BDQ"))

    # valida UF quando possível
    if "ESTADO_IN" in m.columns and "ESTADO_BDQ" in m.columns:
        eq = (m["ESTADO_IN"].fillna("") == m["ESTADO_BDQ"].fillna(""))
        m = m.loc[eq | m["ESTADO_BDQ"].isna()].copy()

    # garantir colunas alvo BDQ
    for c in ("FRP", "RISCO_FOGO", "N_FOCOS", "ID_TARGET"):
        if c not in m.columns:
            m[c] = np.nan

    # consolidar colunas finais principais
    if "PAIS" not in m.columns:
        m["PAIS"] = m.get("PAIS_IN", "BRASIL")
    if "ESTADO" not in m.columns and "ESTADO_IN" in m.columns:
        m["ESTADO"] = m["ESTADO_IN"]

    # ordena principais
    if "HORA" in m.columns:
        m.sort_values(["HORA", "ESTADO", "MUNICIPIO"], kind="stable", inplace=True)

    # põe principais primeiro
    front = [c for c in ["HORA","PAIS","ESTADO","MUNICIPIO","FRP","RISCO_FOGO","N_FOCOS","ID_TARGET"] if c in m.columns]
    other = [c for c in m.columns if c not in front]
    m = m[front + other]
    return m

# -----------------------------------------------------------------------------
# MAIN PIPELINE
# -----------------------------------------------------------------------------
def run(
    years: Optional[Iterable[int]] = None,
    id_preference: str = "foco",   # "foco" ou "bdq"
    validation: bool = False,       # modo rápido (amostra)
    validation_limit: int = 100,    # tamanho da amostra (após filtro por ano)
    chunksize: int = 250_000        # tamanho do chunk para o INMET
) -> Path:
    # 1) Carrega BDQ agregado por hora em memória (relativamente pequeno)
    bdq_hourly = load_bdq(
        BDQ_PATH,
        years=years,
        id_preference=id_preference,
        validation_limit=(validation_limit if validation else None),
    )

    # 2) Define saída
    out_name = "inmet_bdq_hourly.csv" if not years else f"inmet_bdq_hourly_{'-'.join(map(str, sorted({int(y) for y in years})))}.csv"
    out_path = OUT_DIR / out_name
    if out_path.exists():
        out_path.unlink(missing_ok=True)  # reescreve

    # 3) Modo validação: ler pequeno subconjunto em memória e escrever de uma vez
    if validation:
        _log_phase(f"Lendo INMET (validação): {INMET_PATH}")
        # pega N linhas já filtradas do(s) ano(s)
        cols, gen = _iter_inmet_filtered_chunks(INMET_PATH, years, chunksize=max(10_000, chunksize//10))
        kept = 0
        buf: List[pd.DataFrame] = []
        for ch in gen:
            faltam = int(validation_limit) - kept
            if faltam <= 0:
                break
            if len(ch) > faltam:
                ch = ch.head(faltam).copy()
            buf.append(ch)
            kept += len(ch)
        if not buf:
            log.warning("Nenhum registro INMET para validação.")
            # ainda assim gravar CSV vazio com header mínimo
            pd.DataFrame(columns=["HORA","PAIS","ESTADO","MUNICIPIO","FRP","RISCO_FOGO","N_FOCOS","ID_TARGET"]).to_csv(out_path, index=False, encoding="utf-8")
            log.info(f"[DONE] {out_path} (vazio)")
            return out_path

        df_in = pd.concat(buf, ignore_index=True)
        merged = _merge_chunk_with_bdq(df_in, bdq_hourly)
        merged.to_csv(out_path, index=False, encoding="utf-8")
        log.info(f"[DONE] {out_path}  (linhas: {len(merged):,})  [validation]")
        return out_path

    # 4) Modo completo: streaming + append incremental
    _log_phase(f"Lendo INMET (streaming): {INMET_PATH}")
    _, gen = _iter_inmet_filtered_chunks(INMET_PATH, years, chunksize=chunksize)

    total_written = 0
    wrote_header = False
    t0 = time.time()
    for i, chunk_in in enumerate(gen, start=1):
        merged = _merge_chunk_with_bdq(chunk_in, bdq_hourly)
        # escreve em append
        merged.to_csv(out_path, index=False, mode=("w" if not wrote_header else "a"), header=not wrote_header, encoding="utf-8")
        wrote_header = True
        total_written += len(merged)
        if i % 5 == 0:
            log.info(f"  [WRITE] chunks gravados: {i} | linhas acumuladas: {total_written:,}")

    log.info(f"[DONE] {out_path}  (linhas: {total_written:,})  | tempo total={time.time()-t0:,.2f}s")
    return out_path

# -----------------------------------------------------------------------------
# CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Consolidação INMET × BDQueimadas por HORA (streaming incremental)")
    p.add_argument("--years", nargs="*", type=int, default=None,
                   help="Filtra anos (ex.: --years 2012 2013). Se omitido, usa todos.")
    p.add_argument("--id-preference", choices=["foco", "bdq"], default="foco",
                   help="Qual ID trazer do BDQ como representativo da hora (default: foco).")
    p.add_argument("--validation", action="store_true",
                   help="Modo de validação rápida (amostra pequena, após filtro por ano).")
    p.add_argument("--validation-limit", type=int, default=100,
                   help="Tamanho da amostra no modo de validação (default: 100).")
    p.add_argument("--chunksize", type=int, default=250000,
                   help="Tamanho do chunk para leitura do INMET (default: 250k).")
    args = p.parse_args()

    run(
        years=args.years,
        id_preference=args.id_preference,
        validation=args.validation,
        validation_limit=args.validation_limit,
        chunksize=args.chunksize,
    )
