# src/build_dataset_hourly.py
# =============================================================================
# DATASET HORA-A-HORA: INMET (bioma) × BDQUEIMADAS (targets)
# Une, por município normalizado + hora, as séries do INMET (clima) e BDQ (focos).
# Saídas:
#   - data/dataset/inmet_bdq_{YYYY}_{biome}.csv (ano a ano, a partir de 2003)
#   - data/dataset/inmet_bdq_all_years_{biome}.csv (consolidado final)
# Depende de: pandas, utils.py (loadConfig, get_logger, get_path, ensure_dir, normalize_key)
# =============================================================================
from __future__ import annotations

from pathlib import Path
from typing import Iterable, List, Optional, Tuple
import re

import pandas as pd

from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    normalize_key,
)

# -----------------------------------------------------------------------------
# [SEÇÃO 1] PATHS E DESCOBERTA
# -----------------------------------------------------------------------------
def _inmet_consolidated_dir() -> Path:
    # data/consolidated/INMET
    return Path(get_path("paths", "data", "external")) / "INMET"

def _bdq_consolidated_dir() -> Path:
    # data/consolidated/BDQUEIMADAS
    return Path(get_path("paths", "data", "external")) / "BDQUEIMADAS"

def _dataset_dir() -> Path:
    # data/dataset
    return ensure_dir(get_path("paths", "data", "dataset"))

_INMET_RE = re.compile(r"^inmet_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)
_BDQ_RE   = re.compile(r"^bdq_targets_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)

def _list_inmet_years_for_biome(biome: str) -> List[int]:
    root = _inmet_consolidated_dir()
    years: List[int] = []
    for p in root.glob(f"inmet_*_{biome}.csv"):
        m = _INMET_RE.match(p.name)
        if m:
            years.append(int(m.group(1)))
    return sorted(set(years))

def _list_bdq_years_for_biome(biome: str) -> List[int]:
    root = _bdq_consolidated_dir()
    years: List[int] = []
    for p in root.glob(f"bdq_targets_*_{biome}.csv"):
        m = _BDQ_RE.match(p.name)
        if m and (m.group("biome").lower() == biome.lower()):
            years.append(int(m.group(1)))
    return sorted(set(years))

# -----------------------------------------------------------------------------
# [SEÇÃO 2] PARSERS E CHAVES DE JUNÇÃO
# -----------------------------------------------------------------------------
# Colunas possíveis no INMET (pré-2019 vs 2019+)
DATE_COL_NEW = "Data"
HOUR_COL_NEW = "Hora UTC"

DATE_COL_OLD = "DATA (YYYY-MM-DD)"
HOUR_COL_OLD = "HORA (UTC)"

def _detect_inmet_schema(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Detecta automaticamente quais colunas de data/hora usar no INMET.
    Retorna (date_col, hour_col).
    Regras:
      - Se existir par (Data, Hora UTC) -> novo esquema (2019+).
      - Caso contrário, usa (DATA (YYYY-MM-DD), HORA (UTC)) -> antigo.
      - Se nenhum par existir, lança KeyError informativo.
    """
    has_new = (DATE_COL_NEW in df.columns) and (HOUR_COL_NEW in df.columns)
    has_old = (DATE_COL_OLD in df.columns) and (HOUR_COL_OLD in df.columns)
    if has_new:
        return DATE_COL_NEW, HOUR_COL_NEW
    if has_old:
        return DATE_COL_OLD, HOUR_COL_OLD
    raise KeyError(
        "Colunas de data/hora não encontradas no INMET. "
        f"Esperado {DATE_COL_NEW}+{HOUR_COL_NEW} ou {DATE_COL_OLD}+{HOUR_COL_OLD}. "
        f"Encontradas: {list(df.columns)}"
    )

_HH_PREFIX_RE = re.compile(r"^\s*(\d{1,2})")              # captura 1-2 dígitos iniciais (ex.: '7', '07', '07:00')
_HH_0100_RE   = re.compile(r"^\s*(\d{2})\d{2}\s*(?:UTC)?", flags=re.IGNORECASE)  # captura '01' em '0100 UTC'

def _parse_hour_to_hh(hour_text: str) -> int:
    """
    Converte diferentes representações de hora em inteiro 0-23.
    Casos:
      - '0100 UTC' -> 1
      - '0200'     -> 2
      - '7' ou '7:00' ou '07:00' -> 7
      - '12:34' -> 12
      - fallback: tenta primeiros 1-2 dígitos; se falhar, 0
    """
    if not isinstance(hour_text, str):
        return 0
    s = hour_text.strip()

    m2 = _HH_0100_RE.match(s)
    if m2:
        try:
            hh = int(m2.group(1))
            return max(0, min(23, hh))
        except Exception:
            pass

    m1 = _HH_PREFIX_RE.match(s)
    if m1:
        try:
            hh = int(m1.group(1))
            return max(0, min(23, hh))
        except Exception:
            pass

    return 0

def _build_ts_from_inmet_row(row: pd.Series, date_col: str, hour_col: str) -> str:
    """
    Constrói timestamp horário 'YYYY-MM-DD HH:00:00' a partir das colunas detectadas.
    Suporta:
      - date_col ∈ {'DATA (YYYY-MM-DD)', 'Data'}
      - hour_col ∈ {'HORA (UTC)', 'Hora UTC'} com formatos 'H', 'HH', 'HH:MM', '0100 UTC'
    """
    d = str(row.get(date_col, "")).strip()
    h = str(row.get(hour_col, "")).strip()
    hh = _parse_hour_to_hh(h)
    return f"{d} {hh:02d}:00:00"

def _build_ts_from_bdq(series: pd.Series) -> pd.Series:
    """
    BDQ:
      - DATAHORA -> 'YYYY-MM-DD HH:MM:SS'
    Retorna coluna 'ts_hour' formatada 'YYYY-MM-DD HH:00:00'.
    """
    ts = pd.to_datetime(series["DATAHORA"], errors="coerce", utc=False)
    return ts.dt.strftime("%Y-%m-%d %H:00:00")

# -----------------------------------------------------------------------------
# [SEÇÃO 3] LEITURA POR ANO
# -----------------------------------------------------------------------------
def _read_inmet_year(year: int, biome: str, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Lê INMET para um ano/bioma como strings (sem converter decimais),
    normaliza 'CIDADE' e cria 'ts_hour' robusto a ambos esquemas (antigo e 2019+).
    """
    path = _inmet_consolidated_dir() / f"inmet_{year}_{biome}.csv"
    if not path.exists():
        raise FileNotFoundError(f"INMET não encontrado: {path}")

    df = pd.read_csv(path, dtype=str, encoding=encoding)
    if "CIDADE" not in df.columns:
        raise KeyError(f"Coluna 'CIDADE' ausente em {path.name}. Colunas: {list(df.columns)}")

    # cidade normalizada
    df["cidade_norm"] = df["CIDADE"].map(normalize_key)

    # detectar schema de data/hora e construir ts_hour
    date_col, hour_col = _detect_inmet_schema(df)
    df["ts_hour"] = df.apply(lambda r: _build_ts_from_inmet_row(r, date_col, hour_col), axis=1)

    # guarda quais colunas de data/hora serão úteis para ordenação posterior
    df.attrs["date_col"] = date_col
    df.attrs["hour_col"] = hour_col
    return df

def _read_bdq_year_reduced(year: int, biome: str, encoding: str = "utf-8") -> pd.DataFrame:
    """
    Lê BDQ targets do ano/bioma, reduz para uma linha por (municipio_norm, ts_hour)
    escolhendo o foco de maior FRP. Mantém:
      ['municipio_norm','ts_hour','RISCO_FOGO','FRP','FOCO_ID'].
    """
    path = _bdq_consolidated_dir() / f"bdq_targets_{year}_{biome}.csv"
    if not path.exists():
        raise FileNotFoundError(f"BDQ não encontrado: {path}")

    usecols = ["DATAHORA", "MUNICIPIO", "RISCO_FOGO", "FRP", "FOCO_ID"]
    df = pd.read_csv(path, dtype=str, encoding=encoding, usecols=usecols)

    df = df.dropna(subset=["DATAHORA", "MUNICIPIO"]).copy()
    df["municipio_norm"] = df["MUNICIPIO"].map(normalize_key)
    df["ts_hour"] = _build_ts_from_bdq(df)

    frp_num = pd.to_numeric(df["FRP"], errors="coerce")
    df["_FRP_num"] = frp_num.fillna(float("-inf"))

    idx = df.groupby(["municipio_norm", "ts_hour"])["_FRP_num"].idxmax()
    red = df.loc[idx, ["municipio_norm", "ts_hour", "RISCO_FOGO", "FRP", "FOCO_ID"]].reset_index(drop=True)
    return red

# -----------------------------------------------------------------------------
# [SEÇÃO 4] FUSÃO E ESCRITA
# -----------------------------------------------------------------------------
def _fuse_inmet_bdq_year(year: int, biome: str, out_dir: Path, encoding: str = "utf-8") -> Path:
    """
    Une um ano de INMET (filtrado por bioma) com BDQ targets:
      - join por (cidade_norm == municipio_norm) + ts_hour
      - adiciona HAS_FOCO (0/1), RISCO_FOGO, FRP, FOCO_ID
    """
    inmet = _read_inmet_year(year, biome, encoding=encoding)
    bdq   = _read_bdq_year_reduced(year, biome, encoding=encoding)

    merged = inmet.merge(
        bdq,
        left_on=["cidade_norm", "ts_hour"],
        right_on=["municipio_norm", "ts_hour"],
        how="left",
        suffixes=("", "_bdq"),
    )

    merged["HAS_FOCO"] = merged["FOCO_ID"].notna().astype("int64")
    merged = merged.drop(columns=["municipio_norm"], errors="ignore")

    # ordenação opcional: usa colunas detectadas para data/hora se existirem
    date_col = inmet.attrs.get("date_col")
    hour_col = inmet.attrs.get("hour_col")
    if date_col and hour_col and date_col in merged.columns and hour_col in merged.columns:
        merged = merged.sort_values([date_col, hour_col, "CIDADE"], kind="stable")

    out_path = out_dir / f"inmet_bdq_{year}_{biome}.csv"
    merged.to_csv(out_path, index=False, encoding=encoding)
    return out_path

# -----------------------------------------------------------------------------
# [SEÇÃO 5] PIPELINE PRINCIPAL
# -----------------------------------------------------------------------------
def build_hourly_dataset(
    years: Optional[Iterable[int]] = None,
    biome: str = "cerrado",
    overwrite: bool = False,
    encoding: str = "utf-8",
) -> Tuple[List[int], List[Path], Optional[Path]]:
    """
    Constrói dataset hora-a-hora INMET×BDQ:
      - Descobre anos comuns (>= 2003) entre INMET_{biome} e BDQ_{biome}.
      - Gera CSV por ano em data/dataset/.
      - Ao final, concatena todos em inmet_bdq_all_years_{biome}.csv.
    Retorna: (anos_processados, [paths_por_ano], path_consolidado)
    """
    log = get_logger("dataset.build", kind="dataset", per_run_file=True)
    _ = loadConfig()

    inmet_years = _list_inmet_years_for_biome(biome)
    bdq_years   = _list_bdq_years_for_biome(biome)

    candidate_years = sorted(set(inmet_years).intersection(bdq_years))
    candidate_years = [y for y in candidate_years if y >= 2003]

    if years:
        wanted = sorted({int(y) for y in years})
        years_to_run = [y for y in wanted if y in candidate_years]
    else:
        years_to_run = candidate_years

    if not years_to_run:
        raise RuntimeError(
            f"Nenhum ano elegível. INMET({biome})={inmet_years}  BDQ({biome})={bdq_years}  "
            f"Interseção>=2003={candidate_years}"
        )

    out_dir = _dataset_dir()
    written: List[Path] = []
    for y in years_to_run:
        out_y = out_dir / f"inmet_bdq_{y}_{biome}.csv"
        if out_y.exists() and not overwrite:
            log.info(f"[SKIP] {out_y.name} já existe.")
            written.append(out_y)
            continue

        log.info(f"[YEAR] {y} × {biome} ...")
        path = _fuse_inmet_bdq_year(y, biome, out_dir, encoding=encoding)
        log.info(f"[WRITE] {path}")
        written.append(path)

    final_path = out_dir / f"inmet_bdq_all_years_{biome}.csv"
    if (not final_path.exists()) or overwrite:
        frames = [pd.read_csv(p, dtype=str, encoding=encoding) for p in written]
        all_df = pd.concat(frames, ignore_index=True)
        all_df.to_csv(final_path, index=False, encoding=encoding)
        log.info(f"[WRITE] {final_path}")
    else:
        log.info(f"[SKIP] {final_path.name} já existe.")

    return years_to_run, written, final_path

# -----------------------------------------------------------------------------
# [SEÇÃO 6] CLI
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(
        description="Constrói dataset INMET×BDQueimadas hora-a-hora (ano a ano, a partir de 2003)."
    )
    p.add_argument("--biome", type=str, default="cerrado", help="Bioma alvo (default: 'cerrado').")
    p.add_argument("--years", nargs="*", type=int, default=None, help="Lista de anos (ex.: --years 2003 2004).")
    p.add_argument("--overwrite", action="store_true", help="Sobrescreve saídas existentes.")
    p.add_argument("--encoding", type=str, default="utf-8", help="Encoding de I/O (default: utf-8).")
    args = p.parse_args()

    log = get_logger("dataset.build", kind="dataset", per_run_file=True)
    try:
        yrs, per_year, final = build_hourly_dataset(
            years=args.years,
            biome=args.biome,
            overwrite=args.overwrite,
            encoding=args.encoding,
        )
        log.info(f"[DONE] anos={yrs}  arquivos_por_ano={len(per_year)}  final={final}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na construção do dataset: {e}")
