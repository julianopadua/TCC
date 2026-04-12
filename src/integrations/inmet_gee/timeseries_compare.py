# src/integrations/inmet_gee/timeseries_compare.py
# =============================================================================
# SÉRIES TEMPORAIS E vs F — Leitura alinhada, acumulados, export anual
# Lê parquets *_calculated das duas bases, um ano por vez, sem manter tudo
# em memória. Grava Parquet + CSV de amostra de forma atômica.
# =============================================================================
from __future__ import annotations

import gc
import logging
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .config import PipelineConfig, TimeseriesConfig
from .checkpoint import (
    load_ts_state, init_ts_state, mark_ts_year_done, mark_ts_year_failed,
    save_ts_state,
)
from .io_parquet import read_parquet_columns, parquet_path_for_year

COL_TS = "ts_hour"
COL_DATA = "DATA (YYYY-MM-DD)"
COL_HORA = "HORA (UTC)"
COL_CIDADE_NORM = "cidade_norm"
COL_ANO = "ANO"


def _build_ts_hour(df: pd.DataFrame) -> pd.Series:
    """Reconstrói ts_hour a partir de DATA+HORA se a coluna não existir."""
    if COL_TS in df.columns:
        return pd.to_datetime(df[COL_TS], errors="coerce")
    return pd.to_datetime(
        df[COL_DATA].astype(str) + " " + df[COL_HORA].astype(str),
        format="%Y-%m-%d %H:%M",
        errors="coerce",
    )


def _pick_sample_cities(
    dir_e: Path,
    dir_f: Path,
    variables: List[str],
    ref_year: int,
    n: int,
    log: logging.Logger,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
) -> List[str]:
    """
    Seleciona as N cidades com menor % de NaN nas variáveis de interesse,
    calculado sobre a interseção E∩F no ano de referência.
    """
    cols_base = [COL_CIDADE_NORM, COL_TS, COL_DATA, COL_HORA]
    # Inclui apenas variáveis que podem não existir — tratado em read_parquet_columns
    cols_var = list(variables)

    path_e = parquet_path_for_year(dir_e, ref_year)
    path_f = parquet_path_for_year(dir_f, ref_year)

    if not path_e.exists() or not path_f.exists():
        log.warning(
            "Seleção automática de cidades: parquet de %d ausente em E ou F. "
            "Retornando lista vazia — pipeline usará todas as cidades.",
            ref_year,
        )
        return []

    df_e = read_parquet_columns(
        path_e, cols_base + cols_var, log, max_attempts, base_delay, max_delay
    )
    df_f = read_parquet_columns(
        path_f, cols_base + cols_var, log, max_attempts, base_delay, max_delay
    )

    if df_e is None or df_f is None:
        log.warning("Falha ao ler parquets para seleção de cidades. Lista vazia.")
        return []

    cities_e = set(df_e[COL_CIDADE_NORM].dropna().unique())
    cities_f = set(df_f[COL_CIDADE_NORM].dropna().unique())
    common = cities_e & cities_f

    scores = []
    for city in sorted(common):
        sub_e = df_e[df_e[COL_CIDADE_NORM] == city]
        sub_f = df_f[df_f[COL_CIDADE_NORM] == city]
        avail_vars = [v for v in cols_var if v in sub_e.columns and v in sub_f.columns]
        if not avail_vars:
            continue
        nan_e = sub_e[avail_vars].isna().mean().mean()
        nan_f = sub_f[avail_vars].isna().mean().mean()
        scores.append((city, (nan_e + nan_f) / 2))

    scores.sort(key=lambda x: x[1])
    chosen = [c for c, _ in scores[:n]]
    log.info(
        "Seleção automática de cidades (ano ref=%d, N=%d): %s "
        "(política: menor %% NaN médio E∩F).",
        ref_year, n, chosen,
    )

    del df_e, df_f
    gc.collect()
    return chosen


def _resolve_scenario_dir(cfg: PipelineConfig, scenario_key: str) -> Path:
    folder = cfg.modeling_scenarios.get(scenario_key, "")
    if not folder:
        raise ValueError(
            f"Cenário '{scenario_key}' não encontrado em modeling_scenarios no config.yaml."
        )
    return cfg.modeling_dir / folder


def _atomic_write_parquet(path: Path, df: pd.DataFrame) -> None:
    tmp = path.with_suffix(".tmp.parquet")
    df.to_parquet(tmp, index=False, compression="snappy")
    if path.exists():
        bak = path.with_suffix(".bak.parquet")
        path.replace(bak)
    os.replace(tmp, path)


def _atomic_write_csv(path: Path, df: pd.DataFrame, encoding: str = "utf-8") -> None:
    tmp = path.with_suffix(".tmp.csv")
    df.to_csv(tmp, index=False, encoding=encoding)
    if path.exists():
        bak = path.with_suffix(".bak.csv")
        path.replace(bak)
    os.replace(tmp, path)


def _process_year(
    year: int,
    dir_e: Path,
    dir_f: Path,
    ts_cfg: TimeseriesConfig,
    sample_cities: List[str],
    out_dir: Path,
    log: logging.Logger,
    max_attempts: int,
    base_delay: float,
    max_delay: float,
    cumsum_carry: Dict[str, float],
) -> Tuple[bool, Dict[str, float]]:
    """
    Processa um único ano: lê E e F, faz join, computa cumsum de precipitação,
    grava Parquet + CSV. Retorna (success, cumsum_carry_atualizado).
    """
    path_e = parquet_path_for_year(dir_e, year)
    path_f = parquet_path_for_year(dir_f, year)

    for label, path in [("E", path_e), ("F", path_f)]:
        if not path.exists():
            log.error(
                "Série temporal: parquet da base %s ausente para o ano %d: '%s'. "
                "Ano ignorado neste sub-fluxo.",
                label, year, path,
            )
            return False, cumsum_carry

    cols_meta = [COL_CIDADE_NORM, COL_TS, COL_DATA, COL_HORA]
    cols_vars = list(ts_cfg.variables)
    cols_read = list(dict.fromkeys(cols_meta + cols_vars))

    log.info("Séries %d: lendo base E (%s)...", year, path_e.name)
    df_e = read_parquet_columns(
        path_e, cols_read, log, max_attempts, base_delay, max_delay, cast_float32=True
    )
    log.info("Séries %d: lendo base F (%s)...", year, path_f.name)
    df_f = read_parquet_columns(
        path_f, cols_read, log, max_attempts, base_delay, max_delay, cast_float32=True
    )

    if df_e is None or df_f is None:
        return False, cumsum_carry

    # Construir ts_hour
    for df, label in [(df_e, "E"), (df_f, "F")]:
        df["_ts"] = _build_ts_hour(df)
        n_bad = df["_ts"].isna().sum()
        if n_bad:
            log.warning(
                "Séries %d | Base %s: %d linhas com ts_hour inválido — serão descartadas.",
                year, label, n_bad,
            )

    df_e = df_e.dropna(subset=["_ts"])
    df_f = df_f.dropna(subset=["_ts"])

    # Filtrar cidades amostra precocemente
    if sample_cities:
        df_e = df_e[df_e[COL_CIDADE_NORM].isin(sample_cities)]
        df_f = df_f[df_f[COL_CIDADE_NORM].isin(sample_cities)]

    if df_e.empty or df_f.empty:
        log.warning(
            "Séries %d: DataFrame vazio após filtragem de cidades amostra. "
            "Ano ignorado.",
            year,
        )
        return False, cumsum_carry

    # Inner join em (cidade_norm, _ts)
    merge_keys = [COL_CIDADE_NORM, "_ts"]
    vars_in_e = [v for v in cols_vars if v in df_e.columns]
    vars_in_f = [v for v in cols_vars if v in df_f.columns]
    common_vars = sorted(set(vars_in_e) & set(vars_in_f))

    df_merged = pd.merge(
        df_e[merge_keys + vars_in_e].rename(columns={v: f"{v}__E" for v in vars_in_e}),
        df_f[merge_keys + vars_in_f].rename(columns={v: f"{v}__F" for v in vars_in_f}),
        on=merge_keys, how="inner",
    )

    n_only_e = len(df_e) - len(df_merged)
    n_only_f = len(df_f) - len(df_merged)
    if n_only_e or n_only_f:
        log.warning(
            "Séries %d: join inner E∩F descartou %d linhas só em E e %d só em F.",
            year, n_only_e, n_only_f,
        )

    df_merged = df_merged.sort_values([COL_CIDADE_NORM, "_ts"]).reset_index(drop=True)
    df_merged["ts_hour"] = df_merged["_ts"]
    df_merged["ano"] = year
    df_merged = df_merged.drop(columns=["_ts"])

    # Acumulado de precipitação
    PRECIP_COL = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
    if ts_cfg.cumsum_precip and PRECIP_COL in common_vars:
        for suffix in ["E", "F"]:
            col = f"{PRECIP_COL}__{suffix}"
            cum_col = f"precip_cumsum_{suffix}"
            if ts_cfg.cumsum_multiyear:
                # Acumulado contínuo — usa estado do ano anterior por cidade
                results = []
                for city, grp in df_merged.groupby(COL_CIDADE_NORM, sort=False):
                    carry = cumsum_carry.get(f"{city}__{suffix}", 0.0)
                    vals = grp[col].fillna(0).values.astype("float64")
                    cum = np.cumsum(vals) + carry
                    cumsum_carry[f"{city}__{suffix}"] = float(cum[-1]) if len(cum) else carry
                    results.append(pd.Series(cum, index=grp.index, name=cum_col))
                df_merged[cum_col] = pd.concat(results)
            else:
                # Acumulado intra-ano (padrão)
                df_merged[cum_col] = (
                    df_merged.groupby(COL_CIDADE_NORM)[col]
                    .transform(lambda s: s.fillna(0).cumsum())
                    .astype("float32")
                )
        log.info(
            "Séries %d: acumulado de precipitação calculado (%s).",
            year, "multi-anual" if ts_cfg.cumsum_multiyear else "intra-ano",
        )

    # Export Parquet
    out_year_dir = out_dir / "yearly"
    out_year_dir.mkdir(parents=True, exist_ok=True)
    pq_path = out_year_dir / f"ts_compare_{year}.parquet"
    _atomic_write_parquet(pq_path, df_merged)
    log.info("Séries %d: Parquet gravado em '%s' (%d linhas).", year, pq_path, len(df_merged))

    # CSV resumido das cidades amostra
    if ts_cfg.export_sample_csv:
        csv_path = out_year_dir / f"ts_compare_{year}_sample.csv"
        df_sample = df_merged if sample_cities else df_merged
        _atomic_write_csv(csv_path, df_sample)
        log.info("Séries %d: CSV de amostra gravado em '%s'.", year, csv_path)

    del df_e, df_f, df_merged
    gc.collect()
    return True, cumsum_carry


class TimeseriesComparator:
    """
    Orquestra a extração de séries temporais E vs F para todos os anos disponíveis,
    com checkpoint dedicado (timeseries_state.json) e retomada em caso de falha.
    """

    def __init__(self, cfg: PipelineConfig, log: logging.Logger):
        self.cfg = cfg
        self.ts_cfg = cfg.timeseries
        self.log = log

        self.dir_e = _resolve_scenario_dir(cfg, self.ts_cfg.scenarios["E"])
        self.dir_f = _resolve_scenario_dir(cfg, self.ts_cfg.scenarios["F"])
        self.out_dir = cfg.output_dir / "outputs" / "timeseries"
        self.cp_path = cfg.output_dir / "checkpoints" / "timeseries_state.json"

    def _resolve_years(self, state: dict, force_years: Optional[List[int]]) -> List[int]:
        from .io_parquet import discover_years
        avail_e = set(discover_years(self.dir_e))
        avail_f = set(discover_years(self.dir_f))
        common = sorted(avail_e & avail_f)
        if not common:
            self.log.error(
                "Séries: nenhum ano encontrado na interseção E∩F. "
                "E: %s | F: %s", sorted(avail_e), sorted(avail_f),
            )
            return []
        if force_years:
            return [y for y in sorted(force_years) if y in set(common)]
        done = set(state.get("completed_years_ts", []))
        return [y for y in common if y not in done]

    def _resolve_ref_year(self, years_available: List[int]) -> int:
        raw = self.ts_cfg.reference_year_for_city_pick
        if raw == "last":
            return max(years_available) if years_available else 2019
        try:
            return int(raw)
        except (ValueError, TypeError):
            return max(years_available) if years_available else 2019

    def run(self, force_years: Optional[List[int]] = None) -> None:
        if not self.ts_cfg.enabled:
            self.log.info("Séries temporais desabilitadas na config (timeseries.enabled=false).")
            return

        state = load_ts_state(self.cp_path)
        if not state:
            state = init_ts_state()

        years_to_process = self._resolve_years(state, force_years)
        if not years_to_process:
            self.log.info("Séries temporais: nenhum ano a processar (todos já concluídos).")
            return

        self.log.info(
            "Séries temporais: %d ano(s) a processar — %s.",
            len(years_to_process), years_to_process,
        )

        # Resolver cidades amostra
        sample_cities = list(self.ts_cfg.sample_cities)
        if not sample_cities:
            from .io_parquet import discover_years as _disc
            avail = sorted(set(_disc(self.dir_e)) & set(_disc(self.dir_f)))
            ref_year = self._resolve_ref_year(avail)
            sample_cities = _pick_sample_cities(
                self.dir_e, self.dir_f, self.ts_cfg.variables, ref_year,
                self.ts_cfg.auto_sample_n, self.log,
                self.cfg.retry_max_attempts, self.cfg.retry_base_delay_s, self.cfg.retry_max_delay_s,
            )
            state["sample_cities"] = sample_cities

        self.log.info("Cidades amostra para gráficos: %s.", sample_cities)

        # Carry de cumsum multi-anual
        cumsum_carry: Dict[str, float] = state.get("cumsum_carry", {})

        for year in years_to_process:
            self.log.info("=== Séries: iniciando ano %d ===", year)
            try:
                success, cumsum_carry = _process_year(
                    year=year,
                    dir_e=self.dir_e,
                    dir_f=self.dir_f,
                    ts_cfg=self.ts_cfg,
                    sample_cities=sample_cities,
                    out_dir=self.out_dir,
                    log=self.log,
                    max_attempts=self.cfg.retry_max_attempts,
                    base_delay=self.cfg.retry_base_delay_s,
                    max_delay=self.cfg.retry_max_delay_s,
                    cumsum_carry=cumsum_carry,
                )
                if success:
                    state = mark_ts_year_done(state, year)
                    state["cumsum_carry"] = cumsum_carry
                else:
                    state = mark_ts_year_failed(
                        state, year, "ProcessingError",
                        "Falha no processamento do ano — ver log acima.",
                    )
            except Exception as exc:
                self.log.error(
                    "Exceção não esperada ao processar séries do ano %d: %s",
                    year, exc, exc_info=True,
                )
                state = mark_ts_year_failed(state, year, type(exc).__name__, str(exc))
            finally:
                save_ts_state(state, self.cp_path)

        self.log.info(
            "Séries temporais concluídas. Anos processados: %d | Falhas: %d.",
            len(state.get("completed_years_ts", [])),
            len(state.get("failed_years_ts", {})),
        )
