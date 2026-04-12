# src/integrations/inmet_gee/run_pipeline.py
# =============================================================================
# ORQUESTRADOR PRINCIPAL — Pipeline INMET-GEE
# Uso:
#   python -m src.integrations.inmet_gee.run_pipeline [--options]
#   python src/integrations/inmet_gee/run_pipeline.py [--options]
#
# Flags:
#   --skip-gee            Pula validação GEE (útil sem credenciais)
#   --skip-timeseries     Pula extração de séries temporais E vs F
#   --skip-plots          Pula geração de gráficos
#   --only-timeseries     Executa apenas séries + gráficos (pula metadados GEE)
#   --only-plots          Gera apenas gráficos a partir de Parquets já existentes
#   --only-metadata-gee   Apenas metadados + deriva + GEE (sem séries temporais nem plots)
#   --force-year YYYY     Reprocessa o ano indicado mesmo que já esteja no checkpoint
#   --retry-failed        Reprocessa anos que falharam anteriormente
#   --years Y1 Y2 ...     Limita o processamento aos anos listados
# =============================================================================
from __future__ import annotations

import argparse
import gc
import sys
from pathlib import Path
from typing import List, Optional

# Boilerplate de path para execução direta
_here = Path(__file__).resolve()
_project_root = _here.parents[3]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils
from .config import load_pipeline_config
from .checkpoint import (
    load_state, init_state, mark_year_started, mark_year_done,
    mark_year_failed, mark_gee_key_done, save_state,
)
from .io_parquet import discover_years, parquet_path_for_year, read_parquet_columns
from .stations import aggregate_station_year
from .spatial_drift import SpatialDriftDetector
from .gee_client import GeeSampler
from .csv_writers import append_station_year, append_drift_events, append_gee_validations
from .timeseries_compare import TimeseriesComparator
from .plots import PlotRunner


def _parse_args(argv=None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Pipeline INMET-GEE: metadados de estações, deriva espacial, séries E vs F."
    )
    p.add_argument("--skip-gee", action="store_true", help="Pular validação GEE")
    p.add_argument("--skip-timeseries", action="store_true", help="Pular séries temporais")
    p.add_argument("--skip-plots", action="store_true", help="Pular geração de gráficos")
    p.add_argument("--only-timeseries", action="store_true", help="Apenas séries + gráficos")
    p.add_argument("--only-plots", action="store_true", help="Apenas gráficos (Parquets já existentes)")
    p.add_argument(
        "--only-metadata-gee",
        action="store_true",
        help="Apenas metadados, deriva espacial e validação GEE (pula séries E×F e gráficos).",
    )
    p.add_argument("--force-year", type=int, default=None, metavar="YYYY",
                   help="Reprocessar ano específico mesmo que concluído")
    p.add_argument("--retry-failed", action="store_true",
                   help="Reprocessar anos que falharam anteriormente")
    p.add_argument("--years", type=int, nargs="+", metavar="Y",
                   help="Limitar processamento aos anos listados")
    return p.parse_args(argv)


def _resolve_scenario_dir(cfg, scenario_key: str) -> Path:
    folder = cfg.modeling_scenarios.get(scenario_key, "")
    if not folder:
        raise ValueError(
            f"Cenário '{scenario_key}' ausente em modeling_scenarios no config.yaml."
        )
    return cfg.modeling_dir / folder


def run_metadata_pipeline(cfg, args, log) -> None:
    """
    Loop principal: extração de metadados de estações, deriva espacial, validação GEE.
    """
    station_dir = _resolve_scenario_dir(cfg, cfg.station_source_scenario)
    if not station_dir.exists():
        log.critical(
            "Diretório do cenário fonte não encontrado: '%s'. "
            "Verifique 'station_source_scenario' e 'modeling_scenarios' no config.yaml.",
            station_dir,
        )
        sys.exit(1)

    out_csv_dir = cfg.output_dir / "outputs" / "csv"
    cp_path = cfg.output_dir / "checkpoints" / "run_state.json"
    out_csv_dir.mkdir(parents=True, exist_ok=True)

    path_locations = out_csv_dir / "station_year_locations.csv"
    path_drift = out_csv_dir / "spatial_drift_events.csv"
    path_gee = out_csv_dir / "gee_point_validation.csv"

    # Checkpoint
    state = load_state(cp_path)
    if not state:
        state = init_state(
            input_scenario=cfg.station_source_scenario,
            input_root=str(station_dir),
        )
    else:
        log.info(
            "Checkpoint carregado. Anos concluídos anteriormente: %s.",
            state.get("completed_years", []),
        )

    # Restaurar estado de deriva espacial
    drift_detector = SpatialDriftDetector(
        jitter_max_m=cfg.coordinate_jitter_max_m,
        drift_alert_m=cfg.drift_alert_min_m,
        log=log,
    )
    drift_detector.restore_state(state)

    # Descoberta de anos
    all_years = discover_years(station_dir)
    if not all_years:
        log.critical(
            "Nenhum parquet encontrado em '%s'. "
            "Verifique o diretório e o padrão inmet_bdq_{YYYY}_cerrado.parquet.",
            station_dir,
        )
        sys.exit(1)

    if args.years:
        all_years = [y for y in args.years if y in set(all_years)]

    completed = set(state.get("completed_years", []))
    failed = set(state.get("failed_years", {}).keys())

    years_to_process = []
    for y in all_years:
        if args.force_year and y == args.force_year:
            years_to_process.append(y)
        elif args.retry_failed and str(y) in failed:
            years_to_process.append(y)
        elif y not in completed:
            years_to_process.append(y)

    log.info(
        "Anos disponíveis: %s | A processar: %s | Já concluídos: %d.",
        all_years, years_to_process, len(completed),
    )

    if not years_to_process:
        log.info("Nenhum ano a processar. Todos já estão no checkpoint.")
        return

    # GEE
    gee_client: Optional[GeeSampler] = None
    if not args.skip_gee:
        gee_client = GeeSampler(
            cfg=cfg.gee,
            log=log,
            max_attempts=cfg.retry_max_attempts,
            base_delay=cfg.retry_base_delay_s,
            max_delay=cfg.retry_max_delay_s,
        )
        gee_ok = gee_client.initialize()
        if not gee_ok:
            log.warning(
                "GEE não inicializado. Validação de pontos será ignorada. "
                "Para suprimir este aviso, use --skip-gee."
            )
            gee_client = None

    gee_completed_keys = set(state.get("gee", {}).get("completed_keys", []))

    # Colunas mínimas necessárias para metadados
    id_cols = cfg.station_id_columns
    meta_cols = id_cols + ["LATITUDE", "LONGITUDE", "ANO", "CIDADE", "cidade_norm"]

    for year in years_to_process:
        pq_path = parquet_path_for_year(station_dir, year)
        log.info(
            "=== Processando ano %d | Arquivo: %s ===",
            year, pq_path.name,
        )

        state = mark_year_started(state, year, pq_path)
        save_state(state, cp_path)

        try:
            df = read_parquet_columns(
                pq_path, meta_cols, log,
                max_attempts=cfg.retry_max_attempts,
                base_delay=cfg.retry_base_delay_s,
                max_delay=cfg.retry_max_delay_s,
            )
            if df is None:
                state = mark_year_failed(state, year, "IOError", "Falha na leitura do parquet.")
                save_state(state, cp_path)
                continue

            # Agrega metadados por (station_uid, ano)
            station_agg = aggregate_station_year(
                df=df,
                id_columns=id_cols,
                year=year,
                log=log,
                jitter_max_m=cfg.coordinate_jitter_max_m,
            )
            del df
            gc.collect()

            if station_agg.empty:
                state = mark_year_failed(
                    state, year, "EmptyAggregation",
                    "Agregação retornou DataFrame vazio após sanitização.",
                )
                save_state(state, cp_path)
                continue

            # Deriva espacial
            drift_detector.process_year(station_agg)
            drift_events_df = drift_detector.get_events_df()

            # Mapear geo_versions atuais
            geo_versions = {
                uid: drift_detector.get_geo_version(uid)
                for uid in station_agg["station_uid"]
            }

            # Salvar CSVs
            append_station_year(path_locations, station_agg, geo_versions)
            if not drift_events_df.empty:
                append_drift_events(path_drift, drift_events_df)

            # Validação GEE
            gee_results = []
            if gee_client is not None:
                strategy = cfg.gee.validation_strategy
                for _, row in station_agg.iterrows():
                    uid = row["station_uid"]
                    geo_v = geo_versions.get(uid, 1)
                    gee_key = f"{uid}|{geo_v}"

                    if strategy == "per_geo_version" and gee_key in gee_completed_keys:
                        log.info(
                            "Estação '%s' | Geo-versão %d: validação GEE já realizada "
                            "(reutilizando resultado anterior).",
                            uid, geo_v,
                        )
                        continue

                    result = gee_client.validate_point(
                        station_uid=uid,
                        lat=float(row["lat_median"]),
                        lon=float(row["lon_median"]),
                        geo_version=geo_v,
                        year=year,
                    )
                    gee_results.append(result)

                    if result["status"] == "OK":
                        gee_completed_keys.add(gee_key)
                        state = mark_gee_key_done(state, gee_key)

                if gee_results:
                    append_gee_validations(path_gee, gee_results)

            # Atualiza estado de deriva no checkpoint
            drift_dump = drift_detector.dump_state()
            state.update(drift_dump)
            state = mark_year_done(state, year)
            save_state(state, cp_path)

            log.info(
                "Ano %d concluído. Estações: %d | Eventos de deriva: %d | "
                "Validações GEE: %d.",
                year, len(station_agg), len(drift_events_df),
                len(gee_results),
            )

        except Exception as exc:
            log.error(
                "Exceção não esperada ao processar ano %d: %s",
                year, exc, exc_info=True,
            )
            state = mark_year_failed(state, year, type(exc).__name__, str(exc))
            save_state(state, cp_path)

    # Resumo final
    completed_final = state.get("completed_years", [])
    failed_final = state.get("failed_years", {})
    log.info(
        "=== Pipeline de metadados concluído. "
        "Anos processados com sucesso: %d | Falhas: %d | "
        "Eventos de deriva espacial acumulados: %d ===",
        len(completed_final), len(failed_final),
        len(drift_detector.drift_events),
    )


def main(argv=None) -> None:
    args = _parse_args(argv)
    if args.only_metadata_gee:
        args.skip_timeseries = True
        args.skip_plots = True

    cfg = load_pipeline_config()
    utils.ensure_dir(cfg.output_dir)
    utils.ensure_dir(cfg.output_dir / "checkpoints")
    utils.ensure_dir(cfg.output_dir / "outputs" / "csv")
    utils.ensure_dir(cfg.output_dir / "outputs" / "timeseries" / "yearly")
    utils.ensure_dir(cfg.output_dir / "outputs" / "plots")

    log = utils.get_logger("inmet_gee", kind="inmet_gee", per_run_file=True)
    log.info(
        "Pipeline INMET-GEE iniciado. Cenário fonte: '%s'. "
        "Output: '%s'.",
        cfg.station_source_scenario, cfg.output_dir,
    )
    if args.only_metadata_gee:
        log.info(
            "Modo --only-metadata-gee: séries temporais e gráficos desativados para esta execução."
        )

    if args.only_plots:
        log.info("Modo --only-plots: gerando apenas gráficos.")
        PlotRunner(cfg, log).run()
        return

    if not args.only_timeseries:
        run_metadata_pipeline(cfg, args, log)

    if not args.skip_timeseries:
        force_years = [args.force_year] if args.force_year else (args.years or None)
        TimeseriesComparator(cfg, log).run(force_years=force_years)

    if not args.skip_plots:
        PlotRunner(cfg, log).run()

    log.info("Pipeline INMET-GEE finalizado.")


if __name__ == "__main__":
    main()
