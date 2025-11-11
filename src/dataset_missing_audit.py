# src/dataset_missing_audit.py
# =============================================================================
# EDA DATASET — AUDITORIA DE DADOS FALTANTES (INMET + BDQueimadas, CERRADO)
# =============================================================================
# Objetivo:
# - Analisar, por ano e por coluna, a presença de dados faltantes nos arquivos
#   consolidados inmet_bdq_{ANO}_cerrado.csv.
# - Considerar como faltante:
#     * NaN / null
#     * strings vazias (após strip)
#     * códigos especiais negativos como -999 e -9999
# - Destacar:
#     * impacto em exemplos com foco (HAS_FOCO == 1)
#     * impacto global por ano
#     * colunas com missing extremo
#     * anos problemáticos em variáveis de entrada relevantes
# - Diferenciar variáveis alvo (RISCO_FOGO, FRP, FOCO_ID) de variáveis explicativas:
#     * missing nas alvos é esperado e não define ano ruim
# - Harmonizar nomes de colunas equivalentes (ex.: radiação global Kj/KJ).
# - Persistir resultados em reports/eda/dataset.
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from utils import loadConfig, get_logger, get_path, ensure_dir


# -----------------------------------------------------------------------------
# [SEÇÃO 1] CONFIG, LOG E CONSTANTES
# -----------------------------------------------------------------------------
cfg = loadConfig()

# Raiz do projeto
try:
    PROJECT_ROOT: Path = get_path("paths", "root")
except Exception:
    PROJECT_ROOT = Path(__file__).resolve().parents[1]

# Diretório dos CSVs consolidados (dataset)
try:
    DATASET_DIR: Path = get_path("paths", "data", "dataset")
except Exception:
    DATASET_DIR = (PROJECT_ROOT / "data" / "dataset").resolve()

# Diretório base de reports/eda
try:
    REPORTS_EDA_DIR: Path = get_path("paths", "reports", "eda")
    REPORTS_EDA_DIR = ensure_dir(REPORTS_EDA_DIR)
except Exception:
    REPORTS_EDA_DIR = ensure_dir(PROJECT_ROOT / "reports" / "eda")

# Diretório específico para EDA relacionada a dataset
REPORTS_DATASET_DIR: Path = ensure_dir(REPORTS_EDA_DIR / "dataset")

# Logger dedicado
log = get_logger("eda.missing_dataset", kind="eda", per_run_file=True)

# Padrão dos arquivos ano a ano
FILENAME_PATTERN = "inmet_bdq_*_cerrado.csv"

# Códigos especiais tratados como faltantes
MISSING_CODES = {-999, -9999}
MISSING_CODES_STR = {str(v) for v in MISSING_CODES}

# Colunas que não entram na auditoria numérica (IDs, datas, textos)
EXCLUDE_NON_NUMERIC = {
    "DATA (YYYY-MM-DD)",
    "HORA (UTC)",
    "CIDADE",
    "cidade_norm",
    "ts_hour",
}

# Colunas alvo (labels / auxiliares de rótulo)
TARGET_COLS = {
    "RISCO_FOGO",
    "FRP",
    "FOCO_ID",
}

# Colunas críticas de features (para classificar anos bons/ruins)
# Importante: não inclui RISCO_FOGO, FRP, FOCO_ID.
CRITICAL_COLS = [
    "HAS_FOCO",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "TEMPERATURA DO PONTO DE ORVALHO (°C)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
    "VENTO, VELOCIDADE HORARIA (m/s)",
    "RADIACAO GLOBAL (KJ/m²)",  # após harmonização de nomes
]

# Limiares padrão
COL_MISSING_THRESHOLD = 0.40       # 40% global na coluna
YEAR_CRITICAL_THRESHOLD = 0.20     # 20% em qualquer coluna crítica no ano

# Validação imediata
if not DATASET_DIR.exists():
    raise FileNotFoundError(
        f"Diretório de datasets não encontrado.\nTentado: {DATASET_DIR}"
    )


# -----------------------------------------------------------------------------
# [SEÇÃO 2] ESTRUTURAS DE DADOS
# -----------------------------------------------------------------------------
@dataclass
class YearMissingSummary:
    """
    Resumo compacto, por ano, da presença de dados faltantes.
    """
    year: int
    rows_total: int
    focos_total: int
    nonfocos_total: int

    rows_with_any_missing: int
    rows_with_any_missing_focus: int
    rows_with_any_missing_nonfocus: int

    pct_rows_with_any_missing: float
    pct_rows_with_missing_focus: float
    pct_rows_with_missing_nonfocus: float


# -----------------------------------------------------------------------------
# [SEÇÃO 3] CLASSE PRINCIPAL DE AUDITORIA
# -----------------------------------------------------------------------------
@dataclass
class DatasetMissingAnalyzer:
    dataset_dir: Path
    reports_dir: Path
    file_pattern: str = FILENAME_PATTERN
    missing_codes: set[int] = field(default_factory=lambda: MISSING_CODES.copy())
    exclude: set[str] = field(default_factory=lambda: EXCLUDE_NON_NUMERIC.copy())

    def __post_init__(self) -> None:
        self.missing_codes_str = {str(v) for v in self.missing_codes}

    # ------------------------------
    # Descoberta de arquivos
    # ------------------------------
    def discover_year_files(self) -> Dict[int, Path]:
        """
        Localiza arquivos ano a ano pelo padrão informado e extrai o ano do nome.
        Retorna {ano: caminho}.
        """
        mapping: Dict[int, Path] = {}
        for fp in self.dataset_dir.glob(self.file_pattern):
            digits = "".join(ch if ch.isdigit() else " " for ch in fp.stem).split()
            year = None
            for token in digits:
                if len(token) == 4:
                    try:
                        y = int(token)
                    except ValueError:
                        continue
                    if 1900 <= y <= 2100:
                        year = y
                        break
            if year is not None:
                mapping[year] = fp

        mapping = dict(sorted(mapping.items()))
        if not mapping:
            raise FileNotFoundError(
                f"Nenhum CSV encontrado em {self.dataset_dir} com padrão {self.file_pattern}"
            )

        log.info(f"[DISCOVER] {len(mapping)} arquivos anuais detectados.")
        return mapping

    # ------------------------------
    # Harmonização de colunas
    # ------------------------------
    def harmonize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza inconsistências de nomenclatura entre anos.
        Exemplo principal: unificar radiação global Kj/m² e KJ/m².
        """
        df = df.copy()

        old = "RADIACAO GLOBAL (Kj/m²)"
        new = "RADIACAO GLOBAL (KJ/m²)"

        if old in df.columns and new in df.columns:
            # Preenche o novo com valores do antigo onde estiver nulo.
            filled = df[new].combine_first(df[old])
            # Loga se houver conflito real de valores diferentes.
            conflict_mask = (
                df[old].notna()
                & df[new].notna()
                & (df[old] != df[new])
            )
            if conflict_mask.any():
                log.warning(
                    f"[HARMONIZE] Conflitos entre '{old}' e '{new}' em {int(conflict_mask.sum())} linhas; "
                    f"mantendo valores de '{new}'."
                )
            df[new] = filled
            df = df.drop(columns=[old])

        elif old in df.columns:
            # Só existe a versão com Kj: renomeia para a versão canônica.
            df = df.rename(columns={old: new})

        return df

    # ------------------------------
    # Leitura
    # ------------------------------
    def read_year_csv(self, fp: Path) -> pd.DataFrame:
        """
        Lê CSV consolidado e aplica harmonização de nomes.
        """
        df = pd.read_csv(
            fp,
            sep=",",
            decimal=",",
            encoding="utf-8",
            low_memory=False,
        )
        df = self.harmonize_columns(df)
        return df

    # ------------------------------
    # Construção de matriz de missing
    # ------------------------------
    def build_missing_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Retorna um DataFrame booleano indicando, para cada coluna (exceto excluídas),
        se a célula é faltante segundo as regras:

        - NaN / null
        - string vazia (após strip)
        - códigos especiais como -999 e -9999
        """
        cols = [c for c in df.columns if c not in self.exclude]
        missing = pd.DataFrame(index=df.index)

        for c in cols:
            s = df[c]

            if pd.api.types.is_bool_dtype(s):
                mask = s.isna()

            elif pd.api.types.is_numeric_dtype(s):
                mask = s.isna() | s.isin(self.missing_codes)

            else:
                s_str = s.astype("string")
                mask = s_str.isna()
                mask |= s_str.str.strip().eq("")
                mask |= s_str.isin(self.missing_codes_str)

            missing[c] = mask.fillna(False)

        return missing

    # ------------------------------
    # Resumo por ano
    # ------------------------------
    def compute_year_summary(self, df: pd.DataFrame, year: int) -> YearMissingSummary:
        if "HAS_FOCO" not in df.columns:
            raise KeyError(f"Coluna HAS_FOCO não encontrada no ano {year}")

        missing = self.build_missing_matrix(df)
        any_missing = missing.any(axis=1)
        foco_mask = df["HAS_FOCO"] == 1

        rows_total = int(len(df))
        focos_total = int(foco_mask.sum())
        nonfocos_total = rows_total - focos_total

        rows_with_any_missing = int(any_missing.sum())
        rows_with_any_missing_focus = int((any_missing & foco_mask).sum())
        rows_with_any_missing_nonfocus = (
            rows_with_any_missing - rows_with_any_missing_focus
        )

        pct_rows_with_any_missing = (
            rows_with_any_missing / rows_total if rows_total else 0.0
        )
        pct_rows_with_missing_focus = (
            rows_with_any_missing_focus / focos_total if focos_total else 0.0
        )
        pct_rows_with_missing_nonfocus = (
            rows_with_any_missing_nonfocus / nonfocos_total
            if nonfocos_total
            else 0.0
        )

        return YearMissingSummary(
            year=year,
            rows_total=rows_total,
            focos_total=focos_total,
            nonfocos_total=nonfocos_total,
            rows_with_any_missing=rows_with_any_missing,
            rows_with_any_missing_focus=rows_with_any_missing_focus,
            rows_with_any_missing_nonfocus=rows_with_any_missing_nonfocus,
            pct_rows_with_any_missing=pct_rows_with_any_missing,
            pct_rows_with_missing_focus=pct_rows_with_missing_focus,
            pct_rows_with_missing_nonfocus=pct_rows_with_missing_nonfocus,
        )

    def compute_all_year_summaries(self) -> pd.DataFrame:
        year_files = self.discover_year_files()
        summaries: List[YearMissingSummary] = []

        for year, fp in year_files.items():
            log.info(f"[YEAR] {year} — lendo {fp.name}")
            df = self.read_year_csv(fp)
            summaries.append(self.compute_year_summary(df, year))

        return (
            pd.DataFrame(asdict(s) for s in summaries)
            .sort_values("year")
            .reset_index(drop=True)
        )

    # ------------------------------
    # Breakdown por coluna e ano
    # ------------------------------
    def compute_column_breakdown_for_year(
        self,
        df: pd.DataFrame,
        year: int,
    ) -> pd.DataFrame:
        if "HAS_FOCO" not in df.columns:
            raise KeyError(f"Coluna HAS_FOCO não encontrada no ano {year}")

        missing = self.build_missing_matrix(df)
        foco_mask = df["HAS_FOCO"] == 1

        rows_total = int(len(df))
        focos_total = int(foco_mask.sum())
        nonfocos_total = rows_total - focos_total

        records: List[dict] = []

        for c in missing.columns:
            m = missing[c]

            missing_total = int(m.sum())
            missing_focus = int((m & foco_mask).sum())
            missing_nonfocus = missing_total - missing_focus

            records.append(
                {
                    "year": year,
                    "col": c,
                    "rows_total": rows_total,
                    "focos_total": focos_total,
                    "missing_total": missing_total,
                    "missing_focus": missing_focus,
                    "missing_nonfocus": missing_nonfocus,
                    "pct_missing_total": (
                        missing_total / rows_total if rows_total else 0.0
                    ),
                    "pct_missing_focus": (
                        missing_focus / focos_total if focos_total else 0.0
                    ),
                    "pct_missing_nonfocus": (
                        missing_nonfocus / nonfocos_total
                        if nonfocos_total
                        else 0.0
                    ),
                }
            )

        return (
            pd.DataFrame(records)
            .sort_values(["year", "pct_missing_total"], ascending=[True, False])
            .reset_index(drop=True)
        )

    def compute_all_years_column_breakdown(self) -> pd.DataFrame:
        year_files = self.discover_year_files()
        frames: List[pd.DataFrame] = []

        for year, fp in year_files.items():
            log.info(f"[YEAR-COLS] {year} — breakdown por coluna")
            df = self.read_year_csv(fp)
            frames.append(self.compute_column_breakdown_for_year(df, year))

        return pd.concat(frames, ignore_index=True)

    # ------------------------------
    # Vistas globais derivadas
    # ------------------------------
    def compute_global_column_missing(
        self,
        breakdown_df: pd.DataFrame,
        col_missing_threshold: float = COL_MISSING_THRESHOLD,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calcula, para cada coluna, a proporção global de missing (somando anos).
        Retorna:
          - tabela completa
          - tabela filtrada com colunas acima do limiar.
        """
        global_missing = (
            breakdown_df.groupby("col")
            .apply(lambda x: x["missing_total"].sum() / x["rows_total"].sum())
            .sort_values(ascending=False)
            .rename("pct_missing_global")
            .to_frame()
        )

        high_missing = global_missing[
            global_missing["pct_missing_global"] >= col_missing_threshold
        ]
        return global_missing, high_missing

    def evaluate_critical_years(
        self,
        breakdown_df: pd.DataFrame,
        critical_cols: List[str] = CRITICAL_COLS,
        year_threshold: float = YEAR_CRITICAL_THRESHOLD,
    ) -> pd.DataFrame:
        """
        Define anos ruins olhando apenas para colunas críticas de entrada.

        Importante:
        - RISCO_FOGO, FRP e FOCO_ID NÃO entram aqui.
        - Se apenas essas alvos estiverem muito faltantes, o ano não é marcado como ruim.
        """
        crit = breakdown_df[breakdown_df["col"].isin(critical_cols)]
        if crit.empty:
            log.warning(
                "[CRITICAL] Nenhuma coluna crítica encontrada para avaliação anual."
            )
            return pd.DataFrame(
                columns=["year", "max_pct_missing_critical", "bad_year"]
            )

        year_max = (
            crit.groupby("year")["pct_missing_total"]
            .max()
            .rename("max_pct_missing_critical")
            .to_frame()
            .sort_index()
        )
        year_max["bad_year"] = year_max["max_pct_missing_critical"] > year_threshold

        return year_max.reset_index()

    # ------------------------------
    # Pipeline completo com persistência
    # ------------------------------
    def run_and_persist(
        self,
        col_missing_threshold: float = COL_MISSING_THRESHOLD,
        year_threshold: float = YEAR_CRITICAL_THRESHOLD,
    ) -> None:
        """
        Executa a auditoria completa e salva CSVs em reports/eda/dataset.
        """
        ensure_dir(self.reports_dir)

        # 1) Resumo anual
        log.info("[RUN] Resumo anual de dados faltantes")
        year_summary = self.compute_all_year_summaries()
        year_summary_path = self.reports_dir / "missing_summary_by_year.csv"
        year_summary.to_csv(year_summary_path, index=False, encoding="utf-8")
        log.info(f"[WRITE] {year_summary_path}")

        # 2) Breakdown coluna x ano
        log.info("[RUN] Breakdown por coluna e ano")
        col_breakdown = self.compute_all_years_column_breakdown()
        col_breakdown_path = (
            self.reports_dir / "missing_breakdown_by_year_and_column.csv"
        )
        col_breakdown.to_csv(col_breakdown_path, index=False, encoding="utf-8")
        log.info(f"[WRITE] {col_breakdown_path}")

        # 3) Missing global por coluna
        log.info("[RUN] Missing global por coluna")
        global_missing, high_missing = self.compute_global_column_missing(
            col_breakdown,
            col_missing_threshold=col_missing_threshold,
        )

        global_missing_path = self.reports_dir / "missing_global_by_column.csv"
        high_missing_path = (
            self.reports_dir
            / f"missing_columns_over_{int(col_missing_threshold * 100)}pct.csv"
        )

        global_missing.to_csv(global_missing_path, index=True, encoding="utf-8")
        high_missing.to_csv(high_missing_path, index=True, encoding="utf-8")
        log.info(f"[WRITE] {global_missing_path}")
        log.info(f"[WRITE] {high_missing_path}")

        # 4) Anos críticos (somente features críticas)
        log.info("[RUN] Avaliação de anos críticos (features críticas)")
        critical_years = self.evaluate_critical_years(
            col_breakdown,
            critical_cols=CRITICAL_COLS,
            year_threshold=year_threshold,
        )
        critical_years_path = self.reports_dir / "missing_critical_years.csv"
        critical_years.to_csv(critical_years_path, index=False, encoding="utf-8")
        log.info(f"[WRITE] {critical_years_path}")


# -----------------------------------------------------------------------------
# [SEÇÃO 4] CLI
# -----------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Auditoria de dados faltantes nos CSVs inmet_bdq_{ANO}_cerrado.\n"
            "Considera como faltantes: NaN, null, vazios e códigos especiais negativos."
        )
    )

    parser.add_argument(
        "--pattern",
        default=FILENAME_PATTERN,
        help=f"Padrão dos arquivos (default: {FILENAME_PATTERN})",
    )
    parser.add_argument(
        "--col-threshold",
        type=float,
        default=COL_MISSING_THRESHOLD,
        help=(
            "Limiar para destacar colunas com missing global elevado "
            f"(default: {COL_MISSING_THRESHOLD:.2f})."
        ),
    )
    parser.add_argument(
        "--year-threshold",
        type=float,
        default=YEAR_CRITICAL_THRESHOLD,
        help=(
            "Limiar para marcar anos ruins com base em features críticas "
            f"(default: {YEAR_CRITICAL_THRESHOLD:.2f})."
        ),
    )

    args = parser.parse_args()

    analyzer = DatasetMissingAnalyzer(
        dataset_dir=DATASET_DIR,
        reports_dir=REPORTS_DATASET_DIR,
        file_pattern=args.pattern,
    )

    log.info(
        f"[CONFIG] dataset_dir={analyzer.dataset_dir} "
        f"pattern={analyzer.file_pattern} "
        f"reports_dir={analyzer.reports_dir}"
    )

    analyzer.run_and_persist(
        col_missing_threshold=args.col_threshold,
        year_threshold=args.year_threshold,
    )


if __name__ == "__main__":
    main()
