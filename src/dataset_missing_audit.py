# src/dataset_missing_audit.py
# =============================================================================
# EDA DATASET — AUDITORIA DE DADOS FALTANTES (INMET + BDQueimadas, CERRADO)
# =============================================================================
# Objetivo:
# - Analisar, por ano e por coluna de feature, a presença de dados faltantes nos
#   arquivos consolidados inmet_bdq_{ANO}_cerrado.csv.
# - Considerar como faltante:
#     * NaN / null
#     * strings vazias (após strip)
#     * códigos especiais negativos como -999 e -9999
# - Gerar, para cada ano:
#     * um CSV com contagem de missing por coluna de feature
#     * um README_missing.md explicando o CSV e resumindo o ano
# - Ignorar colunas alvo (RISCO_FOGO, FRP, FOCO_ID) nas estatísticas por coluna.
# - Persistir resultados em data/eda/dataset/{ANO}.
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, asdict, field
from pathlib import Path
from typing import Dict, List

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

# Diretório base de EDA em data/
try:
    DATA_EDA_DIR: Path = get_path("paths", "data", "eda")
    DATA_EDA_DIR = ensure_dir(DATA_EDA_DIR)
except Exception:
    DATA_EDA_DIR = ensure_dir(PROJECT_ROOT / "data" / "eda")

# Diretório específico para EDA do dataset
DATASET_EDA_DIR: Path = ensure_dir(DATA_EDA_DIR / "dataset")

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

# Colunas alvo (labels / auxiliares de rótulo) que NÃO entram na auditoria de features
TARGET_COLS = {
    "RISCO_FOGO",
    "FRP",
    "FOCO_ID",
}

# Limiares padrão (podem ser úteis se você quiser reaproveitar em outro lugar)
COL_MISSING_THRESHOLD = 0.40  # 40% global na coluna
YEAR_CRITICAL_THRESHOLD = 0.20  # 20% em qualquer coluna crítica no ano

# Validação imediata
if not DATASET_DIR.exists():
    raise FileNotFoundError(
        f"Diretorio de datasets nao encontrado.\nTentado: {DATASET_DIR}"
    )


# -----------------------------------------------------------------------------
# [SEÇÃO 2] ESTRUTURAS DE DADOS
# -----------------------------------------------------------------------------
@dataclass
class YearMissingSummary:
    """
    Resumo compacto, por ano, da presença de dados faltantes (visão agregada).
    Mantido para possivel reuso, embora o foco atual seja o audit ano a ano.
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
    eda_root_dir: Path
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
                f"Nenhum CSV encontrado em {self.dataset_dir} com padrao {self.file_pattern}"
            )

        log.info(f"[DISCOVER] {len(mapping)} arquivos anuais detectados.")
        return mapping

    # ------------------------------
    # Harmonização de colunas
    # ------------------------------
    def harmonize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Harmoniza inconsistências de nomenclatura entre anos.
        Exemplo principal: unificar radiacao global Kj/m² e KJ/m².
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
            # So existe a versao com Kj: renomeia para a versao canonica.
            df = df.rename(columns={old: new})

        return df

    # ------------------------------
    # Leitura
    # ------------------------------
    def read_year_csv(self, fp: Path) -> pd.DataFrame:
        """
        Le CSV consolidado e aplica harmonizacao de nomes.
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
        Retorna um DataFrame booleano indicando, para cada coluna (exceto excluidas),
        se a celula e faltante segundo as regras:

        - NaN / null
        - string vazia (apos strip)
        - codigos especiais como -999 e -9999
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
    # Resumo por ano (visão agregada)
    # ------------------------------
    def compute_year_summary(self, df: pd.DataFrame, year: int) -> YearMissingSummary:
        if "HAS_FOCO" not in df.columns:
            raise KeyError(f"Coluna HAS_FOCO nao encontrada no ano {year}")

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

    # ------------------------------
    # Breakdown por coluna (features) para um ano
    # ------------------------------
    def compute_feature_breakdown_for_year(
        self,
        df: pd.DataFrame,
        year: int,
    ) -> tuple[pd.DataFrame, YearMissingSummary]:
        """
        Calcula, para um ano, estatisticas de missing apenas para colunas de feature.

        - Ignora:
          * colunas em EXCLUDE_NON_NUMERIC
          * colunas em TARGET_COLS
          * coluna HAS_FOCO (label)
        """
        if "HAS_FOCO" not in df.columns:
            raise KeyError(f"Coluna HAS_FOCO nao encontrada no ano {year}")

        missing = self.build_missing_matrix(df)
        foco_mask = df["HAS_FOCO"] == 1

        rows_total = int(len(df))
        focos_total = int(foco_mask.sum())
        nonfocos_total = rows_total - focos_total

        records: List[dict] = []

        for c in missing.columns:
            # Ignora targets e label
            if c in TARGET_COLS or c == "HAS_FOCO":
                continue

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

        feature_df = (
            pd.DataFrame(records)
            .sort_values("pct_missing_total", ascending=False)
            .reset_index(drop=True)
        )

        summary = YearMissingSummary(
            year=year,
            rows_total=rows_total,
            focos_total=focos_total,
            nonfocos_total=nonfocos_total,
            rows_with_any_missing=int(self.build_missing_matrix(df).any(axis=1).sum()),
            rows_with_any_missing_focus=int(
                (self.build_missing_matrix(df).any(axis=1) & foco_mask).sum()
            ),
            rows_with_any_missing_nonfocus=0,  # nao e foco aqui, so reutilizando a estrutura
            pct_rows_with_any_missing=0.0,
            pct_rows_with_missing_focus=0.0,
            pct_rows_with_missing_nonfocus=0.0,
        )

        return feature_df, summary

    # ------------------------------
    # Escrita do README por ano
    # ------------------------------
    def write_year_readme(
        self,
        year_dir: Path,
        year: int,
        summary: YearMissingSummary,
        feature_df: pd.DataFrame,
        csv_name: str,
    ) -> None:
        """
        Gera o README_missing.md dentro do diretorio do ano, explicando o CSV.
        """
        rows_total = summary.rows_total
        focos_total = summary.focos_total
        foco_ratio = (focos_total / rows_total) if rows_total else 0.0

        top = feature_df.copy()
        if not top.empty:
            top = top.sort_values("pct_missing_total", ascending=False).head(5)

        lines: List[str] = []
        lines.append(f"# Auditoria de dados faltantes - {year}")
        lines.append("")
        lines.append(
            "Este diretorio contem a auditoria de valores faltantes nas colunas de "
            "feature do arquivo consolidado "
            f"`inmet_bdq_{year}_cerrado.csv`."
        )
        lines.append("")
        lines.append("## Resumo geral")
        lines.append("")
        lines.append(f"- Linhas totais: {rows_total}")
        lines.append(f"- Linhas com foco (HAS_FOCO == 1): {focos_total}")
        lines.append(f"- Proporcao de focos: {foco_ratio:.4f}")
        lines.append("")
        lines.append("## Arquivo de resultados")
        lines.append("")
        lines.append(
            f"O arquivo `{csv_name}` traz, para cada coluna de feature, a contagem "
            "e a proporcao de valores faltantes."
        )
        lines.append("")
        lines.append("Colunas do CSV:")
        lines.append("")
        lines.append("- `year`: ano de referencia dos dados.")
        lines.append("- `col`: nome da coluna de feature na base original.")
        lines.append("- `rows_total`: numero total de linhas no arquivo do ano.")
        lines.append("- `focos_total`: numero total de linhas com HAS_FOCO == 1.")
        lines.append(
            "- `missing_total`: numero de linhas em que nao ha valor valido para essa coluna."
        )
        lines.append(
            "- `missing_focus`: numero de linhas com foco (HAS_FOCO == 1) em que nao ha valor valido para essa coluna."
        )
        lines.append(
            "- `missing_nonfocus`: numero de linhas sem foco em que nao ha valor valido para essa coluna."
        )
        lines.append(
            "- `pct_missing_total`: proporcao de linhas com valor faltante na coluna."
        )
        lines.append(
            "- `pct_missing_focus`: proporcao de linhas com foco que estao com valor faltante na coluna."
        )
        lines.append(
            "- `pct_missing_nonfocus`: proporcao de linhas sem foco que estao com valor faltante na coluna."
        )
        lines.append("")
        lines.append("## Top colunas com mais faltantes")
        lines.append("")
        if top.empty:
            lines.append("Nao ha colunas com valores faltantes neste ano.")
        else:
            lines.append(
                "As 5 colunas com maior `pct_missing_total` neste ano sao:"
            )
            lines.append("")
            lines.append("| col | missing_total | pct_missing_total |")
            lines.append("| --- | ------------- | ----------------- |")
            for _, row in top.iterrows():
                col_name = str(row["col"])
                miss_tot = int(row["missing_total"])
                pct_tot = float(row["pct_missing_total"])
                lines.append(
                    f"| {col_name} | {miss_tot} | {pct_tot:.4f} |"
                )

        readme_path = year_dir / "README_missing.md"
        readme_path.write_text("\n".join(lines), encoding="utf-8")
        log.info(f"[WRITE] {readme_path}")

    # ------------------------------
    # Pipeline principal: audit ano a ano
    # ------------------------------
    def run_per_year_audit(self, years: List[int] | None = None) -> None:
        """
        Executa a auditoria ano a ano, gerando:

        data/eda/dataset/{ANO}/
          - missing_by_column.csv
          - README_missing.md

        Se `years` for None, processa todos os anos detectados.
        """
        year_files = self.discover_year_files()

        if years is not None:
            selected = {y: fp for y, fp in year_files.items() if y in years}
            if not selected:
                raise ValueError(
                    f"Nenhum dos anos especificados {years} foi encontrado entre "
                    f"os arquivos detectados: {sorted(year_files.keys())}"
                )
            year_files = dict(sorted(selected.items()))

        log.info(
            f"[RUN] Auditoria ano a ano em data/eda/dataset "
            f"para {len(year_files)} anos."
        )

        for year, fp in year_files.items():
            log.info(f"[YEAR] {year} - lendo {fp.name}")
            df = self.read_year_csv(fp)

            feature_df, summary = self.compute_feature_breakdown_for_year(df, year)

            # Diretorio do ano em data/eda/dataset/{ANO}
            year_dir = ensure_dir(self.eda_root_dir / str(year))

            csv_name = "missing_by_column.csv"
            csv_path = year_dir / csv_name

            feature_df.to_csv(csv_path, index=False, encoding="utf-8")
            log.info(f"[WRITE] {csv_path}")

            # README para explicar o CSV
            self.write_year_readme(
                year_dir=year_dir,
                year=year,
                summary=summary,
                feature_df=feature_df,
                csv_name=csv_name,
            )


# -----------------------------------------------------------------------------
# [SEÇÃO 4] CLI
# -----------------------------------------------------------------------------
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Auditoria de dados faltantes nos CSVs inmet_bdq_{ANO}_cerrado.\n"
            "Gera, para cada ano, um CSV com missing por coluna de feature e "
            "um README explicativo em data/eda/dataset/{ANO}."
        )
    )

    parser.add_argument(
        "--pattern",
        default=FILENAME_PATTERN,
        help=f"Padrao dos arquivos (default: {FILENAME_PATTERN})",
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        help=(
            "Lista de anos a processar. "
            "Se nao for informada, processa todos os anos detectados."
        ),
    )

    args = parser.parse_args()

    analyzer = DatasetMissingAnalyzer(
        dataset_dir=DATASET_DIR,
        eda_root_dir=DATASET_EDA_DIR,
        file_pattern=args.pattern,
    )

    log.info(
        f"[CONFIG] dataset_dir={analyzer.dataset_dir} "
        f"pattern={analyzer.file_pattern} "
        f"eda_root_dir={analyzer.eda_root_dir}"
    )

    analyzer.run_per_year_audit(years=args.years)


if __name__ == "__main__":
    main()
