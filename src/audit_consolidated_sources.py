# src/audit_consolidated_sources.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd


# =============================================================================
# CONFIGURAÇÃO
# =============================================================================
MISSING_CODES = {-999, -9999}
MISSING_CODES_STR = {"-999", "-9999", "-999.0", "-9999.0"}
DEFAULT_CHUNKSIZE = 200_000

DEFAULT_BDQ_PATH = Path(
    r"D:\Projetos\TCC\data\consolidated\BDQUEIMADAS\bdq_targets_2003_2025_cerrado.csv"
)
DEFAULT_INMET_PATH = Path(
    r"D:\Projetos\TCC\data\consolidated\INMET\inmet_all_years_cerrado.csv"
)
DEFAULT_MERGED_PATH = Path(
    r"D:\Projetos\TCC\data\dataset\inmet_bdq_all_years_cerrado.csv"
)
DEFAULT_OUTPUT_DIR = Path(
    r"D:\Projetos\TCC\data\eda\consolidated_audit"
)


# =============================================================================
# ESTRUTURAS
# =============================================================================
@dataclass
class AuditSource:
    name: str
    path: Path
    target_col: Optional[str] = None
    positive_value: object = 1
    output_stem: Optional[str] = None

    def stem(self) -> str:
        return self.output_stem or self.name.lower().replace(" ", "_")


@dataclass
class FileSummary:
    source_name: str
    file_path: str
    file_size_bytes: int
    file_size_human: str
    rows_total: int
    cols_total: int
    rows_with_any_missing: int
    pct_rows_with_any_missing: float
    target_col: Optional[str]
    positive_count: Optional[int]
    negative_count: Optional[int]
    positive_ratio: Optional[float]
    year_min: Optional[int]
    year_max: Optional[int]


# =============================================================================
# UTILIDADES
# =============================================================================
def sizeof_fmt(num: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(num)
    for unit in units:
        if value < 1024.0 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024.0
    return f"{num} B"


def md_escape(value: object) -> str:
    text = str(value)
    return text.replace("|", r"\|").replace("\n", " ").strip()


def fmt_pct(value: Optional[float]) -> str:
    if value is None:
        return "N/A"
    return f"{value:.4%}"


def fmt_int(value: Optional[int]) -> str:
    if value is None:
        return "N/A"
    return f"{value:,}".replace(",", ".")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def harmonize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Harmoniza colunas conhecidas que podem variar entre arquivos/anos.
    """
    df = df.copy()

    old = "RADIACAO GLOBAL (Kj/m²)"
    new = "RADIACAO GLOBAL (KJ/m²)"

    if old in df.columns and new in df.columns:
        df[new] = df[new].combine_first(df[old])
        df = df.drop(columns=[old])
    elif old in df.columns:
        df = df.rename(columns={old: new})

    return df


def is_missing_series(series: pd.Series) -> pd.Series:
    """
    Regras de missing:
    - NaN / null
    - string vazia após strip
    - códigos especiais -999 e -9999
    """
    if pd.api.types.is_bool_dtype(series):
        return series.isna().fillna(False)

    if pd.api.types.is_numeric_dtype(series):
        return (series.isna() | series.isin(MISSING_CODES)).fillna(False)

    s = series.astype("string")
    mask = s.isna()
    mask |= s.str.strip().eq("")
    mask |= s.str.strip().isin(MISSING_CODES_STR)
    return mask.fillna(False)


def count_positive(series: pd.Series, positive_value: object) -> int:
    if pd.api.types.is_numeric_dtype(series):
        numeric = pd.to_numeric(series, errors="coerce")
        return int(numeric.eq(positive_value).sum())

    s = series.astype("string").str.strip()
    return int(s.eq(str(positive_value)).sum())


def infer_year_range_from_chunk(
    chunk: pd.DataFrame,
    current_min: Optional[int],
    current_max: Optional[int],
) -> tuple[Optional[int], Optional[int]]:
    if "ANO" in chunk.columns:
        years = pd.to_numeric(chunk["ANO"], errors="coerce").dropna()
        if not years.empty:
            chunk_min = int(years.min())
            chunk_max = int(years.max())
            current_min = chunk_min if current_min is None else min(current_min, chunk_min)
            current_max = chunk_max if current_max is None else max(current_max, chunk_max)
            return current_min, current_max

    if "ts_hour" in chunk.columns:
        ts = pd.to_datetime(chunk["ts_hour"], errors="coerce")
        ts = ts.dropna()
        if not ts.empty:
            chunk_min = int(ts.dt.year.min())
            chunk_max = int(ts.dt.year.max())
            current_min = chunk_min if current_min is None else min(current_min, chunk_min)
            current_max = chunk_max if current_max is None else max(current_max, chunk_max)

    return current_min, current_max


def markdown_table(headers: List[str], rows: List[List[object]]) -> str:
    line_header = "| " + " | ".join(md_escape(h) for h in headers) + " |"
    line_sep = "| " + " | ".join("---" for _ in headers) + " |"
    line_rows = [
        "| " + " | ".join(md_escape(cell) for cell in row) + " |"
        for row in rows
    ]
    return "\n".join([line_header, line_sep] + line_rows)


# =============================================================================
# AUDITOR
# =============================================================================
class ConsolidatedAuditor:
    def __init__(
        self,
        sources: Iterable[AuditSource],
        output_dir: Path,
        chunksize: int = DEFAULT_CHUNKSIZE,
    ) -> None:
        self.sources = list(sources)
        self.output_dir = ensure_dir(output_dir)
        self.chunksize = chunksize

    def read_csv_in_chunks(self, path: Path):
        encodings = ["utf-8", "utf-8-sig", "latin1"]
        last_error = None

        for enc in encodings:
            try:
                yield from pd.read_csv(
                    path,
                    sep=",",
                    encoding=enc,
                    low_memory=False,
                    chunksize=self.chunksize,
                )
                return
            except UnicodeDecodeError as exc:
                last_error = exc

        raise RuntimeError(
            f"Não foi possível ler o arquivo {path} com os encodings testados."
        ) from last_error

    def audit_source(self, source: AuditSource) -> tuple[FileSummary, pd.DataFrame]:
        if not source.path.exists():
            raise FileNotFoundError(f"Arquivo não encontrado: {source.path}")

        file_size_bytes = source.path.stat().st_size
        file_size_human = sizeof_fmt(file_size_bytes)

        rows_total = 0
        cols_total = 0
        rows_with_any_missing = 0

        positive_count: Optional[int] = 0 if source.target_col else None

        year_min: Optional[int] = None
        year_max: Optional[int] = None

        missing_counts: Dict[str, int] = {}
        dtypes: Dict[str, str] = {}
        ordered_columns: List[str] = []

        for chunk in self.read_csv_in_chunks(source.path):
            chunk = harmonize_columns(chunk)

            if not ordered_columns:
                ordered_columns = list(chunk.columns)
                cols_total = len(ordered_columns)
                for col in ordered_columns:
                    missing_counts[col] = 0
                    dtypes[col] = str(chunk[col].dtype)

            rows_total += len(chunk)
            year_min, year_max = infer_year_range_from_chunk(chunk, year_min, year_max)

            chunk_any_missing = pd.Series(False, index=chunk.index)

            for col in ordered_columns:
                if col not in chunk.columns:
                    # segurança para chunks com schema inesperado
                    missing_counts[col] += len(chunk)
                    chunk_any_missing |= True
                    continue

                mask = is_missing_series(chunk[col])
                missing_counts[col] += int(mask.sum())
                chunk_any_missing |= mask

            rows_with_any_missing += int(chunk_any_missing.sum())

            if source.target_col and source.target_col in chunk.columns:
                positive_count = (positive_count or 0) + count_positive(
                    chunk[source.target_col],
                    source.positive_value,
                )

        positive_ratio: Optional[float] = None
        negative_count: Optional[int] = None

        if source.target_col:
            positive_count = int(positive_count or 0)
            negative_count = rows_total - positive_count
            positive_ratio = positive_count / rows_total if rows_total else None

        column_records: List[dict] = []
        for col in ordered_columns:
            miss = missing_counts[col]
            column_records.append(
                {
                    "source_name": source.name,
                    "column_name": col,
                    "dtype": dtypes.get(col, "unknown"),
                    "rows_total": rows_total,
                    "non_missing_total": rows_total - miss,
                    "missing_total": miss,
                    "pct_missing": miss / rows_total if rows_total else 0.0,
                }
            )

        column_df = (
            pd.DataFrame(column_records)
            .sort_values(["pct_missing", "column_name"], ascending=[False, True])
            .reset_index(drop=True)
        )

        summary = FileSummary(
            source_name=source.name,
            file_path=str(source.path),
            file_size_bytes=file_size_bytes,
            file_size_human=file_size_human,
            rows_total=rows_total,
            cols_total=cols_total,
            rows_with_any_missing=rows_with_any_missing,
            pct_rows_with_any_missing=(rows_with_any_missing / rows_total) if rows_total else 0.0,
            target_col=source.target_col,
            positive_count=positive_count,
            negative_count=negative_count,
            positive_ratio=positive_ratio,
            year_min=year_min,
            year_max=year_max,
        )
        return summary, column_df

    def write_source_markdown(
        self,
        source: AuditSource,
        summary: FileSummary,
        column_df: pd.DataFrame,
        csv_name: str,
    ) -> Path:
        md_path = self.output_dir / f"{source.stem()}_audit.md"

        top_missing = column_df.head(15).copy()

        lines: List[str] = []
        lines.append(f"# Auditoria do consolidado: {summary.source_name}")
        lines.append("")
        lines.append("## Resumo geral")
        lines.append("")
        lines.append(f"- Arquivo: `{summary.file_path}`")
        lines.append(f"- Tamanho em disco: {summary.file_size_human}")
        lines.append(f"- Linhas totais: {fmt_int(summary.rows_total)}")
        lines.append(f"- Colunas totais: {fmt_int(summary.cols_total)}")

        if summary.year_min is not None and summary.year_max is not None:
            lines.append(f"- Intervalo temporal inferido: {summary.year_min} a {summary.year_max}")

        lines.append(
            f"- Linhas com pelo menos um valor faltante: {fmt_int(summary.rows_with_any_missing)} "
            f"({fmt_pct(summary.pct_rows_with_any_missing)})"
        )

        if summary.target_col:
            lines.append(f"- Coluna alvo auditada: `{summary.target_col}`")
            lines.append(f"- Classe positiva: {fmt_int(summary.positive_count)}")
            lines.append(f"- Classe negativa: {fmt_int(summary.negative_count)}")
            lines.append(f"- Proporção da classe positiva: {fmt_pct(summary.positive_ratio)}")
        else:
            lines.append("- Proporção da classe positiva: N/A neste consolidado")

        lines.append("")
        lines.append("## Arquivo CSV gerado")
        lines.append("")
        lines.append(
            f"O arquivo `{csv_name}` contém a auditoria completa por coluna, incluindo "
            "tipo do dado, contagem de faltantes e proporção de missing."
        )
        lines.append("")
        lines.append("## Colunas com maior proporção de missing")
        lines.append("")

        if top_missing.empty:
            lines.append("Nenhuma coluna encontrada.")
        else:
            headers = [
                "column_name",
                "dtype",
                "missing_total",
                "pct_missing",
                "non_missing_total",
            ]
            rows = []
            for _, row in top_missing.iterrows():
                rows.append(
                    [
                        row["column_name"],
                        row["dtype"],
                        fmt_int(int(row["missing_total"])),
                        f"{float(row['pct_missing']):.4%}",
                        fmt_int(int(row["non_missing_total"])),
                    ]
                )
            lines.append(markdown_table(headers, rows))

        lines.append("")
        lines.append("## Auditoria completa por coluna")
        lines.append("")

        headers = [
            "column_name",
            "dtype",
            "missing_total",
            "pct_missing",
            "non_missing_total",
        ]
        rows = []
        for _, row in column_df.iterrows():
            rows.append(
                [
                    row["column_name"],
                    row["dtype"],
                    fmt_int(int(row["missing_total"])),
                    f"{float(row['pct_missing']):.4%}",
                    fmt_int(int(row["non_missing_total"])),
                ]
            )
        lines.append(markdown_table(headers, rows))

        md_path.write_text("\n".join(lines), encoding="utf-8")
        return md_path

    def write_master_readme(
        self,
        summaries: List[FileSummary],
        audit_files: List[dict],
    ) -> Path:
        readme_path = self.output_dir / "README_auditoria_consolidados.md"

        lines: List[str] = []
        lines.append("# Auditoria dos arquivos consolidados")
        lines.append("")
        lines.append(
            "Este diretório reúne a auditoria dos três arquivos consolidados: "
            "BDQueimadas, INMET e base integrada INMET + BDQueimadas."
        )
        lines.append("")
        lines.append("## Visão geral")
        lines.append("")

        headers = [
            "Fonte",
            "Linhas",
            "Colunas",
            "Tamanho",
            "Linhas com missing",
            "% linhas com missing",
            "Classe positiva",
            "% classe positiva",
            "Markdown",
            "CSV",
        ]
        rows = []
        for summary, files in zip(summaries, audit_files):
            rows.append(
                [
                    summary.source_name,
                    fmt_int(summary.rows_total),
                    fmt_int(summary.cols_total),
                    summary.file_size_human,
                    fmt_int(summary.rows_with_any_missing),
                    fmt_pct(summary.pct_rows_with_any_missing),
                    fmt_int(summary.positive_count),
                    fmt_pct(summary.positive_ratio),
                    files["md_name"],
                    files["csv_name"],
                ]
            )

        lines.append(markdown_table(headers, rows))
        lines.append("")
        lines.append(
            "Observação: a proporção da classe positiva só é calculada quando a base "
            "contém explicitamente uma coluna alvo binária, como `HAS_FOCO`."
        )

        readme_path.write_text("\n".join(lines), encoding="utf-8")
        return readme_path

    def run(self) -> None:
        summaries: List[FileSummary] = []
        audit_files: List[dict] = []

        for source in self.sources:
            summary, column_df = self.audit_source(source)

            csv_name = f"{source.stem()}_column_audit.csv"
            csv_path = self.output_dir / csv_name
            column_df.to_csv(csv_path, index=False, encoding="utf-8")

            md_path = self.write_source_markdown(
                source=source,
                summary=summary,
                column_df=column_df,
                csv_name=csv_name,
            )

            summaries.append(summary)
            audit_files.append(
                {
                    "csv_name": csv_path.name,
                    "md_name": md_path.name,
                }
            )

        overview_df = pd.DataFrame(
            [
                {
                    "source_name": s.source_name,
                    "file_path": s.file_path,
                    "file_size_bytes": s.file_size_bytes,
                    "file_size_human": s.file_size_human,
                    "rows_total": s.rows_total,
                    "cols_total": s.cols_total,
                    "rows_with_any_missing": s.rows_with_any_missing,
                    "pct_rows_with_any_missing": s.pct_rows_with_any_missing,
                    "target_col": s.target_col,
                    "positive_count": s.positive_count,
                    "negative_count": s.negative_count,
                    "positive_ratio": s.positive_ratio,
                    "year_min": s.year_min,
                    "year_max": s.year_max,
                }
                for s in summaries
            ]
        )
        overview_df.to_csv(
            self.output_dir / "overview_consolidated_audit.csv",
            index=False,
            encoding="utf-8",
        )

        self.write_master_readme(summaries, audit_files)


# =============================================================================
# CLI
# =============================================================================
def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description=(
            "Audita os arquivos consolidados de BDQueimadas, INMET e da base integrada, "
            "calculando missing por coluna e proporção da classe positiva quando existir."
        )
    )

    parser.add_argument(
        "--bdq-path",
        type=Path,
        default=DEFAULT_BDQ_PATH,
        help=f"Caminho do consolidado BDQueimadas (default: {DEFAULT_BDQ_PATH})",
    )
    parser.add_argument(
        "--inmet-path",
        type=Path,
        default=DEFAULT_INMET_PATH,
        help=f"Caminho do consolidado INMET (default: {DEFAULT_INMET_PATH})",
    )
    parser.add_argument(
        "--merged-path",
        type=Path,
        default=DEFAULT_MERGED_PATH,
        help=f"Caminho da base integrada (default: {DEFAULT_MERGED_PATH})",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Diretório de saída (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=DEFAULT_CHUNKSIZE,
        help=f"Tamanho do chunk de leitura (default: {DEFAULT_CHUNKSIZE})",
    )

    args = parser.parse_args()

    sources = [
        AuditSource(
            name="BDQueimadas consolidado",
            path=args.bdq_path,
            target_col=None,
            output_stem="bdqueimadas_consolidado",
        ),
        AuditSource(
            name="INMET consolidado",
            path=args.inmet_path,
            target_col=None,
            output_stem="inmet_consolidado",
        ),
        AuditSource(
            name="Base integrada INMET + BDQueimadas",
            path=args.merged_path,
            target_col="HAS_FOCO",
            positive_value=1,
            output_stem="dataset_integrado",
        ),
    ]

    auditor = ConsolidatedAuditor(
        sources=sources,
        output_dir=args.output_dir,
        chunksize=args.chunksize,
    )
    auditor.run()

    print(f"Auditoria concluída. Arquivos salvos em: {args.output_dir}")


if __name__ == "__main__":
    main()