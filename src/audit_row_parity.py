"""Compara contagens de linhas entre CSV INMET consolidado e parquets em modeling.

Fontes:
    data/consolidated/INMET/inmet_{ANO}_cerrado.csv (principal)
    ou inmet_bdq_{ANO}_cerrado.csv (fallback legado)
    data/modeling/<base>/inmet_bdq_{ANO}_cerrado.parquet

Referencias esperadas:
    ``base_F_full_original`` deve alinhar com o CSV quando ambos existem e o pipeline
    nao introduziu linhas extras. ``base_C_no_rad_drop_rows`` e
    ``base_D_with_rad_drop_rows`` podem ter menos linhas por desenho (drop).

Nao compara com bases do artigo (coords/fusion): apenas consolidated vs modeling.

Saida:
    data/_article/_audits/{YYYYMMDD_HHMMSS}_row_parity/summary.md
    .../parity.csv
    .../raw.json
    data/_article/_audits/LATEST_ROW_PARITY.md

Uso:
    python -m src.audit_row_parity
    python -m src.audit_row_parity --bases base_F_full_original base_E_with_rad_knn
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pyarrow.parquet as pq

_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.dedupe_base_datasets import DEFAULT_MODELING_BASES
from src.utils import get_logger, loadConfig

_RE_CSV_SHORT = re.compile(r"^inmet_(\d{4})_cerrado\.csv$", re.I)
_RE_CSV_BDQ = re.compile(r"^inmet_bdq_(\d{4})_cerrado\.csv$", re.I)
_RE_PARQUET = re.compile(r"^inmet_bdq_(\d{4})_cerrado\.parquet$", re.I)


def _year_from_csv_path(path: Path) -> Optional[int]:
    for rx in (_RE_CSV_SHORT, _RE_CSV_BDQ):
        m = rx.match(path.name)
        if m:
            return int(m.group(1))
    return None


def _year_from_parquet_path(path: Path) -> Optional[int]:
    m = _RE_PARQUET.match(path.name)
    return int(m.group(1)) if m else None


def _resolve_consolidated_csv(inmet_dir: Path, year: int) -> tuple[Optional[Path], Optional[str]]:
    """Preferencia: inmet_{year}_cerrado.csv; senao inmet_bdq_{year}_cerrado.csv."""
    short_p = inmet_dir / f"inmet_{year}_cerrado.csv"
    if short_p.is_file():
        return short_p, short_p.name
    bdq_p = inmet_dir / f"inmet_bdq_{year}_cerrado.csv"
    if bdq_p.is_file():
        return bdq_p, bdq_p.name
    return None, None


def _count_csv_data_rows(path: Path) -> int:
    """Linhas apos o cabecalho (primeira linha)."""
    with path.open(encoding="utf-8", errors="replace", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def _parquet_num_rows(path: Path) -> int:
    return int(pq.ParquetFile(path).metadata.num_rows)


def _collect_years(inmet_dir: Path, modeling_root: Path, bases: List[str]) -> List[int]:
    years: Set[int] = set()
    for p in sorted(inmet_dir.glob("inmet_*_cerrado.csv")):
        y = _year_from_csv_path(p)
        if y is not None:
            years.add(y)
    for base in bases:
        bdir = modeling_root / base
        if not bdir.is_dir():
            continue
        for p in sorted(bdir.glob("inmet_bdq_*_cerrado.parquet")):
            y = _year_from_parquet_path(p)
            if y is not None:
                years.add(y)
    return sorted(years)


def _parity_row(
    year: int,
    inmet_dir: Path,
    modeling_root: Path,
    bases: List[str],
) -> Dict[str, Any]:
    row: Dict[str, Any] = {"year": year}

    csv_path, csv_name = _resolve_consolidated_csv(inmet_dir, year)
    if csv_path is not None:
        row["inmet_csv_file"] = csv_name
        try:
            row["inmet_csv_rows"] = _count_csv_data_rows(csv_path)
        except OSError as e:
            row["inmet_csv_rows"] = None
            row["inmet_note"] = f"read_error: {e}"
    else:
        row["inmet_csv_rows"] = None
        row["inmet_note"] = "missing_csv"

    ref_inmet = row["inmet_csv_rows"]

    for base in bases:
        pq_path = modeling_root / base / f"inmet_bdq_{year}_cerrado.parquet"
        key = f"{base}_rows"
        if not pq_path.is_file():
            row[key] = None
            row[f"{base}_note"] = "missing_parquet"
            continue
        try:
            n = _parquet_num_rows(pq_path)
            row[key] = n
            if ref_inmet is not None:
                row[f"{base}_delta_vs_inmet"] = n - ref_inmet
        except OSError as e:
            row[key] = None
            row[f"{base}_note"] = f"read_error: {e}"

    # Status sintetico para base_F vs INMET
    f_rows = row.get("base_F_full_original_rows")
    if ref_inmet is not None and f_rows is not None:
        if f_rows == ref_inmet:
            row["base_F_vs_inmet"] = "MATCH"
        elif f_rows < ref_inmet:
            row["base_F_vs_inmet"] = "FEWER"
        else:
            row["base_F_vs_inmet"] = "MORE"
    else:
        row["base_F_vs_inmet"] = "N/A"

    return row


def _md_cell(v: Any) -> str:
    if v is None:
        return "—"
    return str(v)


def _render_summary(rows: List[Dict[str, Any]], bases: List[str], paths_note: str) -> str:
    lines = [
        "# Row parity — consolidated INMET vs modeling",
        "",
        paths_note,
        "",
        "## Por ano",
        "",
        "| year | inmet_csv | " + " | ".join(b.replace("_", "\\_") + "_rows" for b in bases) + " | base_F vs INMET |",
        "| --- | ---:| " + " | ".join("---:" for _ in bases) + " | --- |",
    ]
    for r in rows:
        y = r["year"]
        inc_s = _md_cell(r.get("inmet_csv_rows"))
        cols = [_md_cell(r.get(f"{b}_rows")) for b in bases]
        bf = r.get("base_F_vs_inmet", "N/A")
        lines.append(f"| {y} | {inc_s} | " + " | ".join(cols) + f" | {bf} |")
    lines.extend(
        [
            "",
            "## Legenda",
            "",
            "- **MATCH**: ``base_F_full_original`` tem o mesmo numero de linhas que o CSV consolidado.",
            "- **FEWER** / **MORE**: discrepancia — revisar dedupe/build.",
            "- Colunas *_rows ausentes: ficheiro em falta ou erro de leitura.",
            "- ``base_C`` / ``base_D`` podem ter menos linhas que o INMET por desenho (`drop`).",
            "",
        ]
    )
    return "\n".join(lines)


def run(bases: Optional[List[str]] = None) -> Path:
    cfg = loadConfig()
    log = get_logger("audit.row_parity")
    bases = list(bases or DEFAULT_MODELING_BASES)

    external = Path(cfg["paths"]["data"]["external"])
    inmet_dir = external / "INMET"
    modeling_root = Path(cfg["paths"]["data"]["modeling"])

    paths_note = (
        f"- Consolidated CSV: `{inmet_dir}` / `inmet_<ANO>_cerrado.csv` "
        f"(ou `inmet_bdq_<ANO>_cerrado.csv`)\n"
        f"- Modeling root: `{modeling_root}` / `<base>/inmet_bdq_*_cerrado.parquet`"
    )

    years = _collect_years(inmet_dir, modeling_root, bases)
    if not years:
        log.warning("Nenhum ano encontrado (CSV INMET ou parquets modeling em falta?).")

    rows = [_parity_row(y, inmet_dir, modeling_root, bases) for y in years]

    article_root = Path(cfg["paths"]["data"].get("article") or (_project_root / "data" / "_article"))
    audits_root = article_root / "_audits"
    audits_root.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_dir = audits_root / f"{ts}_row_parity"
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_md = _render_summary(rows, bases, paths_note)
    (out_dir / "summary.md").write_text(summary_md, encoding="utf-8")

    # parity.csv — todas as chaves, ordem estavel
    all_keys: Set[str] = set()
    for r in rows:
        all_keys.update(r.keys())
    priority = ["year", "inmet_csv_rows"] + [f"{b}_rows" for b in bases]
    ordered = [k for k in priority if k in all_keys]
    ordered.extend(sorted(k for k in all_keys if k not in ordered))

    with (out_dir / "parity.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=ordered, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in ordered})

    payload = {
        "timestamp": ts,
        "inmet_dir": str(inmet_dir),
        "modeling_root": str(modeling_root),
        "bases": bases,
        "years": years,
        "rows": rows,
    }
    with (out_dir / "raw.json").open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, ensure_ascii=False)

    latest = audits_root / "LATEST_ROW_PARITY.md"
    rel = out_dir.relative_to(audits_root).as_posix()
    latest.write_text(
        "# Latest row parity (INMET consolidated vs modeling)\n\n"
        f"- Gerado em `{ts}`.\n"
        f"- Relatorio: [{rel}/summary.md]({rel}/summary.md)\n",
        encoding="utf-8",
    )

    log.info(f"Escrito -> {out_dir}")
    return out_dir


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--bases",
        nargs="+",
        default=None,
        help="Subset de pastas em modeling (default: todas as DEFAULT_MODELING_BASES).",
    )
    args = p.parse_args()
    run(bases=args.bases)


if __name__ == "__main__":
    main()
