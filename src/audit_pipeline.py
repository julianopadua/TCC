"""Auditoria unificada e organizada do pipeline de dados do artigo.

Audita todos os estagios e grava relatorios em
``data/_article/_audits/{YYYYMMDD_HHMMSS}_{stage}/`` com:
    - summary.md   (humano-legivel, com status OK/FAIL por arquivo)
    - per_file.csv (tabela long, consumivel por qualquer ferramenta)
    - raw.json     (dump estruturado completo)

Tambem gera ``data/_article/_audits/LATEST.md``, um indice que aponta
sempre para os relatorios mais recentes de cada estagio.

Estagios:
    modeling       -> data/modeling/base_* (fontes pre-features)
    calculated     -> data/modeling/base_*_calculated
    coords         -> data/_article/0_datasets_with_coords/*
    fusion         -> data/_article/1_datasets_with_fusion/*/{method}/
    all            -> todos os acima (default)

Uso:
    python -m src.audit_pipeline                    # roda todos os estagios
    python -m src.audit_pipeline --stage coords
    python -m src.audit_pipeline --stage fusion --methods ewma_lags minirocket
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import pyarrow.parquet as pq

_project_root = Path(__file__).resolve().parents[1]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from src.utils import get_logger, loadConfig

# Chaves que definem 1 observacao unica
KEY_COLS = ("cidade_norm", "ts_hour")

# Thresholds de duplicacao (o bug pre-dedupe era 2.0x ou 4.0x):
#   ratio < RATIO_OK         -> OK         (sem duplicacao detectavel)
#   ratio < RATIO_SOFT       -> SOFT_DUP   (aceitavel; multi-foco genuino, max ~1.07x)
#   ratio >= RATIO_SOFT      -> DUPLICATED (bug: cartesian no physics-merge OU fonte 2x/4x)
RATIO_OK = 1.01
RATIO_SOFT = 1.10

# Bases pre-features (sem sufixo _calculated)
MODELING_BASES = (
    "base_A_no_rad",
    "base_B_no_rad_knn",
    "base_C_no_rad_drop_rows",
    "base_D_with_rad_drop_rows",
    "base_E_with_rad_knn",
    "base_F_full_original",
)

# Bases derivadas (com features fisicas)
CALCULATED_BASES = tuple(f"{b}_calculated" for b in MODELING_BASES)

# Bases efetivamente usadas no artigo
ARTICLE_BASES = (
    "base_D_with_rad_drop_rows_calculated",
    "base_E_with_rad_knn_calculated",
    "base_F_full_original_calculated",
)


# ----------------------------------------------------------------------
# Primitiva: audita 1 parquet quanto a duplicacao por (cidade_norm, ts_hour)
# ----------------------------------------------------------------------
def _audit_parquet(path: Path) -> Dict[str, Any]:
    entry: Dict[str, Any] = {
        "file": path.name,
        "path": str(path),
        "rows": 0,
        "unique_keys": None,
        "dup_ratio": None,
        "status": "UNKNOWN",
        "notes": "",
    }
    try:
        pf = pq.ParquetFile(path)
        entry["rows"] = int(pf.metadata.num_rows)
        schema = set(pf.schema_arrow.names)
        missing = [c for c in KEY_COLS if c not in schema]
        if missing:
            entry["status"] = "NO_KEYS"
            entry["notes"] = f"missing_cols={missing}"
            return entry
        df = pf.read(columns=list(KEY_COLS)).to_pandas()
        unique = int(df.drop_duplicates().shape[0])
        entry["unique_keys"] = unique
        if unique > 0:
            ratio = entry["rows"] / unique
            entry["dup_ratio"] = round(ratio, 4)
            if ratio < RATIO_OK:
                entry["status"] = "OK"
            elif ratio < RATIO_SOFT:
                entry["status"] = "SOFT_DUP"
                entry["notes"] = "minor multi-foco overhead (genuino)"
            else:
                entry["status"] = "DUPLICATED"
        else:
            entry["status"] = "EMPTY"
    except Exception as e:
        entry["status"] = "ERROR"
        entry["notes"] = repr(e)
    return entry


# ----------------------------------------------------------------------
# Descoberta de parquets por estagio
# ----------------------------------------------------------------------
def _find_parquets(root: Path, subdir: str) -> List[Path]:
    d = root / subdir
    if not d.is_dir():
        return []
    return sorted(d.glob("inmet_bdq_*_cerrado.parquet"))


def _discover_stage(
    stage: str,
    cfg: Dict[str, Any],
    bases: Optional[List[str]] = None,
    methods: Optional[List[str]] = None,
) -> List[Tuple[str, Path]]:
    """Retorna lista de (label, parquet_path) a auditar para um estagio."""
    entries: List[Tuple[str, Path]] = []
    modeling_root = Path(cfg["paths"]["data"]["modeling"])
    article_root = Path(cfg["paths"]["data"].get("article") or
                        (_project_root / "data" / "_article"))
    coords_root = article_root / "0_datasets_with_coords"
    fusion_root = article_root / "1_datasets_with_fusion"

    if stage == "modeling":
        for b in bases or list(MODELING_BASES):
            for p in _find_parquets(modeling_root, b):
                entries.append((b, p))
    elif stage == "calculated":
        for b in bases or list(CALCULATED_BASES):
            for p in _find_parquets(modeling_root, b):
                entries.append((b, p))
    elif stage == "coords":
        for b in bases or list(ARTICLE_BASES):
            for p in _find_parquets(coords_root, b):
                entries.append((b, p))
    elif stage == "fusion":
        ms = methods or ["ewma_lags", "minirocket", "champion"]
        for b in bases or list(ARTICLE_BASES):
            bdir = fusion_root / b
            if not bdir.is_dir():
                continue
            for m in ms:
                for p in sorted((bdir / m).glob("inmet_bdq_*_cerrado.parquet")):
                    entries.append((f"{b}/{m}", p))
    else:
        raise ValueError(f"Estagio invalido: {stage!r}")
    return entries


# ----------------------------------------------------------------------
# Relatorio: markdown + csv + json
# ----------------------------------------------------------------------
def _render_summary_md(
    stage: str, ts: str, rows: List[Dict[str, Any]], totals: Dict[str, Any]
) -> str:
    ok = totals["ok"] + totals["soft_dup"]
    bad = totals["duplicated"] + totals["error"] + totals["no_keys"]
    header = "OK" if bad == 0 and ok > 0 else "FAIL"
    lines: List[str] = []
    lines.append(f"# Audit: `{stage}` — {ts}")
    lines.append("")
    lines.append(f"- **Status:** **{header}**")
    lines.append(f"- **Total files:** {totals['total']}")
    lines.append(f"- **OK:** {totals['ok']}")
    lines.append(f"- **Soft dup (multi-foco genuino, <1.10x):** {totals['soft_dup']}")
    lines.append(f"- **Duplicated (bug, >=1.10x):** {totals['duplicated']}")
    lines.append(f"- **No keys:** {totals['no_keys']}")
    lines.append(f"- **Error:** {totals['error']}")
    lines.append(f"- **Empty:** {totals['empty']}")
    lines.append("")

    # Agrupa por base/group
    by_group: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        by_group.setdefault(r["group"], []).append(r)

    for g in sorted(by_group.keys()):
        entries = by_group[g]
        g_ok = sum(1 for e in entries if e["status"] in ("OK", "SOFT_DUP"))
        g_tot = len(entries)
        lines.append(f"## `{g}`  ({g_ok}/{g_tot} OK)")
        lines.append("")
        lines.append("| File | Rows | Unique keys | Ratio | Status | Notes |")
        lines.append("|---|---:|---:|---:|---|---|")
        for e in sorted(entries, key=lambda x: x["file"]):
            rows_s = f"{e['rows']:,}" if e["rows"] else "-"
            uniq_s = f"{e['unique_keys']:,}" if e.get("unique_keys") else "-"
            ratio_s = f"{e['dup_ratio']}x" if e.get("dup_ratio") is not None else "-"
            lines.append(
                f"| `{e['file']}` | {rows_s} | {uniq_s} | {ratio_s} "
                f"| {e['status']} | {e.get('notes') or ''} |"
            )
        lines.append("")

    if bad > 0:
        lines.append("## Action required")
        lines.append("")
        if totals["duplicated"] > 0:
            lines.append(
                "- Arquivos com `dup_ratio > 1.01x` detectados. Se estiver em `modeling/`, "
                "rode `make dedupe` e depois regenere `make physics-features`, "
                "`make pipeline-coords` e `make champion-overwrite`."
            )
        if totals["no_keys"] > 0:
            lines.append("- Arquivos sem colunas `(cidade_norm, ts_hour)` — verifique o build.")
        if totals["error"] > 0:
            lines.append("- Erros de leitura — veja a coluna `Notes`.")
        lines.append("")
    return "\n".join(lines) + "\n"


def _write_reports(
    out_dir: Path,
    stage: str,
    ts: str,
    rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    totals = {
        "total": len(rows),
        "ok": sum(1 for r in rows if r["status"] == "OK"),
        "soft_dup": sum(1 for r in rows if r["status"] == "SOFT_DUP"),
        "duplicated": sum(1 for r in rows if r["status"] == "DUPLICATED"),
        "no_keys": sum(1 for r in rows if r["status"] == "NO_KEYS"),
        "error": sum(1 for r in rows if r["status"] == "ERROR"),
        "empty": sum(1 for r in rows if r["status"] == "EMPTY"),
    }
    # summary.md
    (out_dir / "summary.md").write_text(
        _render_summary_md(stage, ts, rows, totals), encoding="utf-8"
    )
    # per_file.csv
    cols = ["group", "file", "rows", "unique_keys", "dup_ratio", "status", "notes", "path"]
    with (out_dir / "per_file.csv").open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for r in rows:
            w.writerow({c: r.get(c, "") for c in cols})
    # raw.json
    with (out_dir / "raw.json").open("w", encoding="utf-8") as fh:
        json.dump(
            {"stage": stage, "timestamp": ts, "totals": totals, "rows": rows},
            fh, indent=2, ensure_ascii=False
        )
    return totals


def _update_latest(audits_root: Path, stage_results: Dict[str, Dict[str, Any]]) -> None:
    """Escreve LATEST.md com pointer para o relatorio mais recente de cada estagio."""
    lines: List[str] = []
    lines.append("# Latest audits")
    lines.append("")
    lines.append("Aponta para o ultimo relatorio de cada estagio do pipeline.")
    lines.append("")
    lines.append("| Stage | Status | Total | OK | Soft dup | Duplicated | Path |")
    lines.append("|---|---|---:|---:|---:|---:|---|")
    for stage in ("modeling", "calculated", "coords", "fusion"):
        sr = stage_results.get(stage)
        if not sr:
            lines.append(f"| `{stage}` | - | - | - | - | - | (nao rodado) |")
            continue
        tot = sr["totals"]
        status = "OK" if tot["duplicated"] == 0 and tot["error"] == 0 else "FAIL"
        rel = Path(sr["dir"]).relative_to(audits_root).as_posix()
        lines.append(
            f"| `{stage}` | {status} | {tot['total']} | {tot['ok']} "
            f"| {tot['soft_dup']} | {tot['duplicated']} "
            f"| [{rel}/summary.md]({rel}/summary.md) |"
        )
    (audits_root / "LATEST.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


# ----------------------------------------------------------------------
# Entrada publica
# ----------------------------------------------------------------------
def run(
    stages: Iterable[str],
    bases: Optional[List[str]] = None,
    methods: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Any]]:
    cfg = loadConfig()
    log = get_logger("audit.pipeline")
    article_root = Path(cfg["paths"]["data"].get("article") or
                        (_project_root / "data" / "_article"))
    audits_root = article_root / "_audits"
    audits_root.mkdir(parents=True, exist_ok=True)

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    stage_results: Dict[str, Dict[str, Any]] = {}

    for stage in stages:
        log.info(f"=== stage={stage} ===")
        entries = _discover_stage(stage, cfg, bases=bases, methods=methods)
        if not entries:
            log.warning(f"[{stage}] nenhum parquet encontrado. skip.")
            continue
        rows: List[Dict[str, Any]] = []
        for group, path in entries:
            r = _audit_parquet(path)
            r["group"] = group
            rows.append(r)
            flag = "" if r["status"] == "OK" else f" [{r['status']}]"
            log.info(
                f"  {group}/{path.name}: rows={r['rows']} "
                f"unique={r.get('unique_keys')} ratio={r.get('dup_ratio')}x{flag}"
            )
        out_dir = audits_root / f"{ts}_{stage}"
        totals = _write_reports(out_dir, stage, ts, rows)
        stage_results[stage] = {"dir": str(out_dir), "totals": totals}
        log.info(
            f"[{stage}] total={totals['total']} ok={totals['ok']} "
            f"dup={totals['duplicated']} err={totals['error']} -> {out_dir}"
        )

    # Merge com LATEST pre-existente: preserva estagios nao rodados agora.
    latest_path = audits_root / "LATEST.md"
    # (simples: sempre re-escreve com o que rodou agora; estagios anteriores ficam
    # nos diretorios timestamped, acessiveis via filesystem)
    if stage_results:
        _update_latest(audits_root, stage_results)
        log.info(f"[LATEST] {latest_path}")
    return stage_results


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--stage", default="all",
                   choices=["all", "modeling", "calculated", "coords", "fusion"])
    p.add_argument("--bases", nargs="+", default=None,
                   help="Subset de bases (default: tudo que faz sentido no estagio).")
    p.add_argument("--methods", nargs="+", default=None,
                   help="Para --stage fusion: subset de ewma_lags/minirocket/champion/sarimax_exog.")
    args = p.parse_args()
    stages = ["modeling", "calculated", "coords", "fusion"] if args.stage == "all" else [args.stage]
    run(stages, bases=args.bases, methods=args.methods)


if __name__ == "__main__":
    main()
