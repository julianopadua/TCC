# src/article/audit_fusion_dataset.py
# =============================================================================
# Auditoria de schema/colunas entre parquets por metodo (ewma_lags, minirocket,
# champion, sarimax_exog) dentro de 1_datasets_with_fusion/{cenario}/.
#
# Uso:
#   python -m src.article.audit_fusion_dataset --scenario base_E_with_rad_knn_calculated
#   python -m src.article.audit_fusion_dataset --scenario-dir "D:/.../base_E_with_rad_knn_calculated"
#
# Gera audit.md na raiz do cenario (ao lado de ewma_lags/, champion/, ...).
# =============================================================================
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

_project_root = Path(__file__).resolve().parents[2]
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

import src.utils as utils  # noqa: E402

try:
    import pyarrow.parquet as pq
except ImportError:
    pq = None  # type: ignore

try:
    import pandas as pd  # lazy stats (só para --deep)
except Exception:  # pragma: no cover
    pd = None  # type: ignore

# Colunas que o train_runner e o artigo assumem em geral.
CORE_LABEL_KEYS = ("cidade_norm", "ts_hour", "HAS_FOCO", "ANO")

# Quando --deep estiver ligado, lemos apenas estas colunas para estatistica
# leve (NaN ratio, pos_rate). O custo por arquivo é O(linhas * len(cols)).
_DEEP_STATS_COLS = (
    "ANO",
    "HAS_FOCO",
    "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
    "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
    "UMIDADE RELATIVA DO AR, HORARIA (%)",
    "RADIACAO GLOBAL (KJ/m²)",
    "NDVI_buffer",
    "EVI_buffer",
)

# Intervalo explicitamente validado pelo usuario apos re-geracao da base
# champion. O audit sinaliza se cobertura e qualidade deles bate com os anos
# "antigos" (ex.: 2007+).
RECENTLY_GENERATED_YEARS = (2003, 2004, 2005, 2006)

METHOD_SUBDIRS = (
    "ewma_lags",
    "sarimax_exog",
    "minirocket",
    "champion",
)


@dataclass
class FileAudit:
    path: Path
    year: Optional[int]
    num_rows: int
    column_names: Tuple[str, ...]
    dtypes: Dict[str, str]


@dataclass
class YearStats:
    """Estatistica por ano calculada na passagem --deep."""
    year: int
    rows: int
    pos_count: int
    nan_ratios: Dict[str, float] = field(default_factory=dict)

    @property
    def pos_rate(self) -> float:
        return (self.pos_count / self.rows) if self.rows else 0.0


@dataclass
class MethodAudit:
    name: str
    path: Path
    files: List[FileAudit] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    year_stats: Dict[int, YearStats] = field(default_factory=dict)


def _year_from_name(name: str) -> Optional[int]:
    import re

    m = re.search(r"(?:^|[_\-])(\d{4})(?:[_\-]|\.parquet$)", name, re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


def _schema_to_dtypes(schema) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for i in range(len(schema)):
        f = schema[i]
        out[f.name] = str(f.type)
    return out


def _audit_single_parquet(path: Path) -> FileAudit:
    if pq is None:
        raise RuntimeError("pyarrow e obrigatorio para a auditoria.")
    pf = pq.ParquetFile(str(path))
    meta = pf.metadata
    schema = pf.schema_arrow
    names = tuple(schema.names)
    dtypes = _schema_to_dtypes(schema)
    return FileAudit(
        path=path,
        year=_year_from_name(path.name),
        num_rows=int(meta.num_rows),
        column_names=names,
        dtypes=dtypes,
    )


def _compare_schemas(
    files: List[FileAudit],
) -> Tuple[Set[str], List[str], List[str]]:
    """Retorna (colunas_comuns_estritas, issues, warnings)."""
    if not files:
        return set(), ["nenhum parquet"], []

    issues: List[str] = []
    warnings: List[str] = []

    ref = sorted(files, key=lambda f: f.path.name)[0]
    ref_set = set(ref.column_names)

    for c in CORE_LABEL_KEYS:
        if c not in ref_set:
            issues.append(f"coluna core ausente no primeiro arquivo ({ref.path.name}): {c}")

    for fa in files[1:]:
        s = set(fa.column_names)
        if s != ref_set:
            only_ref = sorted(ref_set - s)
            only_other = sorted(s - ref_set)
            if only_ref:
                issues.append(
                    f"{fa.path.name}: faltam colunas vs referencia: {only_ref[:12]}"
                    + ("..." if len(only_ref) > 12 else "")
                )
            if only_other:
                issues.append(
                    f"{fa.path.name}: colunas extras vs referencia: {only_other[:12]}"
                    + ("..." if len(only_other) > 12 else "")
                )

    # dtypes: para cada coluna em intersecao, todos devem coincidir
    common = ref_set.copy()
    for fa in files[1:]:
        common &= set(fa.column_names)

    for col in sorted(common):
        dtypes_seen: Dict[str, List[str]] = defaultdict(list)
        for fa in files:
            if col in fa.dtypes:
                dtypes_seen[fa.dtypes[col]].append(fa.path.name)
        if len(dtypes_seen) <= 1:
            continue
        keys = list(dtypes_seen.keys())
        # float vs double: comum ao longo dos anos; não quebra leitura pandas/pyarrow.
        if _only_float_precision_drift(keys):
            warnings.append(
                f"__FLOAT_DRIFT__ '{col}': "
                + "; ".join(f"{k} em {len(v)} ficheiro(s)" for k, v in dtypes_seen.items())
            )
        else:
            warnings.append(
                f"tipo divergente para '{col}': "
                + "; ".join(f"{k} em {len(v)} ficheiro(s)" for k, v in dtypes_seen.items())
            )

    return common, issues, warnings


def _only_float_precision_drift(type_keys: List[str]) -> bool:
    kinds = set()
    for k in type_keys:
        kl = k.lower()
        if "double" in kl or "float64" in kl:
            kinds.add("f64")
        elif "float" in kl or "float32" in kl:
            kinds.add("f32")
        else:
            return False
    return kinds == {"f32", "f64"}


def _collect_year_stats(path: Path) -> Optional[YearStats]:
    """Le colunas do subset _DEEP_STATS_COLS e devolve YearStats ou None."""
    if pq is None or pd is None:
        return None
    y = _year_from_name(path.name)
    if y is None:
        return None
    try:
        pf = pq.ParquetFile(str(path))
        avail = set(pf.schema_arrow.names)
        cols = [c for c in _DEEP_STATS_COLS if c in avail]
        if not cols:
            return YearStats(year=y, rows=int(pf.metadata.num_rows), pos_count=0)

        total_rows = 0
        pos = 0
        nan_sum: Dict[str, int] = {c: 0 for c in cols if c != "ANO"}
        for batch in pf.iter_batches(batch_size=500_000, columns=cols):
            df = batch.to_pandas()
            n = len(df)
            total_rows += n
            if "HAS_FOCO" in df.columns:
                pos += int(pd.to_numeric(df["HAS_FOCO"], errors="coerce").fillna(0).astype(int).sum())
            for c in nan_sum:
                if c in df.columns:
                    nan_sum[c] += int(df[c].isna().sum())

        nan_ratios = {
            c: (nan_sum[c] / total_rows) if total_rows else 0.0
            for c in nan_sum
        }
        return YearStats(year=y, rows=total_rows, pos_count=pos, nan_ratios=nan_ratios)
    except Exception:
        return None


def audit_method_dir(method_dir: Path, *, deep: bool = False) -> MethodAudit:
    name = method_dir.name
    ma = MethodAudit(name=name, path=method_dir)
    if not method_dir.is_dir():
        ma.errors.append(f"pasta inexistente: {method_dir}")
        return ma

    parquets = sorted(method_dir.glob("*.parquet"))
    if not parquets:
        ma.warnings.append("nenhum *.parquet")
        return ma

    for p in parquets:
        try:
            ma.files.append(_audit_single_parquet(p))
        except Exception as exc:
            ma.errors.append(f"{p.name}: leitura schema falhou — {exc}")

    if ma.files:
        _, issues, warns = _compare_schemas(ma.files)
        ma.errors.extend(issues)
        ma.warnings.extend(warns)

    if deep:
        for p in parquets:
            ys = _collect_year_stats(p)
            if ys is not None:
                ma.year_stats[ys.year] = ys

    return ma


def _count_tsf(cols: Tuple[str, ...]) -> int:
    return sum(1 for c in cols if c.startswith("tsf_"))


def _years_set(audits: List[MethodAudit]) -> Dict[str, Set[int]]:
    out: Dict[str, Set[int]] = {}
    for ma in audits:
        ys = {f.year for f in ma.files if f.year is not None}
        out[ma.name] = ys
    return out


def _split_method_warnings(
    warnings: List[str],
) -> Tuple[List[str], List[str]]:
    """Separa avisos float32/float64 (leves) dos restantes."""
    light: List[str] = []
    heavy: List[str] = []
    for w in warnings:
        if w.startswith("__FLOAT_DRIFT__"):
            light.append(w.replace("__FLOAT_DRIFT__ ", "", 1))
        else:
            heavy.append(w)
    return light, heavy


def _year_alignment_note(ysets: Dict[str, Set[int]]) -> List[str]:
    """Texto sobre alinhamento de anos (ignora sarimax incompleto como caso à parte)."""
    lines: List[str] = []
    if not ysets:
        return lines
    full_methods = ("ewma_lags", "minirocket", "champion")
    core_sets = [ysets[m] for m in full_methods if m in ysets and ysets[m]]
    if len(core_sets) >= 2:
        base = core_sets[0]
        mismatch = any(base != s for s in core_sets[1:])
        if mismatch:
            lines.append(
                "Conjuntos de anos **diferem** entre ewma_lags / minirocket / champion — investigar."
            )
        else:
            lines.append(
                "Anos **alinhados** entre `ewma_lags`, `minirocket` e `champion` "
                f"({len(base)} anos)."
            )
    elif len(core_sets) == 1:
        lines.append(
            f"Apenas um método completo com parquets ({len(core_sets[0])} anos)."
        )
    if "sarimax_exog" in ysets:
        sn = len(ysets["sarimax_exog"])
        others = {k: len(v) for k, v in ysets.items() if k != "sarimax_exog"}
        mx = max(others.values()) if others else 0
        if sn < mx:
            lines.append(
                f"`sarimax_exog`: apenas **{sn}** ano(s) de parquet (cobertura parcial vs "
                f"{mx} nos outros métodos — normal se o pipeline não gerou todos os anos)."
            )
    return lines


def _compare_rowcounts_to_coords(
    coords_dir: Path, method_audits: List[MethodAudit]
) -> List[str]:
    """Por ano, compara num_rows do coords vs cada metodo."""
    lines: List[str] = []
    if not coords_dir.is_dir():
        return [f"coords_dir invalido: {coords_dir}"]

    coord_rows: Dict[int, int] = {}
    for p in sorted(coords_dir.glob("*.parquet")):
        y = _year_from_name(p.name)
        if y is None:
            continue
        try:
            coord_rows[y] = int(pq.ParquetFile(str(p)).metadata.num_rows)
        except Exception as exc:
            lines.append(f"coords {p.name}: {exc}")
            return lines

    for ma in method_audits:
        if not ma.files:
            continue
        for fa in ma.files:
            if fa.year is None:
                continue
            cr = coord_rows.get(fa.year)
            if cr is None:
                lines.append(
                    f"[{ma.name}] {fa.path.name}: ano {fa.year} sem parquet correspondente em coords"
                )
                continue
            if fa.num_rows != cr:
                lines.append(
                    f"[{ma.name}] {fa.year}: rows={fa.num_rows} vs coords={cr} (delta {fa.num_rows - cr})"
                )
    return lines


def render_audit_md(
    scenario_root: Path,
    method_audits: List[MethodAudit],
    coords_notes: List[str],
    *,
    generated_utc: Optional[str] = None,
) -> str:
    ts = generated_utc or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines: List[str] = []
    lines.append(f"# Auditoria — `{scenario_root.name}`")
    lines.append("")
    lines.append(f"**Gerado em:** {ts}")
    lines.append(f"**Caminho:** `{scenario_root.resolve()}`")
    lines.append("")
    lines.append("Este ficheiro resume consistência de **schema**, **colunas** e **contagens de linhas** ")
    lines.append("entre todos os parquets por subpasta de método (`ewma_lags`, `minirocket`, `champion`, …).")
    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## Resumo executivo")
    lines.append("")

    any_err = any(m.errors for m in method_audits)
    light_warn = heavy_warn = False
    for m in method_audits:
        light, heavy = _split_method_warnings(m.warnings)
        if light:
            light_warn = True
        if heavy:
            heavy_warn = True
    coords_mismatch = any(
        ("vs coords=" in n or "delta " in n) for n in coords_notes
    )

    if not any_err and not heavy_warn and not coords_mismatch and not light_warn:
        lines.append("- **Estado:** **OK** — colunas idênticas entre ficheiros por método; ")
        lines.append("coords com mesmo `num_rows` por ano.")
    elif any_err:
        lines.append("- **Estado:** **FALHA** — ver erros de colunas/schema nas secções.")
    elif not any_err and not heavy_warn and not coords_mismatch and light_warn:
        lines.append("- **Estado:** **OK (leve)** — apenas deriva float32/float64 entre anos (comum).")
    else:
        lines.append("- **Estado:** **AVISOS** — ver tipos relevantes ou coords abaixo.")
    lines.append("")

    # Cobertura de anos entre métodos
    ysets = _years_set(method_audits)
    if ysets:
        lines.append("### Cobertura de anos (por método)")
        lines.append("")
        lines.append("| Método | Anos (n) |")
        lines.append("|--------|-----------|")
        for name in sorted(ysets.keys()):
            ys = sorted(ysets[name])
            lines.append(f"| {name} | {len(ys)} |")
        lines.append("")
        for yn in _year_alignment_note(ysets):
            lines.append(f"- {yn}")
        lines.append("")

    lines.append("---")
    lines.append("")

    for ma in method_audits:
        lines.append(f"## `{ma.name}/`")
        lines.append("")
        if ma.errors:
            lines.append("### Erros / inconsistências")
            for e in ma.errors:
                lines.append(f"- {e}")
            lines.append("")
        light, heavy = _split_method_warnings(ma.warnings)
        if light:
            lines.append("### Avisos leves (precisão float)")
            lines.append(
                f"- {len(light)} coluna(s) com `float` vs `double` entre ficheiros "
                "(comum entre anos). Leitura em pandas unifica sem perda de desenho experimental."
            )
            lines.append("")
        if heavy:
            lines.append("### Avisos relevantes")
            for w in heavy:
                lines.append(f"- {w}")
            lines.append("")

        if not ma.files:
            lines.append("*Sem ficheiros parquet.*")
            lines.append("")
            continue

        ref0 = sorted(ma.files, key=lambda x: x.path.name)[0]
        ref_cols = len(ref0.column_names)
        tsf_n = _count_tsf(ref0.column_names)
        lines.append(
            f"- **Ficheiros:** {len(ma.files)} | **Colunas (ref.):** {ref_cols} | **tsf_*:** {tsf_n}"
        )
        lines.append("")
        lines.append("| Ano | Ficheiro | Linhas | Colunas | tsf_* |")
        lines.append("|-----|----------|--------|---------|-------|")
        for fa in sorted(ma.files, key=lambda x: (x.year or 0, x.path.name)):
            yn = fa.year if fa.year is not None else "?"
            lines.append(
                f"| {yn} | `{fa.path.name}` | {fa.num_rows:,} | {len(fa.column_names)} | {_count_tsf(fa.column_names)} |"
            )
        lines.append("")
        lines.append("### Colunas (referência: primeiro ficheiro ordenado)")
        lines.append("")
        ref = sorted(ma.files, key=lambda x: x.path.name)[0]
        # quebra em linhas para legibilidade
        cols = list(ref.column_names)
        chunk = 8
        for i in range(0, len(cols), chunk):
            lines.append("- " + ", ".join(f"`{c}`" for c in cols[i : i + chunk]))
        lines.append("")
        lines.append("---")
        lines.append("")

    if coords_notes:
        lines.append("## Comparação com `0_datasets_with_coords` (mesmo cenário)")
        lines.append("")
        if any("delta" in n or "vs coords" in n for n in coords_notes):
            lines.append(
                "Por ano, **`num_rows`** de cada método deve coincidir com o parquet de **coords** "
                "(mesma base de observações horárias)."
            )
            lines.append("")
            if any("[champion]" in n and "delta" in n for n in coords_notes):
                lines.append(
                    "**Interpretação:** se `champion` tem **mais** linhas que `coords`, costuma ser "
                    "efeito de chaves duplicadas no merge (produto cartesiano). O `article_orchestrator` "
                    "deduplica `(cidade_norm, ts_hour)` no `feat_df` antes do join — regenere champion "
                    "com `--overwrite` se estes deltas forem inesperados."
                )
                lines.append("")
            lines.append("Divergências registadas:")
        else:
            lines.append("Sem divergências de contagem ou notas:")
        lines.append("")
        for n in coords_notes:
            lines.append(f"- {n}")
        lines.append("")

    # Deep stats (ligado por --deep): positive rate + NaN ratios por ano.
    deep_any = any(m.year_stats for m in method_audits)
    if deep_any:
        lines.append("## Integridade por ano (`--deep`)")
        lines.append("")
        lines.append(
            "Estatisticas calculadas lendo um subconjunto de colunas (ANO, HAS_FOCO, "
            "meteo-core, NDVI/EVI). Use para validar anos recem regenerados contra o resto da base."
        )
        lines.append("")

        for ma in method_audits:
            if not ma.year_stats:
                continue
            lines.append(f"### `{ma.name}/`")
            lines.append("")
            lines.append("| Ano | Linhas | Pos (HAS_FOCO=1) | pos_rate | NaN ratio (colunas amostradas) |")
            lines.append("|-----|--------|------------------|----------|---------------------------------|")
            for y in sorted(ma.year_stats):
                ys = ma.year_stats[y]
                nans = ", ".join(
                    f"`{c}`={r:.4f}" for c, r in sorted(ys.nan_ratios.items()) if r > 0
                )
                if not nans:
                    nans = "sem NaN nas amostradas"
                lines.append(
                    f"| {ys.year} | {ys.rows:,} | {ys.pos_count:,} | {ys.pos_rate:.4%} | {nans} |"
                )
            lines.append("")

            # Validacao especifica dos anos recem regenerados.
            yrs_here = set(ma.year_stats.keys())
            check_years = [y for y in RECENTLY_GENERATED_YEARS if y in yrs_here]
            other_years = sorted(yrs_here - set(RECENTLY_GENERATED_YEARS))
            if check_years and other_years:
                avg_pos_rate_others = sum(
                    ma.year_stats[y].pos_rate for y in other_years
                ) / len(other_years)
                avg_pos_rate_new = sum(
                    ma.year_stats[y].pos_rate for y in check_years
                ) / len(check_years)
                lines.append(f"**Validacao {RECENTLY_GENERATED_YEARS[0]}-{RECENTLY_GENERATED_YEARS[-1]}**")
                lines.append("")
                lines.append(
                    f"- pos_rate medio (anos recentes {check_years}): **{avg_pos_rate_new:.4%}**"
                )
                lines.append(
                    f"- pos_rate medio (demais anos, n={len(other_years)}): **{avg_pos_rate_others:.4%}**"
                )
                # Sinais simples
                if avg_pos_rate_new == 0 and avg_pos_rate_others > 0:
                    lines.append(
                        "- ⚠️  Anos recentes com **0 focos** — provavelmente o merge com bdq nao ocorreu para esses anos."
                    )
                elif avg_pos_rate_others > 0 and abs(avg_pos_rate_new - avg_pos_rate_others) / avg_pos_rate_others > 5.0:
                    lines.append(
                        "- ⚠️  Divergencia grande de pos_rate vs demais anos; inspecionar ingestao."
                    )
                else:
                    lines.append(
                        "- OK: pos_rate dos anos recentes em ordem de grandeza compativel com o resto da base."
                    )
                # NaN sanity: se anos recentes tiverem >50% NaN em colunas meteo-core, provavel falha de join.
                bad_nan = []
                for y in check_years:
                    for c, r in ma.year_stats[y].nan_ratios.items():
                        if r > 0.5:
                            bad_nan.append((y, c, r))
                if bad_nan:
                    lines.append(
                        "- ⚠️  NaN>50% nas colunas amostradas: "
                        + "; ".join(f"{y}/{c}={r:.2f}" for y, c, r in bad_nan)
                    )
                lines.append("")

        lines.append("---")
        lines.append("")

    lines.append("## Como regenerar")
    lines.append("")
    lines.append("```bash")
    lines.append(
        f"python -m src.article.audit_fusion_dataset --scenario {scenario_root.name} --deep"
    )
    lines.append("```")
    lines.append("")
    return "\n".join(lines)


def resolve_scenario_root(
    scenario_name: Optional[str], scenario_dir: Optional[Path]
) -> Path:
    if scenario_dir is not None:
        return scenario_dir.resolve()
    if not scenario_name:
        raise ValueError("Informe --scenario ou --scenario-dir")
    cfg = utils.loadConfig()
    root = Path(cfg["paths"]["data"]["article"]) / "1_datasets_with_fusion" / scenario_name
    return root.resolve()


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="Audita parquets de fusão temporal (artigo) e gera audit.md."
    )
    ap.add_argument(
        "--scenario",
        type=str,
        default=None,
        help="Nome da pasta do cenário sob 1_datasets_with_fusion (ex.: base_E_with_rad_knn_calculated).",
    )
    ap.add_argument(
        "--scenario-dir",
        type=Path,
        default=None,
        help="Caminho absoluto para a raiz do cenário (alternativa a --scenario).",
    )
    ap.add_argument(
        "--coords-scenario",
        type=str,
        default=None,
        help="Nome da pasta em 0_datasets_with_coords para comparar num_rows (default: mesmo nome do cenário).",
    )
    ap.add_argument(
        "--no-coords-compare",
        action="store_true",
        help="Não comparar contagens com a base de coords.",
    )
    ap.add_argument(
        "--deep",
        action="store_true",
        help=(
            "Calcula estatisticas por ano (pos_rate, NaN ratios) lendo um subset de "
            "colunas. Valida integracao de anos recem gerados (ex.: 2003-2006)."
        ),
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Ficheiro de saída (default: <scenario>/audit.md).",
    )
    ap.add_argument(
        "--json",
        type=Path,
        default=None,
        help="Opcional: dump estruturado JSON para CI.",
    )
    args = ap.parse_args(argv)

    if pq is None:
        print("ERRO: instale pyarrow.", file=sys.stderr)
        return 2

    try:
        scenario_root = resolve_scenario_root(args.scenario, args.scenario_dir)
    except Exception as exc:
        print(exc, file=sys.stderr)
        return 2

    if not scenario_root.is_dir():
        print(f"ERRO: pasta inexistente: {scenario_root}", file=sys.stderr)
        return 2

    method_audits: List[MethodAudit] = []
    for sub in METHOD_SUBDIRS:
        d = scenario_root / sub
        if d.is_dir():
            method_audits.append(audit_method_dir(d, deep=bool(args.deep)))

    coords_notes: List[str] = []
    if not args.no_coords_compare:
        scen = args.coords_scenario or scenario_root.name
        cfg = utils.loadConfig()
        cr = utils.article_coords_root(cfg) / scen
        coords_notes = _compare_rowcounts_to_coords(cr, method_audits)

    md = render_audit_md(scenario_root, method_audits, coords_notes)
    out = args.output or (scenario_root / "audit.md")
    out.write_text(md, encoding="utf-8")
    print(f"Escrito: {out}")

    if args.json:
        payload = {
            "scenario_root": str(scenario_root),
            "methods": {
                m.name: {
                    "errors": m.errors,
                    "warnings": m.warnings,
                    "n_files": len(m.files),
                }
                for m in method_audits
            },
            "coords_notes": coords_notes,
        }
        args.json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Escrito: {args.json}")

    failed = any(m.errors for m in method_audits)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
