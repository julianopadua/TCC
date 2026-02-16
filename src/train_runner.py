# src/train_runner.py
# =============================================================================
# ORQUESTRADOR DE EXPERIMENTOS (BATCHED LOAD + CLI MULTI-VARIACOES)
# =============================================================================
# Objetivos desta versao:
# 1) Carregar os parquets em batches (ano a ano) para reduzir pico de memoria
# 2) Voltar ao menu "antigo" para Logistic/XGBoost: base + 3 opcoes com GridSearch
# 3) Permitir selecionar MULTIPLAS variacoes por modelo (ex: 1,2,4)
# 4) Parser de selecao robusto: "3,4" "3, 4" "3 4" "3;4" etc.
# 5) Nunca usar o caractere "—"
# =============================================================================

import gc
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# Path Setup
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

try:
    import src.utils as utils
    from src.ml.core import TemporalSplitter, MemoryMonitor
    from src.models.logistic import LogisticTrainer
    from src.models.dummy import DummyTrainer
    from src.models.xgboost_model import XGBoostTrainer
except ImportError as e:
    sys.exit(f"[CRITICAL] Dependencias: {e}")


# ----------------------------
# Helpers
# ----------------------------
def _pos_rate(y: pd.Series) -> float:
    try:
        n = len(y)
        if n == 0:
            return 0.0
        return float(y.sum()) / float(n)
    except Exception:
        return 0.0


def _safe_int_series(s: pd.Series, dtype: str = "int32") -> pd.Series:
    s2 = pd.to_numeric(s, errors="coerce")
    s2 = s2.dropna().astype(dtype)
    return s2


def _downcast_floats(df: pd.DataFrame) -> None:
    # float64 -> float32 (bem efetivo em RAM)
    cols = df.select_dtypes(include=["float64"]).columns
    for c in cols:
        df[c] = df[c].astype("float32")


def _coerce_binary_target(df: pd.DataFrame, target: str) -> None:
    # garante 0/1 em int8 para reduzir RAM e evitar int64 gigante
    df[target] = pd.to_numeric(df[target], errors="coerce")
    df.dropna(subset=[target], inplace=True)
    df[target] = df[target].astype("int8")


def _parse_int_tokens(text: str) -> List[int]:
    """
    Aceita: "1,3" "1, 3" "1 3" "1;3" "1|3" etc.
    Retorna lista de ints (sem duplicatas, preservando ordem de aparicao).
    """
    t = (text or "").strip().lower()
    if not t:
        return []
    parts = re.split(r"[,\s;|]+", t)
    out: List[int] = []
    seen = set()
    for p in parts:
        if not p:
            continue
        try:
            v = int(p)
            if v not in seen:
                out.append(v)
                seen.add(v)
        except Exception:
            continue
    return out


def _select_many(opts: Dict[int, Any], title: str) -> List[Any]:
    print(f"\n--- {title} ---")
    for k, v in opts.items():
        print(f"[{k}] {v}")

    while True:
        x = input(">> Select (ex: 1,3 | 1 3 | 1;3 | 'all'): ").strip().lower()
        if x == "all":
            return list(opts.values())

        toks = _parse_int_tokens(x)
        if not toks:
            print("Entrada invalida. Tente novamente.")
            continue

        chosen = [opts[i] for i in toks if i in opts]
        if chosen:
            return chosen

        print("Nenhuma opcao valida selecionada. Tente novamente.")


def _select_one(opts: Dict[int, Any], title: str, default_key: Optional[int] = None) -> Any:
    print(f"\n--- {title} ---")
    for k, v in opts.items():
        print(f"[{k}] {v}")
    suffix = f" [default {default_key}]" if default_key is not None else ""
    while True:
        x = input(f">> Select{suffix}: ").strip().lower()
        if not x and default_key is not None:
            return opts[default_key]
        toks = _parse_int_tokens(x)
        if len(toks) == 1 and toks[0] in opts:
            return opts[toks[0]]
        print("Opcao invalida. Tente novamente.")


def _downsample_keep_all_pos(
    df: pd.DataFrame,
    target: str,
    max_rows_remaining: Optional[int],
    neg_pos_ratio: int,
    min_neg_keep: int,
    seed: int,
) -> pd.DataFrame:
    """
    Mantem 100% dos positivos.
    Mantem negativos por amostragem, limitado por:
      - max_rows_remaining (orcamento do split)
      - neg_pos_ratio (no maximo X negativos por positivo)
      - min_neg_keep (garante um minimo de negativos mesmo se pos=0)
    """
    if df is None or df.empty:
        return df

    pos = df[df[target] == 1]
    neg = df[df[target] == 0]

    n_pos = int(len(pos))

    # limite por razao
    neg_cap_by_ratio = max(int(min_neg_keep), int(n_pos) * int(neg_pos_ratio))

    # limite por orcamento do split
    if max_rows_remaining is None:
        neg_cap_by_budget = len(neg)
    else:
        budget_for_neg = max(0, int(max_rows_remaining) - n_pos)
        neg_cap_by_budget = int(budget_for_neg)

    n_neg_keep = int(min(len(neg), neg_cap_by_ratio, neg_cap_by_budget))

    if n_neg_keep < len(neg):
        neg = neg.sample(n=n_neg_keep, random_state=int(seed))

    # Se o orcamento ficar menor que o numero de positivos (caso extremo), amostra positivos
    if max_rows_remaining is not None and n_pos > int(max_rows_remaining):
        pos = pos.sample(n=int(max_rows_remaining), random_state=int(seed))

    out = pd.concat([pos, neg], ignore_index=True)
    return out


# ----------------------------
# Variacao (4 opcoes)
# ----------------------------
@dataclass(frozen=True)
class VariationOption:
    key: int
    label: str
    settings: Dict[str, Any]


def _variation_menu_legacy(model_key: str) -> List[VariationOption]:
    """
    Menu antigo:
      1) Base (cru)
      2) GridSearch + SMOTE + Weight
      3) GridSearch + SMOTE (sem Weight)
      4) GridSearch + Weight (sem SMOTE)
    Observacao:
      - use_scale == Weight (class_weight / scale_pos_weight)
      - Para XGBoost em bases grandes, default de GridSearch mais leve:
        cv_splits=2 e grid_mode="fast"
    """
    base_common: Dict[str, Any] = {
        "cv_splits": 3,
        "scoring": "average_precision",
        "smote_sampling_strategy": 0.1,
        "smote_k_neighbors": 5,
        "thr": 0.5,
    }

    if model_key == "logistic":
        base_common["feature_scaling"] = True

    # Para XGBoost, GridSearch mais leve por padrao (evita dias por base)
    xgb_grid_common = dict(base_common)
    if model_key == "xgboost":
        xgb_grid_common["cv_splits"] = 2
        xgb_grid_common["grid_mode"] = "fast"  # usado pelo XGBoostTrainer
        xgb_grid_common["model_n_jobs"] = None  # auto (threads no fit), GridSearch fica serial

    grid_common = xgb_grid_common if model_key == "xgboost" else base_common

    return [
        VariationOption(
            1,
            "Base (cru) - sem SMOTE, sem GridSearch, sem peso",
            {**base_common, "optimize": False, "use_smote": False, "use_scale": False},
        ),
        VariationOption(
            2,
            "GridSearch + SMOTE + Weight",
            {**grid_common, "optimize": True, "use_smote": True, "use_scale": True},
        ),
        VariationOption(
            3,
            "GridSearch + SMOTE (sem Weight)",
            {**grid_common, "optimize": True, "use_smote": True, "use_scale": False},
        ),
        VariationOption(
            4,
            "GridSearch + Weight (sem SMOTE)",
            {**grid_common, "optimize": True, "use_smote": False, "use_scale": True},
        ),
    ]


def _variation_meta_from_settings(st: Dict[str, Any]) -> Tuple[str, List[str], str]:
    optimize = bool(st.get("optimize", False))
    use_smote = bool(st.get("use_smote", False))
    use_weight = bool(st.get("use_scale", False))

    tags: List[str] = []
    if optimize:
        tags.append("gridsearch")
    if use_smote:
        tags.append("smote")
    if use_weight:
        tags.append("weight")

    run_name = "base" if not tags else "_".join(tags)

    if not tags:
        desc = "Base (sem SMOTE, sem GridSearch, sem balanceamento por peso)"
    else:
        mapping = {"gridsearch": "GridSearchCV", "smote": "SMOTE", "weight": "balanceamento por peso"}
        desc = " + ".join(mapping[t] for t in tags)

    return run_name, tags, desc


# ----------------------------
# Data loading (batched by year-file)
# ----------------------------
_YEAR_RE = re.compile(r"(?:^|[_\-])(\d{4})(?:[_\-]|\.parquet$)", re.IGNORECASE)


def _year_from_filename(p: Path) -> Optional[int]:
    m = _YEAR_RE.search(p.name)
    if not m:
        return None
    try:
        return int(m.group(1))
    except Exception:
        return None


class TrainingOrchestrator:
    def __init__(self, scenario_key: str):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)

        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg["modeling_scenarios"].get(scenario_key)
        if not self.scenario_folder:
            raise ValueError(f"Cenario {scenario_key} invalido.")

        self.random_seed = int(self.cfg.get("project", {}).get("random_seed", 42))

        # Observacao: seus parquets usam acentos nos nomes INMET. Aqui mantemos exatamente como voce tinha antes.
        self.features = [
            "PRECIPITACAO TOTAL, HORARIO (mm)".replace("PRECIPITACAO", "PRECIPITAÇÃO").replace("HORARIO", "HORÁRIO"),
            "RADIACAO GLOBAL (KJ/m²)",
            "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)".replace("HORARIA", "HORARIA"),
            "UMIDADE RELATIVA DO AR, HORARIA (%)".replace("UMIDADE", "UMIDADE").replace("HORARIA", "HORARIA"),
            "VENTO, VELOCIDADE HORARIA (m/s)".replace("HORARIA", "HORARIA"),
            "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)".replace("PRESSAO", "PRESSAO").replace("HORARIA", "HORARIA"),
            "VENTO, RAJADA MAXIMA (m/s)",
            "precip_ewma",
            "dias_sem_chuva",
            "risco_temp_max",
            "risco_umid_critica",
            "risco_umid_alerta",
            "fator_propagacao",
        ]

        self.target = "HAS_FOCO"
        self.year_col = "ANO"

    def _discover_files(self) -> List[Path]:
        base_path = Path(self.cfg["paths"]["data"]["modeling"])
        path = base_path / self.scenario_folder
        self.log.info(f"[LOAD] scenario_folder={self.scenario_folder} | path={path}")
        files = sorted(path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"Sem parquets em {path}")
        return files

    def _select_columns(self, first_file: Path) -> Optional[List[str]]:
        cols = None
        try:
            import pyarrow.parquet as pq  # type: ignore

            avail = pq.read_schema(first_file).names
            cols = [c for c in self.features if c in avail] + [self.target, self.year_col]
            self.log.info(f"[LOAD] colunas selecionadas via schema: {len(cols)}")
        except Exception as e:
            self.log.warning(f"[LOAD] sem pyarrow/schema otimizado (vai ler colunas padrao): {e}")
        return cols

    def load_split_batched(
        self,
        test_size_years: int = 2,
        gap_years: int = 0,
        max_train_rows: Optional[int] = None,
        max_test_rows: Optional[int] = None,
        neg_pos_ratio: int = 200,
        min_neg_keep_per_chunk: int = 50_000,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """
        Carrega em batches por parquet (normalmente um parquet por ano).
        Evita criar um df gigante "full" antes do split, reduzindo pico de memoria.

        Se max_train_rows/max_test_rows forem definidos:
          - mantem 100% dos positivos
          - amostra negativos respeitando orcamento e neg_pos_ratio
        """
        files = self._discover_files()
        cols = self._select_columns(files[0])

        years: List[int] = []
        file_years: List[Tuple[Path, Optional[int]]] = []
        for f in files:
            y = _year_from_filename(f)
            file_years.append((f, y))
            if y is not None:
                years.append(y)

        if years:
            years = sorted(set(years))
            if len(years) < test_size_years + 1:
                raise ValueError("Anos insuficientes para split temporal (por filename).")
            cut = years[-test_size_years]
            train_max_year = int(cut - gap_years - 1)
            self.log.info(f"[SPLIT-PRE] years={years[0]}..{years[-1]} | cut={cut} | train_max_year={train_max_year}")
        else:
            self.log.warning("[SPLIT-PRE] nao foi possivel inferir ano por filename. Usando fallback (carregar e splitar).")
            df_full = self._load_concat(files, cols)
            valid = [f for f in self.features if f in df_full.columns]
            df_full.dropna(subset=valid + [self.target, self.year_col], inplace=True)
            df_full[self.year_col] = pd.to_numeric(df_full[self.year_col], errors="coerce")
            df_full.dropna(subset=[self.year_col], inplace=True)
            df_full[self.year_col] = df_full[self.year_col].astype("int32")
            _coerce_binary_target(df_full, self.target)

            splitter = TemporalSplitter(test_size_years=test_size_years, gap_years=gap_years)
            train_df, test_df = splitter.split_holdout(df_full, col=self.year_col)
            train_df = train_df.sort_values(self.year_col).reset_index(drop=True)
            test_df = test_df.sort_values(self.year_col).reset_index(drop=True)
            del df_full
            gc.collect()
            return train_df, test_df, valid

        train_parts: List[pd.DataFrame] = []
        test_parts: List[pd.DataFrame] = []
        read_ok = 0
        read_fail = 0
        total_rows_seen = 0

        kept_train = 0
        kept_test = 0

        valid_features: Optional[List[str]] = None

        for f, y_from_name in file_years:
            try:
                df = pd.read_parquet(f, columns=cols)
                read_ok += 1

                if valid_features is None:
                    valid_features = [ft for ft in self.features if ft in df.columns]

                df.dropna(subset=(valid_features or []) + [self.target, self.year_col], inplace=True)

                df[self.year_col] = pd.to_numeric(df[self.year_col], errors="coerce")
                df.dropna(subset=[self.year_col], inplace=True)
                df[self.year_col] = df[self.year_col].astype("int32")
                _coerce_binary_target(df, self.target)
                _downcast_floats(df)

                total_rows_seen += int(len(df))

                if y_from_name is not None:
                    is_train = bool(y_from_name <= train_max_year)
                else:
                    y_max = int(df[self.year_col].max()) if len(df) else -999999
                    is_train = bool(y_max <= train_max_year)

                if is_train and max_train_rows is not None:
                    remaining = max(0, int(max_train_rows) - int(kept_train))
                    if remaining <= 0:
                        continue
                    df = _downsample_keep_all_pos(
                        df=df,
                        target=self.target,
                        max_rows_remaining=remaining,
                        neg_pos_ratio=neg_pos_ratio,
                        min_neg_keep=min_neg_keep_per_chunk,
                        seed=self.random_seed + int(y_from_name or 0),
                    )

                if (not is_train) and max_test_rows is not None:
                    remaining = max(0, int(max_test_rows) - int(kept_test))
                    if remaining <= 0:
                        continue
                    df = _downsample_keep_all_pos(
                        df=df,
                        target=self.target,
                        max_rows_remaining=remaining,
                        neg_pos_ratio=neg_pos_ratio,
                        min_neg_keep=min_neg_keep_per_chunk,
                        seed=self.random_seed + 10_000 + int(y_from_name or 0),
                    )

                if is_train:
                    train_parts.append(df)
                    kept_train += int(len(df))
                else:
                    test_parts.append(df)
                    kept_test += int(len(df))

                if read_ok % 5 == 0:
                    self.log.info(
                        f"[LOAD] batches_ok={read_ok}/{len(files)} | rows_seen={total_rows_seen} | kept_train={kept_train} kept_test={kept_test}"
                    )
                    MemoryMonitor.log_usage(self.log, "durante load batched")

            except Exception as e:
                read_fail += 1
                self.log.warning(f"[LOAD] falha ao ler {f.name}: {e}")

        if valid_features is None:
            raise RuntimeError("[LOAD] nenhum parquet foi carregado com sucesso.")

        if not train_parts or not test_parts:
            self.log.warning("[SPLIT] train_parts ou test_parts vazio. Verifique test_size_years e nomes dos arquivos.")

        train_df = (
            pd.concat(train_parts, ignore_index=True)
            if train_parts
            else pd.DataFrame(columns=(valid_features + [self.target, self.year_col]))
        )
        test_df = (
            pd.concat(test_parts, ignore_index=True)
            if test_parts
            else pd.DataFrame(columns=(valid_features + [self.target, self.year_col]))
        )

        if len(train_df):
            train_df = train_df.sort_values(self.year_col).reset_index(drop=True)
        if len(test_df):
            test_df = test_df.sort_values(self.year_col).reset_index(drop=True)

        downsample_on = bool(max_train_rows is not None or max_test_rows is not None)
        self.log.info(
            f"[LOAD] ok={read_ok} fail={read_fail} | train_rows={len(train_df)} test_rows={len(test_df)} | features={len(valid_features)} | downsample={'ON' if downsample_on else 'OFF'}"
        )
        MemoryMonitor.log_usage(self.log, "apos load batched/concat")

        del train_parts, test_parts
        gc.collect()
        MemoryMonitor.log_usage(self.log, "apos gc pos-load")

        return train_df, test_df, valid_features

    def _load_concat(self, files: List[Path], cols: Optional[List[str]]) -> pd.DataFrame:
        dfs: List[pd.DataFrame] = []
        for f in files:
            df = pd.read_parquet(f, columns=cols)
            _downcast_floats(df)
            dfs.append(df)
        out = pd.concat(dfs, ignore_index=True)
        del dfs
        gc.collect()
        return out

    def run(
        self,
        plan: List[Dict[str, Any]],
        overwrite_all: bool,
        skip_all: bool,
    ) -> Tuple[bool, bool]:
        """
        overwrite_all:
          - True: nunca pergunta, sempre sobrescreve se existir.
        skip_all:
          - True: nunca pergunta, nunca sobrescreve, sempre pula se existir.
        """
        is_calculated = "calculated" in (self.scenario_folder or "").lower()

        try:
            train_df, test_df, valid = self.load_split_batched(
                test_size_years=2,
                gap_years=0,
                max_train_rows=(8_000_000 if is_calculated else None),
                max_test_rows=(2_000_000 if is_calculated else None),
                neg_pos_ratio=200,
                min_neg_keep_per_chunk=50_000,
            )
        except Exception as e:
            self.log.error(f"[CRITICAL] load_split_batched: {e}")
            return overwrite_all, skip_all

        if len(train_df) == 0 or len(test_df) == 0:
            self.log.error("[DATA] train/test vazio. Nao da para treinar.")
            return overwrite_all, skip_all

        X_tr = train_df[valid]
        y_tr = train_df[self.target].astype("int8")

        X_te = test_df[valid]
        y_te = test_df[self.target].astype("int8")

        self.log.info(
            f"[SPLIT] train={len(X_tr)} (pos_rate={_pos_rate(y_tr):.4%}) | "
            f"test={len(X_te)} (pos_rate={_pos_rate(y_te):.4%}) | features={len(valid)}"
        )

        del train_df, test_df
        gc.collect()
        MemoryMonitor.log_usage(self.log, "apos preparar X/y")

        for item in plan:
            m: str = item["type"]
            st: Dict[str, Any] = item["settings"]

            trainer = None
            if m == "logistic":
                trainer = LogisticTrainer(self.scenario_folder, random_state=self.random_seed)
            elif m == "xgboost":
                trainer = XGBoostTrainer(self.scenario_folder, random_state=self.random_seed)
            elif m.startswith("dummy_"):
                trainer = DummyTrainer(self.scenario_folder, m.split("_", 1)[1], random_state=self.random_seed)

            if trainer is None:
                self.log.warning(f"[SKIP] modelo desconhecido: {m}")
                continue

            if m in ("logistic", "xgboost"):
                run_name, tags, desc = _variation_meta_from_settings(st)
                trainer.set_custom_folder_name(run_name)
                trainer.variation_tags = tags
                trainer.variation_desc = desc

            print(
                f"\n    >> [MODELO] {trainer.model_type}/{trainer.run_name} - {getattr(trainer, 'variation_desc', '')} @ {self.scenario_key}"
            )

            try:
                exists = trainer.output_dir.exists() and any(trainer.output_dir.iterdir())

                if exists and skip_all:
                    print(f"       [SKIP] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder} (skip_all ativo)")
                    continue

                if exists and overwrite_all:
                    print(f"       [OVERWRITE_ALL] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder} (overwrite_all ativo)")
                elif exists:
                    print(f"       [AVISO] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder}")
                    r = input("       >> Overwrite? [y/N/all/none_all]: ").strip().lower()

                    if r in ("all", "y_all", "yes_all"):
                        overwrite_all = True
                        print("       [OK] overwrite_all ativado: proximas execucoes existentes serao sobrescritas.")
                    elif r in ("none_all", "no_all", "skip_all"):
                        skip_all = True
                        print("       [OK] skip_all ativado: proximas execucoes existentes serao puladas.")
                        continue
                    elif r in ("y", "yes"):
                        pass
                    else:
                        continue

                trainer.train(X_tr, y_tr, **st)
                thr = float(st.get("thr", 0.5))
                metrics = trainer.evaluate(X_te, y_te, thr=thr)

                run_meta = {
                    "scenario_key": self.scenario_key,
                    "scenario_folder": self.scenario_folder,
                    "features_used": valid,
                    "target": self.target,
                    "year_col": self.year_col,
                    "train_rows": int(len(y_tr)),
                    "test_rows": int(len(y_te)),
                    "train_pos_rate": _pos_rate(y_tr),
                    "test_pos_rate": _pos_rate(y_te),
                    "settings": st,
                    "threshold": thr,
                }

                trainer.save_artifacts(metrics, run_meta=run_meta)

                pr = metrics.get("pr_auc", None)
                pr_str = "None" if pr is None else f"{float(pr):.6f}"
                print(f"       >> OK: PR-AUC={pr_str}")

            except Exception as e:
                self.log.error(f"[ERROR] {m}: {e}")
                import traceback

                traceback.print_exc()

        del X_tr, y_tr, X_te, y_te
        gc.collect()
        MemoryMonitor.log_usage(self.log, "fim do batch")
        return overwrite_all, skip_all


# ----------------------------
# CLI
# ----------------------------
def _build_plan(selected_models: List[str]) -> List[Dict[str, Any]]:
    """
    Permite varias variacoes por modelo.
    Para logistic e xgboost: escolhe 1..4 (multi).
    Para dummies: apenas entra como item unico (sem variacao extra).
    """
    plan: List[Dict[str, Any]] = []

    for m in selected_models:
        if m.startswith("dummy_"):
            plan.append({"type": m, "settings": {}})

    for m in selected_models:
        if m not in ("logistic", "xgboost"):
            continue

        opts = _variation_menu_legacy(m)
        print(f"\n[CFG] {m.upper()} - selecione uma ou mais variacoes (ex: 1,2,4 ou all)")
        for o in opts:
            print(f"[{o.key}] {o.label}")

        while True:
            x = input(">> Variacoes [1]: ").strip().lower()
            if not x:
                chosen_keys = [1]
            elif x == "all":
                chosen_keys = [o.key for o in opts]
            else:
                chosen_keys = _parse_int_tokens(x)
                if not chosen_keys:
                    print("Entrada invalida. Tente novamente.")
                    continue

            chosen = [o for o in opts if o.key in chosen_keys]
            if not chosen:
                print("Nenhuma variacao valida selecionada. Tente novamente.")
                continue

            for o in chosen:
                plan.append({"type": m, "settings": dict(o.settings)})
            break

    return plan


def main():
    cfg = utils.loadConfig()
    scens = cfg.get("modeling_scenarios", {})
    if not scens:
        print("[CRITICAL] modeling_scenarios vazio no config.yaml")
        return

    bases = _select_many({i + 1: k for i, k in enumerate(sorted(scens.keys()))}, "Bases")
    models = _select_many(
        {1: "dummy_stratified", 2: "dummy_prior", 3: "logistic", 4: "xgboost"},
        "Modelos",
    )

    plan = _build_plan(models)

    print(f"\n{'=' * 40}")
    print(f"BATCH START: {len(bases)} Bases x {len(plan)} Runs")
    print(f"{'=' * 40}")

    overwrite_all = False
    skip_all = False

    for i, b in enumerate(bases):
        print(f"\n>>> [BASE {i + 1}/{len(bases)}] {b}")
        try:
            orc = TrainingOrchestrator(b)
            overwrite_all, skip_all = orc.run(plan, overwrite_all=overwrite_all, skip_all=skip_all)
        except Exception as e:
            print(e)
        gc.collect()


if __name__ == "__main__":
    main()
