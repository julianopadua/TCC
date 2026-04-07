# src/train_runner.py
# =============================================================================
# ORQUESTRADOR DE EXPERIMENTOS (BATCHED LOAD + CLI MULTI-VARIACOES)
# =============================================================================
# Objetivos:
# 1) Carregar os parquets em batches (ano a ano) para reduzir pico de memoria
# 2) Menu de variacoes consistente (base + combinacoes com GridSearch/SMOTE/Weight)
# 3) Permitir selecionar MULTIPLAS variacoes por modelo (ex: 1,2,4)
# 4) Parser robusto de selecao: "3,4" "3, 4" "3 4" "3;4" etc.
# 5) Nunca usar o caractere "—"
# =============================================================================

import gc
import re
import shutil
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

# Core deps (obrigatorios)
try:
    import src.utils as utils
    from src.ml.core import MemoryMonitor, TemporalSplitter
    from src.models.dummy import DummyTrainer
    from src.models.logistic import LogisticTrainer
    from src.models.xgboost_model import XGBoostTrainer
except ImportError as e:
    sys.exit(f"[CRITICAL] Dependencias obrigatorias: {e}")

# Modelos opcionais (presentes apenas se voce tiver criado os arquivos)
NaiveBayesTrainer = None
SVMTrainer = None
RandomForestTrainer = None

try:
    from src.models.naive_bayes import NaiveBayesTrainer as _NB  # type: ignore

    NaiveBayesTrainer = _NB
except Exception:
    NaiveBayesTrainer = None

try:
    from src.models.svm_linear import SVMLinearTrainer as _SVM  # type: ignore

    SVMTrainer = _SVM
except Exception:
    try:
        from src.models.svm import SVMTrainer as _SVM2  # type: ignore

        SVMTrainer = _SVM2
    except Exception:
        SVMTrainer = None

try:
    from src.models.random_forest import RandomForestTrainer as _RF  # type: ignore

    RandomForestTrainer = _RF
except Exception:
    RandomForestTrainer = None


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


def _downcast_floats(df: pd.DataFrame) -> None:
    # float64 -> float32 (impacto grande em RAM)
    cols = df.select_dtypes(include=["float64"]).columns
    for c in cols:
        df[c] = df[c].astype("float32")


def _coerce_binary_target(df: pd.DataFrame, target: str) -> None:
    # Garante target binario 0/1 em int8 e remove valores invalidos
    df[target] = pd.to_numeric(df[target], errors="coerce")
    df.dropna(subset=[target], inplace=True)
    df = df.loc[df[target].isin([0, 1])]
    # Reatribui no df original (mantem referencia)
    df[target] = df[target].astype("int8")


def _parse_int_tokens(text: str) -> List[int]:
    """
    Aceita: "1,3" "1, 3" "1 3" "1;3" "1|3" etc.
    Retorna lista de ints (sem duplicatas, preservando ordem).
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
        x = input(">> Select (ex: 1,3 | 1 3 | 1;3 | all): ").strip().lower()
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


def _clear_dir(path: Path) -> None:
    # Limpa somente o conteudo da pasta do run (nao remove a pasta)
    if not path.exists():
        return
    for p in path.iterdir():
        try:
            if p.is_file() or p.is_symlink():
                p.unlink()
            elif p.is_dir():
                shutil.rmtree(p, ignore_errors=True)
        except Exception:
            continue


def _dir_has_outputs(path: Path) -> bool:
    # Define "existe run" se tiver pelo menos um metrics_*.json ou model_*.joblib
    if not path.exists():
        return False
    try:
        for p in path.iterdir():
            if p.is_file() and (p.name.startswith("metrics_") and p.name.endswith(".json")):
                return True
            if p.is_file() and (p.name.startswith("model_") and p.name.endswith(".joblib")):
                return True
        return any(path.iterdir())
    except Exception:
        return False


# ----------------------------
# Variacao (menu consistente)
# ----------------------------
@dataclass(frozen=True)
class VariationOption:
    key: int
    label: str
    settings: Dict[str, Any]


def _variation_menu_legacy(model_key: str) -> List[VariationOption]:
    """
    Menu:
      1) Base
      2) GridSearch + SMOTE + Weight
      3) GridSearch + SMOTE (sem Weight)
      4) GridSearch + Weight (sem SMOTE)

    Observacao:
      - use_scale == Weight (class_weight / scale_pos_weight / sample_weight)
      - Ajustes por modelo para custo computacional
      - RandomForest: default cv_splits=2 e grid_mode="full" para limitar fits
    """
    base_common: Dict[str, Any] = {
        "cv_splits": 3,
        "scoring": "average_precision",
        "smote_sampling_strategy": 0.1,
        "smote_k_neighbors": 5,
        "thr": 0.5,
    }

    # Modelos que tipicamente precisam de scaling
    if model_key in ("logistic", "svm"):
        base_common["feature_scaling"] = True

    # Base de configuracao para runs com GridSearch (ajustes por modelo)
    grid_common = dict(base_common)

    # XGBoost: GridSearch mais leve por padrao
    if model_key == "xgboost":
        grid_common["cv_splits"] = 2
        grid_common["grid_mode"] = "fast"
        grid_common["model_n_jobs"] = None

    # SVM: GridSearch pode ser caro, reduzir splits por padrao
    if model_key == "svm":
        grid_common["cv_splits"] = 2

    # RandomForest: limitar fits por padrao
    if model_key == "random_forest":
        grid_common["cv_splits"] = 2
        grid_common["grid_mode"] = "full"

    return [
        VariationOption(
            1,
            "Base - sem SMOTE, sem GridSearch, sem peso",
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
    Amostra negativos por:
      - max_rows_remaining (orcamento)
      - neg_pos_ratio (max negativos por positivo)
      - min_neg_keep (minimo de negativos mesmo se pos=0)
    """
    if df is None or df.empty:
        return df

    pos = df[df[target] == 1]
    neg = df[df[target] == 0]

    n_pos = int(len(pos))
    neg_cap_by_ratio = max(int(min_neg_keep), int(n_pos) * int(neg_pos_ratio))

    if max_rows_remaining is None:
        neg_cap_by_budget = len(neg)
    else:
        budget_for_neg = max(0, int(max_rows_remaining) - n_pos)
        neg_cap_by_budget = int(budget_for_neg)

    n_neg_keep = int(min(len(neg), neg_cap_by_ratio, neg_cap_by_budget))
    if n_neg_keep < len(neg):
        neg = neg.sample(n=n_neg_keep, random_state=int(seed))

    if max_rows_remaining is not None and n_pos > int(max_rows_remaining):
        pos = pos.sample(n=int(max_rows_remaining), random_state=int(seed))

    out = pd.concat([pos, neg], ignore_index=True)
    return out


class TrainingOrchestrator:
    def __init__(self, scenario_key: str):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)

        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg["modeling_scenarios"].get(scenario_key)
        if not self.scenario_folder:
            raise ValueError(f"Cenario {scenario_key} invalido.")

        self.random_seed = int(self.cfg.get("project", {}).get("random_seed", 42))

        # Mantem a mesma lista que voce ja usa
        self.features = [
            "PRECIPITACAO TOTAL, HORARIO (mm)".replace("PRECIPITACAO", "PRECIPITAÇÃO").replace("HORARIO", "HORÁRIO"),
            "RADIACAO GLOBAL (KJ/m²)",
            "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)",
            "UMIDADE RELATIVA DO AR, HORARIA (%)",
            "VENTO, VELOCIDADE HORARIA (m/s)",
            "PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)",
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
        # Tenta ler apenas colunas necessarias via schema
        try:
            import pyarrow.parquet as pq  # type: ignore

            avail = set(pq.read_schema(first_file).names)

            if self.target not in avail or self.year_col not in avail:
                self.log.warning("[LOAD] target/year_col nao encontrados no schema. Vai ler sem selecao de colunas.")
                return None

            feats = [c for c in self.features if c in avail]
            cols = feats + [self.target, self.year_col]

            # Unico e preserva ordem
            seen = set()
            out = []
            for c in cols:
                if c not in seen:
                    out.append(c)
                    seen.add(c)

            self.log.info(f"[LOAD] colunas selecionadas via schema: {len(out)}")
            return out
        except Exception as e:
            self.log.warning(f"[LOAD] sem pyarrow/schema otimizado (vai ler colunas padrao): {e}")
            return None

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
        Carrega em batches por parquet (idealmente 1 por ano).
        Faz split temporal por ano do filename, evitando df full gigante.

        Se max_train_rows/max_test_rows:
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
            # Fallback: carrega tudo e usa split por coluna ANO
            self.log.warning("[SPLIT-PRE] nao inferiu ano por filename. Fallback: load full + split por ANO.")
            df_full = self._load_concat(files, cols)
            valid = [f for f in self.features if f in df_full.columns]
            df_full.dropna(subset=valid + [self.target, self.year_col], inplace=True)
            df_full[self.year_col] = pd.to_numeric(df_full[self.year_col], errors="coerce")
            df_full.dropna(subset=[self.year_col], inplace=True)
            df_full[self.year_col] = df_full[self.year_col].astype("int32")

            _coerce_binary_target(df_full, self.target)
            _downcast_floats(df_full)

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

                if not valid_features:
                    raise RuntimeError("Nenhuma feature valida encontrada no parquet.")

                df.dropna(subset=valid_features + [self.target, self.year_col], inplace=True)

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
            f"[LOAD] ok={read_ok} fail={read_fail} | train_rows={len(train_df)} test_rows={len(test_df)} | "
            f"features={len(valid_features)} | downsample={'ON' if downsample_on else 'OFF'}"
        )
        MemoryMonitor.log_usage(self.log, "apos load batched/concat")

        del train_parts, test_parts
        gc.collect()
        MemoryMonitor.log_usage(self.log, "apos gc pos-load")

        return train_df, test_df, valid_features

    def run(
        self,
        plan: List[Dict[str, Any]],
        overwrite_all: bool,
        skip_all: bool,
    ) -> Tuple[bool, bool]:
        """
        overwrite_all:
          - True: nao pergunta, limpa pasta do run e sobrescreve.
        skip_all:
          - True: nao pergunta, pula se existir.
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

        # X/y
        X_tr = train_df[valid]
        y_tr = train_df[self.target].astype("int8")

        X_te = test_df[valid]
        y_te = test_df[self.target].astype("int8")

        # Forca float32 nas features (evita float64 inesperado)
        _downcast_floats(X_tr)
        _downcast_floats(X_te)

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
            elif m == "naive_bayes" and NaiveBayesTrainer is not None:
                trainer = NaiveBayesTrainer(self.scenario_folder, random_state=self.random_seed)
            elif m == "svm" and SVMTrainer is not None:
                trainer = SVMTrainer(self.scenario_folder, random_state=self.random_seed)
            elif m == "random_forest" and RandomForestTrainer is not None:
                trainer = RandomForestTrainer(self.scenario_folder, random_state=self.random_seed)
            elif m.startswith("dummy_"):
                trainer = DummyTrainer(self.scenario_folder, m.split("_", 1)[1], random_state=self.random_seed)

            if trainer is None:
                self.log.warning(f"[SKIP] modelo indisponivel/desconhecido: {m}")
                continue

            # Nome de pasta por variacao (padronizado)
            if not m.startswith("dummy_"):
                run_name, tags, desc = _variation_meta_from_settings(st)
                trainer.set_custom_folder_name(run_name)
                trainer.variation_tags = tags
                trainer.variation_desc = desc

            print(f"\n    >> [MODELO] {trainer.model_type}/{trainer.run_name} - {getattr(trainer, 'variation_desc', '')} @ {self.scenario_key}")

            try:
                exists = _dir_has_outputs(trainer.output_dir)

                if exists and skip_all:
                    print(f"       [SKIP] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder} (skip_all ativo)")
                    continue

                if exists and overwrite_all:
                    print(f"       [OVERWRITE_ALL] Limpando: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder}")
                    _clear_dir(trainer.output_dir)
                elif exists:
                    print(f"       [AVISO] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder}")
                    r = input("       >> Overwrite? [y/N/all/none_all]: ").strip().lower()

                    if r in ("all", "y_all", "yes_all"):
                        overwrite_all = True
                        print("       [OK] overwrite_all ativado.")
                        _clear_dir(trainer.output_dir)
                    elif r in ("none_all", "no_all", "skip_all"):
                        skip_all = True
                        print("       [OK] skip_all ativado.")
                        continue
                    elif r in ("y", "yes"):
                        _clear_dir(trainer.output_dir)
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
    Permite varias variacoes por modelo (exceto dummies).
    """
    plan: List[Dict[str, Any]] = []

    # Dummies entram sem variacao
    for m in selected_models:
        if m.startswith("dummy_"):
            plan.append({"type": m, "settings": {}})

    # Modelos com variacoes
    for m in selected_models:
        if m.startswith("dummy_"):
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

    # Menu dinamico de modelos (so mostra os que existem)
    models_menu: List[str] = ["dummy_stratified", "dummy_prior", "logistic", "xgboost"]
    if NaiveBayesTrainer is not None:
        models_menu.append("naive_bayes")
    if SVMTrainer is not None:
        models_menu.append("svm")
    if RandomForestTrainer is not None:
        models_menu.append("random_forest")

    models_dict = {i + 1: name for i, name in enumerate(models_menu)}
    models = _select_many(models_dict, "Modelos")

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
