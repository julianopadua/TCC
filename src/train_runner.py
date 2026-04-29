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

import argparse
import gc
import os
import re
import shutil
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import numpy as np
import pandas as pd

# Default streaming batch size (rows) when reading parquets. Keeps
# peak-RAM bounded even on wide minirocket schemas (~200 cols).
_DEFAULT_BATCH_ROWS = int(os.environ.get("TRAIN_RUNNER_BATCH_ROWS", "500000"))

try:
    from tqdm.auto import tqdm
except ImportError:  # pragma: no cover
    def tqdm(it, **_kwargs):  # type: ignore
        return it

# Path Setup
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Core deps (obrigatorios)
try:
    import src.utils as utils
    from src.utils import (
        article_coords_root,
        article_fusion_output_root,
        article_parquet_dir_has_files,
        list_article_coord_dataset_folders,
        list_article_fusion_train_menu_keys,
        resolve_parquet_dir,
    )
    from src.article.config import biomass_modeling_columns_for_schema
    from src.ml.core import MemoryMonitor, TemporalSplitter
except ImportError as e:
    sys.exit(f"[CRITICAL] Dependencias obrigatorias: {e}")

# Load lazy dos trainers para permitir import de utilitarios (ex.: eval/viz)
# mesmo em ambientes sem todas as libs de treino em runtime.
DummyTrainer = None
LogisticTrainer = None
XGBoostTrainer = None
NaiveBayesTrainer = None
SVMTrainer = None
RandomForestTrainer = None


def _ensure_trainers_loaded() -> None:
    global DummyTrainer, LogisticTrainer, XGBoostTrainer
    global NaiveBayesTrainer, SVMTrainer, RandomForestTrainer

    if DummyTrainer is None:
        try:
            from src.models.dummy import DummyTrainer as _DummyTrainer  # type: ignore

            DummyTrainer = _DummyTrainer
        except Exception:
            DummyTrainer = None
    if LogisticTrainer is None:
        try:
            from src.models.logistic import LogisticTrainer as _LogisticTrainer  # type: ignore

            LogisticTrainer = _LogisticTrainer
        except Exception:
            LogisticTrainer = None
    if XGBoostTrainer is None:
        try:
            from src.models.xgboost_model import XGBoostTrainer as _XGBoostTrainer  # type: ignore

            XGBoostTrainer = _XGBoostTrainer
        except Exception:
            XGBoostTrainer = None
    if NaiveBayesTrainer is None:
        try:
            from src.models.naive_bayes import NaiveBayesTrainer as _NB  # type: ignore

            NaiveBayesTrainer = _NB
        except Exception:
            NaiveBayesTrainer = None
    if SVMTrainer is None:
        try:
            from src.models.svm_linear import SVMLinearTrainer as _SVM  # type: ignore

            SVMTrainer = _SVM
        except Exception:
            try:
                from src.models.svm import SVMTrainer as _SVM2  # type: ignore

                SVMTrainer = _SVM2
            except Exception:
                SVMTrainer = None
    if RandomForestTrainer is None:
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


def _source_rows_by_split(
    source_audit: Dict[str, Any],
    train_years: List[int],
    test_years: List[int],
) -> Tuple[Optional[int], Optional[int]]:
    """
    Soma linhas brutas (pré-downsampling) por split, usando o per-file audit.

    Retorna (train_total, test_total) — None se não for possível calcular.
    """
    if not source_audit:
        return None, None
    per_file = source_audit.get("per_file") or []
    if not per_file:
        return None, None

    train_set = set(train_years)
    test_set = set(test_years)
    tr_total = 0
    te_total = 0

    for entry in per_file:
        fname = entry.get("file", "")
        m = _YEAR_RE.search(fname)
        if not m:
            continue
        try:
            y = int(m.group(1))
        except Exception:
            continue
        rows = int(entry.get("rows") or 0)
        if y in train_set:
            tr_total += rows
        elif y in test_set:
            te_total += rows

    return (tr_total if tr_total else None), (te_total if te_total else None)


def _downcast_floats(df: pd.DataFrame) -> None:
    # float64 -> float32 (impacto grande em RAM)
    cols = df.select_dtypes(include=["float64"]).columns
    for c in cols:
        df[c] = df[c].astype("float32")


def _concat_parts_low_mem(
    parts: List[pd.DataFrame],
    features: List[str],
    target_col: str,
    year_col: str,
    log,
) -> pd.DataFrame:
    """Concatena partes em um DataFrame final usando buffers numpy pre-alocados.

    Por que existe: pd.concat consolida colunas same-dtype no BlockManager,
    o que exige ~2x a RAM final (partes originais + bloco consolidado vivos
    simultaneamente). Em cenarios largos como minirocket (180 features
    float32 x 7M+ linhas = ~5 GiB), isso estoura a RAM mesmo com streaming
    chunked do parquet. Aqui o pico fica em ~1x (buffer final) + 1 chunk
    temporario, pois cada parte e liberada imediatamente apos a copia.
    """
    if not parts:
        return pd.DataFrame(columns=list(features) + [target_col, year_col])

    parts = [p for p in parts if p is not None and len(p) > 0]
    if not parts:
        return pd.DataFrame(columns=list(features) + [target_col, year_col])

    n_total = sum(len(p) for p in parts)
    n_feat = len(features)
    bytes_x = n_total * n_feat * 4  # float32

    log.info(
        f"[CONCAT] low-mem: rows={n_total:,} x feats={n_feat} float32 "
        f"~ {bytes_x / 1024**3:.2f} GiB | parts={len(parts)}"
    )

    X = np.empty((n_total, n_feat), dtype=np.float32)
    y = np.empty(n_total, dtype=np.int8)
    yr = np.empty(n_total, dtype=np.int32)

    offset = 0
    for i in range(len(parts)):
        p = parts[i]
        m = len(p)
        if m == 0:
            parts[i] = None
            continue
        X[offset:offset + m, :] = p[features].to_numpy(dtype=np.float32, copy=False)
        y[offset:offset + m] = p[target_col].to_numpy(dtype=np.int8, copy=False)
        yr[offset:offset + m] = p[year_col].to_numpy(dtype=np.int32, copy=False)
        offset += m
        parts[i] = None

    gc.collect()

    df = pd.DataFrame(X, columns=list(features), copy=False)
    df[target_col] = y
    df[year_col] = yr
    return df


def _coerce_binary_target(df: pd.DataFrame, target: str) -> None:
    # Garante target binario 0/1 em int8 e remove valores invalidos
    df[target] = pd.to_numeric(df[target], errors="coerce")
    df.dropna(subset=[target], inplace=True)
    # drop rows where target is not 0 or 1 (e.g. fractional values after coercion)
    non_binary = ~df[target].isin([0, 1])
    if non_binary.any():
        df.drop(index=df.index[non_binary], inplace=True)
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


def _parse_name_tokens(text: str) -> List[str]:
    """Tokens de nomes (cenarios, modelos) separados por virgula/espaco/etc."""
    t = (text or "").strip()
    if not t:
        return []
    parts = re.split(r"[,\s;|]+", t)
    out: List[str] = []
    seen = set()
    for p in parts:
        p = p.strip()
        if not p:
            continue
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def _flatten_variation_args(tokens: List[str]) -> List[int]:
    """Junta tokens CLI de variacoes (ex.: ['1','2'] ou ['1,2','4']) em lista de int."""
    out: List[int] = []
    seen = set()
    for tok in tokens:
        for v in _parse_int_tokens(tok):
            if v not in seen:
                out.append(v)
                seen.add(v)
    return out


def _select_many(
    opts: Dict[int, Any],
    title: str,
    *,
    allow_empty: bool = False,
) -> List[Any]:
    print(f"\n--- {title} ---")
    for k, v in opts.items():
        print(f"[{k}] {v}")
    if allow_empty:
        print("    (Enter vazio = nenhuma desta secao)")

    while True:
        x = input(">> Select (ex: 1,3 | 1 3 | 1;3 | all): ").strip().lower()
        if allow_empty and x == "":
            return []
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


class TrainRunnerOutputExistsError(Exception):
    """Saida ja existe e o modo --on-exist error foi pedido."""


@dataclass
class EvalSplitData:
    """Pacote reutilizavel com os dados de treino/teste preparados."""

    X_train: pd.DataFrame
    y_train: pd.Series
    X_test: pd.DataFrame
    y_test: pd.Series
    valid_features: List[str]
    data_audit: Dict[str, Any]


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


def _article_temporal_test_size_years(cfg: Dict[str, Any]) -> int:
    """Single source of truth: article_pipeline.temporal_fusion.test_size_years."""
    ap = cfg.get("article_pipeline") or {}
    tf = ap.get("temporal_fusion") or {}
    return int(tf.get("test_size_years", 2))


def _scenario_accepted_for_run(cfg: Dict[str, Any], token: str, *, use_article: bool) -> bool:
    """
    Cenario valido para o CLI se:
      - chave em modeling_scenarios; ou
      - --article e pasta em 0_datasets_with_coords com parquets; ou
      - --article e resolve_parquet_dir(article) tem parquets (ex.: nome de scenario_folder / tf_*).
    """
    scens = cfg.get("modeling_scenarios") or {}
    if token in scens:
        return True
    if not use_article:
        return False
    if token in set(list_article_coord_dataset_folders(cfg)):
        return True
    if article_parquet_dir_has_files(cfg, token):
        return True
    return False


class TrainingOrchestrator:
    def __init__(self, scenario_key: str, *, use_article_data: bool = False):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)

        self.scenario_key = scenario_key
        scens = self.cfg.get("modeling_scenarios") or {}
        folder = scens.get(scenario_key)
        if folder:
            self.scenario_folder = folder
        elif use_article_data:
            cr = article_coords_root(self.cfg)
            cand = cr / scenario_key
            if cand.is_dir() and any(cand.glob("*.parquet")):
                self.scenario_folder = scenario_key
            else:
                raise ValueError(
                    f"Cenario {scenario_key!r} invalido: nao esta em modeling_scenarios e "
                    f"nao ha parquets em {cand}"
                )
        else:
            raise ValueError(f"Cenario {scenario_key} invalido.")

        self.use_article_data = bool(use_article_data)
        self._parquet_source = "article" if self.use_article_data else "tcc"

        self.random_seed = int(self.cfg.get("project", {}).get("random_seed", 42))

        self._base_features = [
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

        # article_pipeline.modeling_biomass_mode == "biomass_only" restricts
        # the feature set to NDVI/EVI exclusively (article stage 5 simplification).
        ap = self.cfg.get("article_pipeline") or {}
        self._biomass_only_mode = (
            str(ap.get("modeling_biomass_mode", "buffers")).strip().lower()
            == "biomass_only"
        )

        # For tsfusion / tf_* scenarios, auto-detect tsf_* columns from the
        # first parquet so they enter the feature set without hard-coding names.
        if self._biomass_only_mode and self.use_article_data:
            self.features = []
        else:
            self.features = list(self._base_features)

        if self._is_temporal_fusion_scenario():
            self._extend_with_tsf_columns()
        if self.use_article_data:
            self._extend_with_article_biomass_columns()

        self.target = "HAS_FOCO"
        self.year_col = "ANO"

    def _log_run_banner(self) -> None:
        """Registra cenario, caminho absoluto dos parquets, PID e uso de host."""
        pq = resolve_parquet_dir(
            self.cfg, self.scenario_folder, source=self._parquet_source
        ).resolve()
        self.log.info("=" * 72)
        self.log.info("TRAIN_RUNNER | INICIO DO CENARIO")
        self.log.info(f"scenario_key={self.scenario_key}")
        self.log.info(f"scenario_folder={self.scenario_folder}")
        self.log.info(f"parquet_source={self._parquet_source}")
        self.log.info(f"parquet_dir={pq}")
        if self.use_article_data:
            self.log.info(f"article_coords_root={article_coords_root(self.cfg)}")
        self.log.info(
            f"n_features={len(self.features)} | target={self.target} | year_col={self.year_col} | "
            f"biomass_only_mode={getattr(self, '_biomass_only_mode', False)}"
        )
        self.log.info(
            f"pid={os.getpid()} | python={sys.version.split()[0]} | random_seed={self.random_seed}"
        )
        self.log.info(
            f"test_size_years={_article_temporal_test_size_years(self.cfg)} | "
            f"use_article_data={self.use_article_data}"
        )
        MemoryMonitor.log_usage(self.log, "pre-load")
        self.log.info("=" * 72)

    def _is_temporal_fusion_scenario(self) -> bool:
        """True if this scenario carries temporal fusion (tsf_*) features."""
        folder = self.scenario_folder or ""
        if "tsfusion" in folder.lower():
            return True
        if folder.startswith("tf_"):
            return True
        tf_paths: dict = self.cfg.get("temporal_fusion_paths", {}) or {}
        if folder in tf_paths:
            return True
        return False

    def _extend_with_tsf_columns(self) -> None:
        """Auto-detect tsf_* columns from the first parquet in the scenario."""
        try:
            import pyarrow.parquet as pq

            path = resolve_parquet_dir(
                self.cfg, self.scenario_folder, source=self._parquet_source
            )
            first = next(path.glob("*.parquet"), None)
            if first is None:
                return
            schema_names = set(pq.read_schema(first).names)
            tsf_cols = sorted(c for c in schema_names if c.startswith("tsf_"))
            if tsf_cols:
                self.features.extend(tsf_cols)
                self.log.info(
                    f"[TSF] Auto-detected {len(tsf_cols)} temporal fusion "
                    f"columns from {first.name}"
                )
        except Exception as e:
            self.log.warning(f"[TSF] Could not auto-detect tsf_* columns: {e}")

    def _extend_with_article_biomass_columns(self) -> None:
        """NDVI/EVI do GEE no conjunto de features (--article), conforme modeling_biomass_mode."""
        if not self.use_article_data:
            return
        try:
            import pyarrow.parquet as pq

            path = resolve_parquet_dir(
                self.cfg, self.scenario_folder, source=self._parquet_source
            )
            first = next(path.glob("*.parquet"), None)
            if first is None:
                return
            schema_names = set(pq.read_schema(first).names)
            biomass_cols = biomass_modeling_columns_for_schema(self.cfg, schema_names)
            ap = self.cfg.get("article_pipeline") or {}
            mode = str(ap.get("modeling_biomass_mode", "buffers")).strip().lower()
            seen = set(self.features)
            added: List[str] = []
            for c in biomass_cols:
                if c not in seen:
                    self.features.append(c)
                    seen.add(c)
                    added.append(c)
            if added:
                self.log.info(
                    f"[BIOM] modeling_biomass_mode={mode!r} | +{len(added)} colunas: {added} "
                    f"(schema={first.name})"
                )
        except Exception as e:
            self.log.warning(f"[BIOM] Nao foi possivel adicionar NDVI/EVI: {e}")

    def _audit_source_parquets(self, files: List[Path]) -> Dict[str, Any]:
        """Le apenas (cidade_norm, ts_hour) de cada parquet fonte e computa
        ratio de duplicacao. Custa 2 colunas por arquivo, barato."""
        import pyarrow.parquet as pq  # type: ignore

        per_file: List[Dict[str, Any]] = []
        anomalies: List[Dict[str, Any]] = []
        total_rows = 0
        total_unique = 0
        for f in files:
            entry: Dict[str, Any] = {"file": f.name, "path": str(f)}
            try:
                pf = pq.ParquetFile(f)
                num_rows = int(pf.metadata.num_rows)
                entry["rows"] = num_rows
                schema_names = set(pf.schema_arrow.names)
                key_cols = [c for c in ("cidade_norm", "ts_hour") if c in schema_names]
                if len(key_cols) == 2:
                    tbl = pf.read(columns=key_cols)
                    df_keys = tbl.to_pandas()
                    uniq = int(df_keys.drop_duplicates().shape[0])
                    ratio = (num_rows / uniq) if uniq else 0.0
                    entry["unique_keys"] = uniq
                    entry["dup_ratio"] = round(ratio, 4)
                    entry["status"] = "OK" if ratio < 1.01 else "DUPLICATED"
                    total_rows += num_rows
                    total_unique += uniq
                    if ratio >= 1.01:
                        anomalies.append(entry)
                else:
                    entry["status"] = "NO_KEYS"
                    entry["missing_cols"] = [
                        c for c in ("cidade_norm", "ts_hour") if c not in schema_names
                    ]
            except Exception as e:
                entry["status"] = "ERROR"
                entry["error"] = repr(e)
            per_file.append(entry)

        overall_ratio = (total_rows / total_unique) if total_unique else 0.0
        return {
            "per_file": per_file,
            "anomalies": anomalies,
            "total_source_rows": total_rows,
            "total_source_unique_keys": total_unique,
            "overall_dup_ratio": round(overall_ratio, 4),
            "source_clean": bool(total_unique and overall_ratio < 1.01),
        }

    def _discover_files(self) -> List[Path]:
        path = resolve_parquet_dir(
            self.cfg, self.scenario_folder, source=self._parquet_source
        )
        self.log.info(
            f"[LOAD] parquet_source={self._parquet_source} | "
            f"scenario_folder={self.scenario_folder} | path={path}"
        )
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
        # Concat legado para o fallback sem filename-year: ainda usa o streaming
        # para que nenhum arquivo unico estoure a RAM. Cada chunk ja vem em f32.
        dfs: List[pd.DataFrame] = []
        for f in files:
            for df_chunk in self._iter_parquet_chunks_f32(f, cols):
                dfs.append(df_chunk)
        if not dfs:
            return pd.DataFrame(columns=cols or [])
        out = pd.concat(dfs, ignore_index=True)
        del dfs
        gc.collect()
        return out

    def _iter_parquet_chunks_f32(
        self,
        path: Path,
        columns: Optional[List[str]],
        batch_rows: Optional[int] = None,
    ) -> Iterator[pd.DataFrame]:
        """
        Le um parquet em batches pequenos usando PyArrow e materializa cada
        batch como DataFrame com floats ja em float32 (via cast Arrow-side).

        Evita o pico de ~4-5 GiB observado em load_split_batched: o binding
        pandas padrao aloca blocos float64 gigantes ao converter a tabela
        inteira, mesmo quando os dados sao float32. Com row-batch streaming
        + cast Arrow + self_destruct, o pico por chunk cai para:
          batch_rows * n_cols * 4 bytes  (ex.: 500_000 x 200 x 4 ~= 380 MiB).
        """
        try:
            import pyarrow as pa  # type: ignore
            import pyarrow.parquet as pq  # type: ignore
        except Exception as exc:
            # Fallback absoluto: le tudo de uma vez (preserva compat, mas
            # imprime aviso para que o gargalo nao passe despercebido).
            self.log.warning(
                f"[LOAD] pyarrow indisponivel ({exc}); usando pd.read_parquet integral."
            )
            df = pd.read_parquet(path, columns=columns)
            _downcast_floats(df)
            yield df
            return

        bs = int(batch_rows if batch_rows is not None else _DEFAULT_BATCH_ROWS)
        if bs < 10_000:
            bs = 10_000  # proteger contra batches patologicamente pequenos

        pf = pq.ParquetFile(str(path))

        # Constroi schema-alvo com float64 -> float32 para evitar pico f64
        # durante a conversao para pandas.
        try:
            arrow_schema = pf.schema_arrow
            requested = set(columns) if columns is not None else None
            target_fields: List[Any] = []
            for fld in arrow_schema:
                if requested is not None and fld.name not in requested:
                    continue
                if pa.types.is_floating(fld.type) and fld.type == pa.float64():
                    target_fields.append(pa.field(fld.name, pa.float32()))
                else:
                    target_fields.append(fld)
            target_schema = pa.schema(target_fields)
        except Exception:
            target_schema = None  # type: ignore

        iter_kwargs: Dict[str, Any] = {"batch_size": bs}
        if columns is not None:
            iter_kwargs["columns"] = columns

        for batch in pf.iter_batches(**iter_kwargs):
            if target_schema is not None:
                try:
                    batch = batch.cast(target_schema)
                except Exception:
                    pass  # segue sem cast; downcast pandas-side cobre o resto

            # self_destruct libera os buffers Arrow apos a conversao.
            df = batch.to_pandas(split_blocks=True, self_destruct=True)

            # Garantia defensiva (ex.: caso cast tenha falhado).
            _downcast_floats(df)
            yield df

    def load_split_batched(
        self,
        test_size_years: int = 2,
        gap_years: int = 0,
        max_train_rows: Optional[int] = None,
        max_test_rows: Optional[int] = None,
        neg_pos_ratio: int = 200,
        min_neg_keep_per_chunk: int = 50_000,
        batch_rows: Optional[int] = None,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, List[str]]:
        """
        Carrega em batches por parquet (idealmente 1 por ano).
        Faz split temporal por ano do filename, evitando df full gigante.

        Se max_train_rows/max_test_rows:
          - mantem 100% dos positivos
          - amostra negativos respeitando orcamento e neg_pos_ratio
        """
        files = self._discover_files()
        names = [f.name for f in files]
        preview = names[:40]
        tail = " ..." if len(names) > 40 else ""
        self.log.info(f"[LOAD] n_parquets={len(files)} arquivos: {', '.join(preview)}{tail}")

        # Pre-flight audit: detecta duplicacao em (cidade_norm, ts_hour) na fonte.
        source_audit = self._audit_source_parquets(files)
        if source_audit["anomalies"]:
            self.log.warning(
                f"[AUDIT] duplicacao detectada em {len(source_audit['anomalies'])} "
                f"parquet(s) fonte. overall_ratio={source_audit['overall_dup_ratio']}x"
            )
            for a in source_audit["anomalies"][:5]:
                self.log.warning(
                    f"[AUDIT]   {a['file']}: rows={a.get('rows')} "
                    f"unique={a.get('unique_keys')} ratio={a.get('dup_ratio')}x"
                )
        else:
            self.log.info(
                f"[AUDIT] fonte OK | {len(files)} parquet(s) | "
                f"overall_ratio={source_audit['overall_dup_ratio']}x"
            )
        self._last_source_audit = source_audit

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

        pbar = tqdm(
            file_years,
            desc="[LOAD] parquets",
            unit="arq",
            total=len(file_years),
            leave=True,
        )

        eff_batch_rows = int(batch_rows if batch_rows is not None else _DEFAULT_BATCH_ROWS)
        self.log.info(
            f"[LOAD] streaming chunks batch_rows={eff_batch_rows:,} "
            f"(pyarrow iter_batches + float32 cast)"
        )

        for f, y_from_name in pbar:
            try:
                pbar.set_postfix_str(f.name[:28], refresh=False)

                # Partition e decidida pelo ano do nome do arquivo; todo o
                # arquivo vai inteiro para train ou test. Se nao tiver ano
                # no nome, cairia no fallback de _load_concat acima.
                is_train = (
                    bool(y_from_name <= train_max_year)
                    if y_from_name is not None
                    else None
                )

                chunk_idx = 0
                budget_hit = False
                for df in self._iter_parquet_chunks_f32(f, cols, batch_rows=eff_batch_rows):
                    chunk_idx += 1

                    if valid_features is None:
                        valid_features = [ft for ft in self.features if ft in df.columns]

                    if not valid_features:
                        raise RuntimeError("Nenhuma feature valida encontrada no parquet.")

                    # Limpeza / coerção (sempre in-place para evitar copias).
                    df.dropna(subset=valid_features + [self.target, self.year_col], inplace=True)

                    df[self.year_col] = pd.to_numeric(df[self.year_col], errors="coerce")
                    df.dropna(subset=[self.year_col], inplace=True)
                    df[self.year_col] = df[self.year_col].astype("int32")

                    _coerce_binary_target(df, self.target)

                    total_rows_seen += int(len(df))

                    # Para filenames sem ano, decide por chunk usando y_max.
                    if is_train is None:
                        y_max = int(df[self.year_col].max()) if len(df) else -999999
                        chunk_is_train = bool(y_max <= train_max_year)
                    else:
                        chunk_is_train = is_train

                    # Downsampling por chunk: mantem 100% dos positivos e
                    # limita negativos de acordo com budget global restante.
                    if chunk_is_train and max_train_rows is not None:
                        remaining = max(0, int(max_train_rows) - int(kept_train))
                        if remaining <= 0:
                            budget_hit = True
                            del df
                            continue
                        df = _downsample_keep_all_pos(
                            df=df,
                            target=self.target,
                            max_rows_remaining=remaining,
                            neg_pos_ratio=neg_pos_ratio,
                            min_neg_keep=min_neg_keep_per_chunk,
                            seed=self.random_seed + int(y_from_name or 0) + chunk_idx,
                        )

                    if (not chunk_is_train) and max_test_rows is not None:
                        remaining = max(0, int(max_test_rows) - int(kept_test))
                        if remaining <= 0:
                            budget_hit = True
                            del df
                            continue
                        df = _downsample_keep_all_pos(
                            df=df,
                            target=self.target,
                            max_rows_remaining=remaining,
                            neg_pos_ratio=neg_pos_ratio,
                            min_neg_keep=min_neg_keep_per_chunk,
                            seed=self.random_seed + 10_000 + int(y_from_name or 0) + chunk_idx,
                        )

                    if chunk_is_train:
                        train_parts.append(df)
                        kept_train += int(len(df))
                    else:
                        test_parts.append(df)
                        kept_test += int(len(df))

                    # Libera referencia local antes do proximo batch.
                    del df

                if budget_hit:
                    # Stream pode ter parado cedo por causa do orcamento;
                    # ainda conta como leitura bem-sucedida.
                    self.log.debug(f"[LOAD] budget_hit em {f.name} apos {chunk_idx} chunks")

                read_ok += 1

                # gc entre arquivos: reduz heap-fragmentation em long runs.
                gc.collect()

                if read_ok % 3 == 0:
                    self.log.info(
                        f"[LOAD] ok={read_ok}/{len(files)} | rows_seen={total_rows_seen:,} | "
                        f"kept_train={kept_train:,} kept_test={kept_test:,}"
                    )
                    MemoryMonitor.log_usage(self.log, "durante load streaming")

            except Exception as e:
                read_fail += 1
                self.log.warning(f"[LOAD] falha ao ler {f.name}: {e}")
                gc.collect()

        if valid_features is None:
            raise RuntimeError("[LOAD] nenhum parquet foi carregado com sucesso.")

        train_df = _concat_parts_low_mem(
            train_parts, valid_features, self.target, self.year_col, self.log
        )
        train_parts.clear()
        gc.collect()

        test_df = _concat_parts_low_mem(
            test_parts, valid_features, self.target, self.year_col, self.log
        )
        test_parts.clear()
        gc.collect()

        # Os parquets sao descobertos por ordem alfabetica do filename
        # (inmet_bdq_YYYY_*.parquet) -> ja chegam em ordem cronologica e
        # cada arquivo cai inteiro em train OU test. Logo train_df/test_df
        # sao monotonic non-decreasing em year_col na entrada. Reordenar
        # alocaria uma copia full (~5 GiB para minirocket) sem necessidade.
        # Sort defensivo somente se detectarmos quebra de ordem.
        def _maybe_sort_by_year(df: pd.DataFrame) -> pd.DataFrame:
            if not len(df):
                return df
            yrs = df[self.year_col].to_numpy(copy=False)
            if yrs.size > 1 and not bool(np.all(yrs[1:] >= yrs[:-1])):
                self.log.warning(
                    f"[SORT] {self.year_col} fora de ordem; aplicando sort_values "
                    f"(pico de RAM esperado para esta operacao)."
                )
                return df.sort_values(self.year_col, kind="mergesort").reset_index(drop=True)
            return df.reset_index(drop=True)

        train_df = _maybe_sort_by_year(train_df)
        test_df = _maybe_sort_by_year(test_df)

        downsample_on = bool(max_train_rows is not None or max_test_rows is not None)
        self.log.info(
            f"[LOAD] ok={read_ok} fail={read_fail} | train_rows={len(train_df)} test_rows={len(test_df)} | "
            f"features={len(valid_features)} | downsample={'ON' if downsample_on else 'OFF'}"
        )

        # Audit pos-load: anos efetivos, dups, pos_rate.
        # Para datasets muito largos (e.g. minirocket), drop_duplicates faria
        # hash em ~7M linhas x 180 cols e alocaria uma copia da saida -> pico
        # >2x. Skip nesse caso; auditoria de duplicacao ja e feita upstream
        # via _audit_source_parquets() (na fonte) e via dedupe-stage.
        _DUP_CHECK_MAX_ROWS = 3_000_000

        def _split_summary(df: pd.DataFrame) -> Dict[str, Any]:
            n = int(len(df))
            if n == 0:
                return {"rows": 0, "unique_rows": 0, "dup_ratio": 0.0,
                        "years": [], "pos_count": 0, "pos_rate": 0.0,
                        "exact_dup_check": "empty"}
            if n > _DUP_CHECK_MAX_ROWS:
                exact_uniq = None
                dup_ratio = None
                check_status = f"skipped (n={n:,} > {_DUP_CHECK_MAX_ROWS:,})"
            else:
                exact_uniq = int(df.drop_duplicates().shape[0])
                dup_ratio = round(n / exact_uniq, 4) if exact_uniq else 0.0
                check_status = "ok"
            years_list = sorted({int(y) for y in df[self.year_col].unique().tolist()})
            pos = int((df[self.target] == 1).sum())
            return {
                "rows": n,
                "unique_rows": exact_uniq,
                "dup_ratio": dup_ratio,
                "years": years_list,
                "pos_count": pos,
                "pos_rate": round(pos / n, 6),
                "exact_dup_check": check_status,
            }

        train_stats = _split_summary(train_df)
        test_stats = _split_summary(test_df)

        self._last_data_audit = {
            "scenario_key": self.scenario_key,
            "scenario_folder": self.scenario_folder,
            "parquet_source": self._parquet_source,
            "n_parquets": len(files),
            "read_ok": read_ok,
            "read_fail": read_fail,
            "n_features": len(valid_features),
            "downsample": downsample_on,
            "max_train_rows": max_train_rows,
            "max_test_rows": max_test_rows,
            "train_max_year": train_max_year if years else None,
            "test_size_years": test_size_years,
            "gap_years": gap_years,
            "source": self._last_source_audit,
            "train": train_stats,
            "test": test_stats,
        }
        MemoryMonitor.log_usage(self.log, "apos load batched/concat")

        del train_parts, test_parts
        gc.collect()
        MemoryMonitor.log_usage(self.log, "apos gc pos-load")

        return train_df, test_df, valid_features

    def prepare_eval_split_data(
        self,
        *,
        test_size_years: int,
        gap_years: int = 0,
        max_train_rows: Optional[int] = None,
        max_test_rows: Optional[int] = None,
        neg_pos_ratio: int = 200,
        min_neg_keep_per_chunk: int = 50_000,
        batch_rows: Optional[int] = None,
    ) -> EvalSplitData:
        """Carrega split temporal e retorna X/y prontos para treino/avaliacao."""
        train_df, test_df, valid = self.load_split_batched(
            test_size_years=test_size_years,
            gap_years=gap_years,
            max_train_rows=max_train_rows,
            max_test_rows=max_test_rows,
            neg_pos_ratio=neg_pos_ratio,
            min_neg_keep_per_chunk=min_neg_keep_per_chunk,
            batch_rows=batch_rows,
        )
        if len(train_df) == 0 or len(test_df) == 0:
            raise ValueError("[DATA] train/test vazio. Nao da para treinar/avaliar.")

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

        return EvalSplitData(
            X_train=X_tr,
            y_train=y_tr,
            X_test=X_te,
            y_test=y_te,
            valid_features=valid,
            data_audit=getattr(self, "_last_data_audit", None) or {},
        )

    def run(
        self,
        plan: List[Dict[str, Any]],
        overwrite_all: bool,
        skip_all: bool,
        on_exist: str = "interactive",
        batch_rows: Optional[int] = None,
        max_train_rows_override: Optional[int] = None,
        max_test_rows_override: Optional[int] = None,
    ) -> Tuple[bool, bool]:
        """
        _ensure_trainers_loaded()
        overwrite_all:
          - True: nao pergunta, limpa pasta do run e sobrescreve.
        skip_all:
          - True: nao pergunta, pula se existir.
        on_exist:
          - interactive: pergunta no terminal (legado).
          - skip: pula run se ja existir saida (nao interativo).
          - overwrite: limpa e sobrescreve se ja existir (nao interativo).
          - error: falha se ja existir saida (nao interativo).
        """
        # tf_* and tsfusion scenarios are always derived from *_calculated bases
        is_calculated = (
            "calculated" in (self.scenario_folder or "").lower()
            or self._is_temporal_fusion_scenario()
        )

        self._log_run_banner()

        # Limites default por cenario, com overrides do CLI quando presentes.
        default_max_train = 8_000_000 if is_calculated else None
        default_max_test = 2_000_000 if is_calculated else None
        max_train = (
            max_train_rows_override
            if max_train_rows_override is not None
            else default_max_train
        )
        max_test = (
            max_test_rows_override
            if max_test_rows_override is not None
            else default_max_test
        )

        try:
            split_data = self.prepare_eval_split_data(
                test_size_years=_article_temporal_test_size_years(self.cfg),
                gap_years=0,
                max_train_rows=max_train,
                max_test_rows=max_test,
                neg_pos_ratio=200,
                min_neg_keep_per_chunk=50_000,
                batch_rows=batch_rows,
            )
        except Exception as e:
            self.log.error(f"[CRITICAL] load_split_batched: {e}")
            return overwrite_all, skip_all

        X_tr = split_data.X_train
        y_tr = split_data.y_train
        X_te = split_data.X_test
        y_te = split_data.y_test
        valid = split_data.valid_features

        for item in plan:
            m: str = item["type"]
            st: Dict[str, Any] = item["settings"]

            trainer = None

            ar = self.use_article_data

            if m == "logistic":
                trainer = LogisticTrainer(
                    self.scenario_folder, random_state=self.random_seed, article_results=ar
                )
            elif m == "xgboost":
                trainer = XGBoostTrainer(
                    self.scenario_folder, random_state=self.random_seed, article_results=ar
                )
            elif m == "naive_bayes" and NaiveBayesTrainer is not None:
                trainer = NaiveBayesTrainer(
                    self.scenario_folder, random_state=self.random_seed, article_results=ar
                )
            elif m == "svm" and SVMTrainer is not None:
                trainer = SVMTrainer(
                    self.scenario_folder, random_state=self.random_seed, article_results=ar
                )
            elif m == "random_forest" and RandomForestTrainer is not None:
                trainer = RandomForestTrainer(
                    self.scenario_folder, random_state=self.random_seed, article_results=ar
                )
            elif m.startswith("dummy_"):
                trainer = DummyTrainer(
                    self.scenario_folder,
                    m.split("_", 1)[1],
                    random_state=self.random_seed,
                    article_results=ar,
                )

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
            self.log.info(
                f"[PLANO] model={trainer.model_type} | variation={trainer.run_name} | "
                f"desc={getattr(trainer, 'variation_desc', '')} | scenario_key={self.scenario_key}"
            )

            try:
                exists = _dir_has_outputs(trainer.output_dir)

                if exists and skip_all:
                    print(f"       [SKIP] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder} (skip_all ativo)")
                    continue

                if exists and on_exist == "skip":
                    print(f"       [SKIP] Ja existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder} (--on-exist skip)")
                    continue

                if exists and on_exist == "error":
                    raise TrainRunnerOutputExistsError(
                        f"Saida ja existe (use --on-exist overwrite ou skip): "
                        f".../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder}"
                    )

                if exists and (overwrite_all or on_exist == "overwrite"):
                    print(f"       [OVERWRITE_ALL] Limpando: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder}")
                    _clear_dir(trainer.output_dir)
                elif exists and on_exist == "interactive":
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
                elif exists:
                    # Estado inconsistente: existe mas nenhum ramo tratou
                    self.log.warning(f"[SKIP] saida existente nao tratada para on_exist={on_exist!r}")
                    continue

                pq_resolved = resolve_parquet_dir(
                    self.cfg, self.scenario_folder, source=self._parquet_source
                ).resolve()
                self.log.info("-" * 72)
                self.log.info(f"[TRAIN] inicio | model={trainer.model_type} | variation={trainer.run_name}")
                self.log.info(f"[TRAIN] output_dir={trainer.output_dir}")
                self.log.info(f"[TRAIN] parquet_dir={pq_resolved}")
                MemoryMonitor.log_usage(self.log, "pre-fit")
                t_fit = time.time()
                trainer.train(X_tr, y_tr, **st)
                train_wall_s = time.time() - t_fit
                self.log.info(
                    f"[TRAIN] fim | wall_train_s={train_wall_s:.2f} | model={trainer.model_type}"
                )
                MemoryMonitor.log_usage(self.log, "pos-fit")

                thr = float(st.get("thr", 0.5))
                metrics = trainer.evaluate(X_te, y_te, thr=thr)
                MemoryMonitor.log_usage(self.log, "pos-eval")

                _da = getattr(self, "_last_data_audit", None) or {}
                _train_audit = _da.get("train") or {}
                _test_audit = _da.get("test") or {}
                _src_audit = _da.get("source") or {}
                _tr_years: List[int] = _train_audit.get("years") or []
                _te_years: List[int] = _test_audit.get("years") or []
                _tr_src_rows, _te_src_rows = _source_rows_by_split(
                    _src_audit, _tr_years, _te_years
                )

                run_meta = {
                    "scenario_key": self.scenario_key,
                    "scenario_folder": self.scenario_folder,
                    "parquet_source": self._parquet_source,
                    "parquet_dir": str(pq_resolved),
                    "features_used": valid,
                    "n_features": len(valid),
                    "target": self.target,
                    "year_col": self.year_col,
                    # --- split temporal ---
                    "split_train_max_year": _da.get("train_max_year"),
                    "split_test_size_years": _da.get("test_size_years"),
                    "train_years": _tr_years if _tr_years else None,
                    "test_years": _te_years if _te_years else None,
                    # --- linhas usadas (pos-downsampling, o que o modelo viu) ---
                    "train_rows": int(len(y_tr)),
                    "test_rows": int(len(y_te)),
                    # --- linhas brutas nos parquets dos anos de treino/teste ---
                    "train_rows_total": _tr_src_rows,
                    "test_rows_total": _te_src_rows,
                    "train_pos_rate": _pos_rate(y_tr),
                    "test_pos_rate": _pos_rate(y_te),
                    "settings": st,
                    "threshold": thr,
                    "host_snapshot": MemoryMonitor.get_snapshot(),
                    "train_wall_s": round(train_wall_s, 3),
                    "data_audit": getattr(self, "_last_data_audit", None),
                }

                trainer.save_artifacts(metrics, run_meta=run_meta)

                pr = metrics.get("pr_auc", None)
                pr_str = "None" if pr is None else f"{float(pr):.6f}"
                print(f"       >> OK: PR-AUC={pr_str}")

            except TrainRunnerOutputExistsError:
                raise
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
def available_model_names() -> List[str]:
    """Modelos que o runner pode usar (mesma ordem logica do menu legado)."""
    _ensure_trainers_loaded()
    names: List[str] = ["dummy_stratified", "dummy_prior"]
    if LogisticTrainer is not None:
        names.append("logistic")
    if XGBoostTrainer is not None:
        names.append("xgboost")
    if NaiveBayesTrainer is not None:
        names.append("naive_bayes")
    if SVMTrainer is not None:
        names.append("svm")
    if RandomForestTrainer is not None:
        names.append("random_forest")
    return names


def build_plan_from_specs(
    selected_models: List[str],
    default_variation_keys: List[int],
    per_model_variation: Optional[Dict[str, List[int]]] = None,
) -> List[Dict[str, Any]]:
    """
    Monta o plano de treinos: dummies sem variacao; demais conforme chaves 1-4 por modelo.
    per_model_variation: sobrescreve default_variation_keys para modelos listados.
    """
    per_model_variation = dict(per_model_variation or {})
    plan: List[Dict[str, Any]] = []

    for m in selected_models:
        if m.startswith("dummy_"):
            plan.append({"type": m, "settings": {}})

    for m in selected_models:
        if m.startswith("dummy_"):
            continue

        opts = _variation_menu_legacy(m)
        opt_by_key = {o.key: o for o in opts}
        keys = per_model_variation.get(m, default_variation_keys)
        if not keys:
            keys = [1]

        for k in keys:
            if k not in opt_by_key:
                valid = sorted(opt_by_key.keys())
                raise ValueError(f"Variacao invalida {k} para modelo {m!r} (validas: {valid})")
            o = opt_by_key[k]
            plan.append({"type": m, "settings": dict(o.settings)})

    return plan


def _parse_model_variation_arg(spec: str) -> Tuple[str, List[int]]:
    """Uma entrada tipo logistic=1,2 ou xgboost=4."""
    s = (spec or "").strip()
    if "=" not in s:
        raise ValueError(f"Esperado MODEL=1,2,...; recebido: {spec!r}")
    name, rest = s.split("=", 1)
    name = name.strip()
    keys = _parse_int_tokens(rest)
    if not name:
        raise ValueError(f"Nome de modelo vazio em: {spec!r}")
    if not keys:
        raise ValueError(f"Nenhuma variacao numerica em: {spec!r}")
    return name, keys


def _merge_model_variation_args(specs: List[str]) -> Dict[str, List[int]]:
    out: Dict[str, List[int]] = {}
    for spec in specs:
        name, keys = _parse_model_variation_arg(spec)
        merged: List[int] = []
        seen = set()
        for k in out.get(name, []) + keys:
            if k not in seen:
                merged.append(k)
                seen.add(k)
        out[name] = merged
    return out


def _build_plan(selected_models: List[str]) -> List[Dict[str, Any]]:
    """Modo interativo: pergunta variacoes por modelo e delega a build_plan_from_specs."""
    per_model: Dict[str, List[int]] = {}

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

            per_model[m] = [o.key for o in chosen]
            break

    return build_plan_from_specs(selected_models, default_variation_keys=[1], per_model_variation=per_model)


def _cli_epilog() -> str:
    return """subcomandos:
  run                 Treina com cenarios e modelos escolhidos (nao interativo).
  list-scenarios      Lista chaves em modeling_scenarios (config.yaml).
  list-models         Lista modelos disponiveis neste ambiente.
  describe-variations Mostra opcoes de variacao (1-4) para um --model.
  interactive         Menu legado com input() (bases, modelos, variacoes, overwrite).

Exemplos:
  python src/train_runner.py run --scenario base_E_calculated --model logistic --variations 1
  python src/train_runner.py run --article -s base_E_calculated -m logistic -v 1
"""


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="train_runner.py",
        description="Orquestrador de experimentos: batches por ano, split temporal, metricas por run.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_cli_epilog(),
    )
    sub = p.add_subparsers(dest="command", metavar="COMMAND")

    # --- run ---
    pr = sub.add_parser(
        "run",
        help="Executa treinos para cenarios e modelos informados.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=(
            "Cenarios sao chaves de modeling_scenarios no config.yaml (ex.: base_E_calculated, tf_D_champion). "
            "Variacoes 1-4 correspondem ao menu legado (_variation_menu_legacy)."
        ),
        epilog=(
            "Exemplos:\n"
            "  python src/train_runner.py run -s base_E_calculated -m logistic -v 1\n"
            "  python src/train_runner.py run --article -s base_E_calculated -m logistic -v 1\n"
            "  python src/train_runner.py run --article -s tf_E_champion -m xgboost -v 1 2\n"
            "  python src/train_runner.py run -s base_A -s base_B -m logistic --model-variation logistic=2,3\n"
            "  python src/train_runner.py run -s base_E_calculated -m logistic --dry-run\n"
        ),
    )
    pr.add_argument(
        "--scenario",
        "--base",
        dest="scenarios",
        action="append",
        metavar="KEY",
        help="Chave de modeling_scenarios (repita ou use varias em uma unica string com -s uma vez).",
    )
    pr.add_argument(
        "-s",
        dest="scenarios_short",
        action="append",
        metavar="KEY",
        help="Atalho para --scenario.",
    )
    pr.add_argument(
        "--model",
        "-m",
        dest="models",
        action="append",
        metavar="NAME",
        help="Nome do modelo: dummy_stratified, dummy_prior, logistic, xgboost, ... (repita para varios).",
    )
    pr.add_argument(
        "--variations",
        "-v",
        nargs="*",
        metavar="N",
        help="Chaves de variacao 1-4 para todos os modelos nao-dummy (default: 1). Ex.: -v 1 2 ou -v 1,2",
    )
    pr.add_argument(
        "--model-variation",
        action="append",
        metavar="MODEL=N[,N]",
        help="Variacoes por modelo (repetivel). Ex.: --model-variation logistic=1,2 --model-variation xgboost=4",
    )
    pr.add_argument(
        "--on-exist",
        choices=("skip", "overwrite", "error"),
        default="skip",
        help="Se ja existir saida do run: skip (default), overwrite ou error.",
    )
    pr.add_argument(
        "--dry-run",
        action="store_true",
        help="So valida entradas e imprime o plano; nao carrega dados nem treina.",
    )
    pr.add_argument(
        "--batch-rows",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Rows por batch no streaming de parquets (default: env TRAIN_RUNNER_BATCH_ROWS "
            f"ou {_DEFAULT_BATCH_ROWS}). Reduza em maquinas com pouca RAM (ex.: 200000)."
        ),
    )
    pr.add_argument(
        "--max-train-rows",
        type=int,
        default=None,
        metavar="N",
        help="Override do limite de linhas de treino (default: 8M em cenarios *_calculated/tf_*).",
    )
    pr.add_argument(
        "--max-test-rows",
        type=int,
        default=None,
        metavar="N",
        help="Override do limite de linhas de teste (default: 2M em cenarios *_calculated/tf_*).",
    )
    pr.add_argument(
        "--article",
        action="store_true",
        help=(
            "Le parquets do pipeline do artigo: paths.data.article / "
            "0_datasets_with_coords (ou 1_datasets_with_fusion para tf_*). "
            "Grava metricas/modelos em data/_article/<train_runner_results_subdir>/ "
            "(default _results), por tipo de modelo e variacao — ver article_pipeline no config.yaml."
        ),
    )

    # --- list-scenarios ---
    ps = sub.add_parser(
        "list-scenarios",
        help="Lista chaves de modeling_scenarios disponiveis no config.",
    )
    ps.add_argument(
        "--show-folders",
        action="store_true",
        help="Mostra tambem o scenario_folder de cada chave.",
    )

    # --- list-models ---
    sub.add_parser("list-models", help="Lista nomes de modelos usaveis neste ambiente.")

    # --- describe-variations ---
    pdv = sub.add_parser(
        "describe-variations",
        help="Imprime as opcoes de variacao (1-4) para um modelo.",
    )
    pdv.add_argument(
        "--model",
        "-m",
        required=True,
        metavar="NAME",
        help="Ex.: logistic, xgboost, random_forest",
    )

    # --- interactive ---
    sub.add_parser(
        "interactive",
        help="Menu interativo legado (input): bases, modelos, variacoes e conflitos de pasta.",
    )

    return p


def cmd_run(args: argparse.Namespace) -> None:
    cfg = utils.loadConfig()
    scens: Dict[str, Any] = cfg.get("modeling_scenarios") or {}
    if not scens:
        print("[CRITICAL] modeling_scenarios vazio no config.yaml")
        sys.exit(1)

    raw_lists: List[str] = []
    if args.scenarios:
        raw_lists.extend(args.scenarios)
    if getattr(args, "scenarios_short", None):
        raw_lists.extend(args.scenarios_short)
    bases: List[str] = []
    seen_b = set()
    for chunk in raw_lists:
        for name in _parse_name_tokens(chunk):
            if name not in seen_b:
                bases.append(name)
                seen_b.add(name)

    if not bases:
        print("[ERROR] Informe ao menos um cenario: --scenario KEY (ou -s KEY).")
        sys.exit(2)

    for b in bases:
        if not _scenario_accepted_for_run(cfg, b, use_article=args.article):
            print(f"[ERROR] Cenario invalido {b!r}. Chaves validas (ordenadas): {sorted(scens.keys())}")
            if args.article:
                cr = article_coords_root(cfg)
                coords = list_article_coord_dataset_folders(cfg)
                print(
                    f"  Com --article: pasta em {cr} com *.parquet, ou chave tf_* "
                    f"(ewma_lags/minirocket/champion) com dados em {article_fusion_output_root(cfg)}. "
                    f"Exemplos coords: {coords[:12]}{'...' if len(coords) > 12 else ''}"
                )
            sys.exit(2)

    avail = available_model_names()
    avail_set = set(avail)
    models_raw: List[str] = []
    if args.models:
        for chunk in args.models:
            models_raw.extend(_parse_name_tokens(chunk))
    models: List[str] = []
    seen_m = set()
    for m in models_raw:
        if m not in seen_m:
            models.append(m)
            seen_m.add(m)

    if not models:
        print("[ERROR] Informe ao menos um modelo: --model NAME (ou -m NAME).")
        sys.exit(2)

    for m in models:
        if m not in avail_set:
            print(f"[ERROR] Modelo invalido {m!r}. Disponiveis: {avail}")
            sys.exit(2)

    var_toks: List[str] = []
    if args.variations is not None:
        var_toks = list(args.variations)
    default_keys = _flatten_variation_args(var_toks) if var_toks else [1]
    if not default_keys:
        default_keys = [1]

    per_model: Optional[Dict[str, List[int]]] = None
    if args.model_variation:
        per_model = _merge_model_variation_args(args.model_variation)
        for m in per_model:
            if m not in avail_set:
                print(f"[ERROR] Modelo em --model-variation desconhecido: {m!r}. Disponiveis: {avail}")
                sys.exit(2)

    try:
        plan = build_plan_from_specs(models, default_variation_keys=default_keys, per_model_variation=per_model)
    except ValueError as e:
        print(f"[ERROR] {e}")
        sys.exit(2)

    print(f"\n{'=' * 40}")
    print(f"PLANO: {len(bases)} cenario(s) x {len(plan)} run(s) por cenario")
    print(f"Cenarios: {bases}")
    print(f"Fonte dos parquets: {'article (data/_article/...)' if args.article else 'tcc (data/modeling/ ou temporal_fusion/)'}")
    print(f"Modelos/variacoes: {len(plan)} entradas no plano")
    print(f"on-exist: {args.on_exist}")
    print(f"{'=' * 40}")

    if args.dry_run:
        print("[dry-run] Encerrando sem treinar.")
        return

    overwrite_all = False
    skip_all = False
    on_exist = args.on_exist

    for i, b in enumerate(bases):
        print(f"\n>>> [CENARIO {i + 1}/{len(bases)}] {b}")
        try:
            orc = TrainingOrchestrator(b, use_article_data=args.article)
            overwrite_all, skip_all = orc.run(
                plan,
                overwrite_all=overwrite_all,
                skip_all=skip_all,
                on_exist=on_exist,
                batch_rows=getattr(args, "batch_rows", None),
                max_train_rows_override=getattr(args, "max_train_rows", None),
                max_test_rows_override=getattr(args, "max_test_rows", None),
            )
        except TrainRunnerOutputExistsError as e:
            print(f"[ERROR] {e}")
            sys.exit(1)
        except Exception as e:
            print(e)
        gc.collect()


def cmd_list_scenarios(args: argparse.Namespace) -> None:
    cfg = utils.loadConfig()
    scens = cfg.get("modeling_scenarios") or {}
    for k in sorted(scens.keys()):
        if args.show_folders:
            print(f"{k}\t{scens[k]}")
        else:
            print(k)


def cmd_list_models(_args: argparse.Namespace) -> None:
    for m in available_model_names():
        print(m)


def cmd_describe_variations(args: argparse.Namespace) -> None:
    m = args.model.strip()
    avail = [x for x in available_model_names() if not x.startswith("dummy_")]
    if m not in avail:
        print(f"[ERROR] Modelo {m!r} nao suporta variacao ou nao existe. Candidatos: {avail}")
        sys.exit(2)
    opts = _variation_menu_legacy(m)
    print(f"Variacoes para modelo={m}:\n")
    for o in opts:
        print(f"  [{o.key}] {o.label}")


def cmd_interactive(_args: argparse.Namespace) -> None:
    cfg = utils.loadConfig()
    scens = cfg.get("modeling_scenarios") or {}
    if not scens:
        print("[CRITICAL] modeling_scenarios vazio no config.yaml")
        return

    print("\n--- Fonte dos Parquets ---")
    print("[1] TCC — data/modeling/ ou data/temporal_fusion/ (padrao)")
    print("[2] Artigo — coords (0_datasets_with_coords) e fusao (1_datasets_with_fusion: ewma_lags | minirocket | champion)")
    use_article_data = False
    while True:
        x = input(">> Fonte [1]: ").strip().lower()
        if not x or x in ("1", "tcc", "t"):
            use_article_data = False
            break
        if x in ("2", "article", "art", "a"):
            use_article_data = True
            break
        print("Entrada invalida. Digite 1 ou 2.")

    if use_article_data:
        cr = article_coords_root(cfg)
        fusion_root = article_fusion_output_root(cfg)
        coord = list_article_coord_dataset_folders(cfg)
        fusion_keys = list_article_fusion_train_menu_keys(cfg)

        coord_bases: List[str] = []
        fusion_bases: List[str] = []

        if coord:
            print("\n=== [COORDS] Bases em 0_datasets_with_coords (com *.parquet) ===")
            print(f"    Raiz: {cr.resolve()}")
            coord_opts = {i + 1: name for i, name in enumerate(coord)}
            coord_bases = _select_many(
                coord_opts,
                "Selecione coords (Enter vazio = nenhuma)",
                allow_empty=True,
            )
        else:
            print(f"\n[INFO] Nenhuma subpasta com *.parquet em {cr.resolve()}")

        if fusion_keys:
            print("\n=== [FUSAO] Cenarios em 1_datasets_with_fusion — metodos: ewma_lags | minirocket | champion ===")
            print(f"    Raiz: {fusion_root.resolve()}")
            print("    (SARIMAX: adicione tf_*_sarimax_exog em modeling_scenarios + temporal_fusion_paths se precisar.)")
            fusion_opts = {i + 1: k for i, k in enumerate(fusion_keys)}
            fusion_bases = _select_many(
                fusion_opts,
                "Selecione fusao (Enter vazio = nenhuma)",
                allow_empty=True,
            )
        else:
            print(
                f"\n[INFO] Nenhum cenario ewma_lags/minirocket/champion com parquets sob {fusion_root.resolve()}"
            )

        bases = coord_bases + fusion_bases
        if not bases:
            print(
                "\n[WARN] Nenhuma base selecionada e nada encontrado no disco. "
                "Usando lista completa do config (modeling_scenarios)."
            )
            bases = _select_many(
                {i + 1: k for i, k in enumerate(sorted(scens.keys()))},
                "Bases (fallback: config)",
            )
    else:
        bases = _select_many({i + 1: k for i, k in enumerate(sorted(scens.keys()))}, "Bases (TCC)")

    models_dict = {i + 1: name for i, name in enumerate(available_model_names())}
    models = _select_many(models_dict, "Modelos")

    plan = _build_plan(models)

    print(f"\n{'=' * 40}")
    print(f"BATCH START: {len(bases)} Bases x {len(plan)} Runs")
    print(
        f"Fonte dos parquets: {'article' if use_article_data else 'tcc'}"
    )
    print(f"{'=' * 40}")

    overwrite_all = False
    skip_all = False

    for i, b in enumerate(bases):
        print(f"\n>>> [BASE {i + 1}/{len(bases)}] {b}")
        try:
            orc = TrainingOrchestrator(b, use_article_data=use_article_data)
            overwrite_all, skip_all = orc.run(
                plan,
                overwrite_all=overwrite_all,
                skip_all=skip_all,
                on_exist="interactive",
            )
        except Exception as e:
            print(e)
        gc.collect()


def main(argv: Optional[List[str]] = None) -> None:
    argv = list(sys.argv[1:] if argv is None else argv)
    parser = build_arg_parser()
    if not argv:
        parser.print_help()
        return

    args = parser.parse_args(argv)
    cmd = args.command
    if cmd is None:
        parser.print_help()
        return

    if cmd == "run":
        cmd_run(args)
    elif cmd == "list-scenarios":
        cmd_list_scenarios(args)
    elif cmd == "list-models":
        cmd_list_models(args)
    elif cmd == "describe-variations":
        cmd_describe_variations(args)
    elif cmd == "interactive":
        cmd_interactive(args)
    else:
        parser.error(f"Comando desconhecido: {cmd!r}")


if __name__ == "__main__":
    main()
