# src/train_runner.py
# =============================================================================
# ORQUESTRADOR DE EXPERIMENTOS (REFATORADO — NOMENCLATURA PROFISSIONAL)
# =============================================================================

import gc
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
    sys.exit(f"[CRITICAL] Dependências: {e}")


def _pos_rate(y: pd.Series) -> float:
    try:
        n = len(y)
        if n == 0:
            return 0.0
        return float(y.sum()) / float(n)
    except Exception:
        return 0.0


class TrainingOrchestrator:
    def __init__(self, scenario_key: str):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)

        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg["modeling_scenarios"].get(scenario_key)
        if not self.scenario_folder:
            raise ValueError(f"Cenário {scenario_key} inválido.")

        # Seed central (cai no default se não existir)
        self.random_seed = int(self.cfg.get("project", {}).get("random_seed", 42))

        # Features/target/ano (mantém seu padrão; se quiser, depois dá pra mover pro config)
        self.features = [
            "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)",
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
        self.year = "ANO"

    # ----------------------------
    # Variações (folder + metadata)
    # ----------------------------
    def _variation_meta(self, st: Dict[str, Any]) -> Tuple[str, List[str], str]:
        """
        Retorna:
          run_name: nome da subpasta (ex: base, weight, smote, gridsearch_smote_weight, ...)
          tags: ["gridsearch","smote","weight"]
          desc: descrição humana para salvar no JSON
        Observação: aqui 'use_scale' = balanceamento por PESO (class_weight / scale_pos_weight).
        """
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
            mapping = {
                "gridsearch": "GridSearchCV",
                "smote": "SMOTE",
                "weight": "balanceamento por peso",
            }
            desc = " + ".join(mapping[t] for t in tags)

        return run_name, tags, desc

    # ----------------------------
    # Data Loading
    # ----------------------------
    def load_data(self) -> pd.DataFrame:
        base_path = Path(self.cfg["paths"]["data"]["modeling"])
        path = base_path / self.scenario_folder

        self.log.info(f"[LOAD] scenario_folder={self.scenario_folder} | path={path}")
        files = sorted(path.glob("*.parquet"))
        if not files:
            raise FileNotFoundError(f"Sem parquets em {path}")

        # Tenta ler schema do primeiro parquet para reduzir I/O
        cols = None
        try:
            import pyarrow.parquet as pq  # type: ignore

            avail = pq.read_schema(files[0]).names
            cols = [c for c in self.features if c in avail] + [self.target, self.year]
            self.log.info(f"[LOAD] colunas selecionadas via schema: {len(cols)}")
        except Exception as e:
            self.log.warning(f"[LOAD] sem pyarrow/schema otimizado (vai ler colunas padrão): {e}")

        dfs: List[pd.DataFrame] = []
        failed = 0

        for f in files:
            try:
                df = pd.read_parquet(f, columns=cols)
                # downcast float64 -> float32 (economiza memória)
                for c in df.select_dtypes("float64").columns:
                    df[c] = df[c].astype("float32")
                dfs.append(df)
            except Exception as e:
                failed += 1
                self.log.warning(f"[LOAD] falha ao ler {f.name}: {e}")

        if not dfs:
            raise RuntimeError("[LOAD] nenhum parquet foi carregado com sucesso.")

        out = pd.concat(dfs, ignore_index=True)
        self.log.info(f"[LOAD] lidos={len(files) - failed}/{len(files)} | rows={len(out)} | cols={len(out.columns)}")
        MemoryMonitor.log_usage(self.log, "após load/concat")
        return out

    # ----------------------------
    # Run
    # ----------------------------
    def run(self, plan: List[Dict[str, Any]], auto: bool) -> bool:
        try:
            df = self.load_data()
        except Exception as e:
            self.log.error(f"[CRITICAL] load_data: {e}")
            return auto

        valid = [f for f in self.features if f in df.columns]

        needed = valid + [self.target, self.year]
        df.dropna(subset=needed, inplace=True)

        # Garantir ANO numérico (TimeSeriesSplit depende da ordem!)
        df[self.year] = pd.to_numeric(df[self.year], errors="coerce")
        df.dropna(subset=[self.year], inplace=True)
        df[self.year] = df[self.year].astype(int)

        if len(df) == 0:
            self.log.error("[DATA] Dataset vazio após dropna/limpeza.")
            return auto

        splitter = TemporalSplitter(test_size_years=2)
        try:
            train, test = splitter.split_holdout(df, self.year)

            # Ordena por ANO para dar sentido ao TimeSeriesSplit
            train = train.sort_values(self.year).reset_index(drop=True)
            test = test.sort_values(self.year).reset_index(drop=True)

            X_tr, y_tr = train[valid], train[self.target].astype(int)
            X_te, y_te = test[valid], test[self.target].astype(int)

            self.log.info(
                f"[SPLIT] train={len(train)} (pos_rate={_pos_rate(y_tr):.4%}) | "
                f"test={len(test)} (pos_rate={_pos_rate(y_te):.4%}) | features={len(valid)}"
            )
            del df, train, test
            gc.collect()
            MemoryMonitor.log_usage(self.log, "após split")
        except Exception as e:
            self.log.error(f"[CRITICAL] split_holdout: {e}")
            return auto

        for item in plan:
            m: str = item["type"]
            st: Dict[str, Any] = item["settings"]

            trainer = None
            if m == "logistic":
                trainer = LogisticTrainer(self.scenario_folder, random_state=self.random_seed)
            elif m == "xgboost":
                trainer = XGBoostTrainer(self.scenario_folder, random_state=self.random_seed)
            elif "dummy" in m:
                # m = dummy_stratified, dummy_prior, ...
                trainer = DummyTrainer(self.scenario_folder, m.split("_", 1)[1], random_state=self.random_seed)

            if trainer is None:
                self.log.warning(f"[SKIP] modelo desconhecido: {m}")
                continue

            # Define variação profissional (subpasta) ANTES da checagem de overwrite
            if m in ("logistic", "xgboost"):
                run_name, tags, desc = self._variation_meta(st)
                trainer.set_custom_folder_name(run_name)
                # Preserva metadata para o JSON (porque set_custom bloqueia o auto-set interno)
                trainer.variation_tags = tags
                trainer.variation_desc = desc

            print(f"\n    >> [MODELO] {trainer.model_type}/{trainer.run_name} — {getattr(trainer, 'variation_desc', '')} @ {self.scenario_key}")

            try:
                # Checagem de segurança DEPOIS de definir o path correto
                if not auto and trainer.output_dir.exists() and any(trainer.output_dir.iterdir()):
                    print(f"       [AVISO] Já existe: .../{trainer.model_type}/{trainer.run_name}/{self.scenario_folder}")
                    r = input("       >> Sobrescrever? [y/N/all]: ").strip().lower()
                    if r == "all":
                        auto = True
                    elif r not in ["y", "yes"]:
                        continue

                # Treino + avaliação
                trainer.train(X_tr, y_tr, **st)
                thr = float(st.get("thr", 0.5))
                metrics = trainer.evaluate(X_te, y_te, thr=thr)

                # Metadata útil no JSON de métricas
                run_meta = {
                    "scenario_key": self.scenario_key,
                    "scenario_folder": self.scenario_folder,
                    "features_used": valid,
                    "target": self.target,
                    "year_col": self.year,
                    "train_rows": int(len(y_tr)),
                    "test_rows": int(len(y_te)),
                    "train_pos_rate": _pos_rate(y_tr),
                    "test_pos_rate": _pos_rate(y_te),
                    "settings": st,
                    "threshold": thr,
                }

                trainer.save_artifacts(metrics, run_meta=run_meta)

                pr = metrics.get("pr_auc", None)
                pr_str = "None" if pr is None else f"{float(pr):.4f}"
                print(f"       >> OK: PR-AUC={pr_str}")

            except Exception as e:
                self.log.error(f"[ERROR] {m}: {e}")
                import traceback

                traceback.print_exc()

        del X_tr, y_tr, X_te, y_te
        gc.collect()
        MemoryMonitor.log_usage(self.log, "fim do batch")
        return auto


# ----------------------------
# CLI helpers
# ----------------------------
def select(opts: Dict[int, Any], title: str):
    print(f"\n--- {title} ---")
    for k, v in opts.items():
        print(f"[{k}] {v}")
    while True:
        x = input(">> Select (ex: 1,3 or 'all'): ").strip().lower()
        if x == "all":
            return list(opts.values())
        try:
            return [opts[int(i)] for i in x.split(",") if int(i) in opts]
        except Exception:
            pass


def config_model(m: str) -> Dict[str, Any]:
    """
    Para logistic e xgboost, cria configs com nomenclatura acadêmica:
      - base
      - weight (balanceamento por peso)
      - smote
      - smote_weight
      - gridsearch
      - gridsearch_weight
      - gridsearch_smote
      - gridsearch_smote_weight
    Obs: chave permanece 'use_scale' por compatibilidade com os trainers atuais,
         mas aqui ela significa PESO (class_weight / scale_pos_weight).
    """
    if m not in ["xgboost", "logistic"]:
        return {}

    defaults = {
        "cv_splits": 3,
        "scoring": "average_precision",
        "smote_sampling_strategy": 0.1,
        "smote_k_neighbors": 5,
        "thr": 0.5,
    }

    # Para logística, scaler costuma ser desejável
    if m == "logistic":
        defaults["feature_scaling"] = True

    options = {
        1: ("base (sem SMOTE, sem GridSearch, sem peso)", {"optimize": False, "use_smote": False, "use_scale": False}),
        2: ("weight (sem SMOTE, sem GridSearch)", {"optimize": False, "use_smote": False, "use_scale": True}),
        3: ("smote (sem GridSearch, sem peso)", {"optimize": False, "use_smote": True, "use_scale": False}),
        4: ("smote + weight (sem GridSearch)", {"optimize": False, "use_smote": True, "use_scale": True}),
        5: ("gridsearch (sem SMOTE, sem peso)", {"optimize": True, "use_smote": False, "use_scale": False}),
        6: ("gridsearch + weight (sem SMOTE)", {"optimize": True, "use_smote": False, "use_scale": True}),
        7: ("gridsearch + smote (sem peso)", {"optimize": True, "use_smote": True, "use_scale": False}),
        8: ("gridsearch + smote + weight", {"optimize": True, "use_smote": True, "use_scale": True}),
    }

    print(f"\n[CFG] {m.upper()} — escolha a variação")
    for k, (label, _) in options.items():
        print(f"[{k}] {label}")

    x = input(">> Opt [2]: ").strip()  # default: weight (fast)
    try:
        k = int(x) if x else 2
        base_cfg = options[k][1]
    except Exception:
        base_cfg = options[2][1]

    # Merge defaults + base_cfg
    out = {**defaults, **base_cfg}
    return out


def main():
    cfg = utils.loadConfig()
    scens = cfg.get("modeling_scenarios", {})
    if not scens:
        print("[CRITICAL] modeling_scenarios vazio no config.yaml")
        return

    bases = select({i + 1: k for i, k in enumerate(sorted(scens.keys()))}, "Bases")
    models = select({1: "dummy_stratified", 2: "dummy_prior", 3: "logistic", 4: "xgboost"}, "Modelos")

    unique = set(models)
    cfgs = {m: config_model(m) for m in unique}
    plan = [{"type": m, "settings": cfgs[m]} for m in models]

    print(f"\n{'=' * 40}\nBATCH START: {len(bases)} Bases x {len(plan)} Models\n{'=' * 40}")
    auto = False

    for i, b in enumerate(bases):
        print(f"\n>>> [BASE {i + 1}/{len(bases)}] {b}")
        try:
            orc = TrainingOrchestrator(b)
            auto = orc.run(plan, auto)
        except Exception as e:
            print(e)
        gc.collect()


if __name__ == "__main__":
    main()
