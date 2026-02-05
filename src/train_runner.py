# src/train_runner.py
# =============================================================================
# ORQUESTRADOR DE EXPERIMENTOS
# =============================================================================

import pandas as pd
import sys
import gc
from pathlib import Path
from typing import Dict, List, Any

# Path Setup
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path: sys.path.append(str(project_root))

try:
    import src.utils as utils
    from src.ml.core import TemporalSplitter
    from src.models.logistic import LogisticTrainer
    from src.models.dummy import DummyTrainer
    from src.models.xgboost_model import XGBoostTrainer
except ImportError as e:
    sys.exit(f"[CRITICAL] Dependências: {e}")

class TrainingOrchestrator:
    def __init__(self, scenario_key: str):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)
        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg['modeling_scenarios'].get(scenario_key)
        if not self.scenario_folder: raise ValueError(f"Cenário {scenario_key} inválido.")

        self.features = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)', 'RADIACAO GLOBAL (KJ/m²)',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)', 'UMIDADE RELATIVA DO AR, HORARIA (%)',
            'VENTO, VELOCIDADE HORARIA (m/s)', 'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)',
            'VENTO, RAJADA MAXIMA (m/s)', 'precip_ewma', 'dias_sem_chuva', 'risco_temp_max', 
            'risco_umid_critica', 'risco_umid_alerta', 'fator_propagacao'
        ]
        self.target = 'HAS_FOCO'
        self.year = 'ANO'

    def _get_folder(self, m_type: str, st: Dict) -> str:
        """Define o nome da PASTA DE VARIAÇÃO (Subpasta)"""
        suffix = []
        if st.get('use_smote'): suffix.append("SMOTE")
        if st.get('use_scale'): suffix.append("Scale")
        
        if m_type == 'xgboost':
            if not st.get('optimize'): return "XGBoost_Fast_Base"
            if not suffix: suffix.append("GridOnly")
            return f"XGBoost_{'_'.join(suffix)}"
            
        if m_type == 'logistic':
            if not st.get('optimize'): return "Logistic_Fast_Base"
            if not suffix: suffix.append("GridOnly")
            return f"Logistic_{'_'.join(suffix)}"
            
        if 'dummy' in m_type: return f"Dummy_{m_type.split('_')[1]}"
        return "Unknown"

    def _check_perm(self, folder: str, auto: bool) -> Any:
        if auto: return True
        # Nota: Agora a verificação de existência é um pouco mais genérica porque
        # a pasta exata depende do ModelType que só instanciamos depois.
        # Mas podemos verificar a pasta da variação.
        root = Path(self.cfg['paths']['data']['modeling']) / "results"
        # Precisamos achar onde essa variação cairia. 
        # Simplificação: verificamos se existe *alguma* pasta com esse nome de variação dentro de qualquer modelo?
        # Melhor: O usuário só quer saber se vai sobrescrever.
        # Assumindo a estrutura nova, vamos confiar no fluxo.
        return True

    def load_data(self) -> pd.DataFrame:
        path = Path(self.cfg['paths']['data']['modeling']) / self.scenario_folder
        self.log.info(f"Load: {self.scenario_folder}")
        files = sorted(path.glob("*.parquet"))
        if not files: raise FileNotFoundError(f"Sem parquets em {path}")
        
        try:
            import pyarrow.parquet as pq
            avail = pq.read_schema(files[0]).names
            cols = [c for c in self.features if c in avail] + [self.target, self.year]
        except: cols = None

        dfs = []
        for f in files:
            try:
                df = pd.read_parquet(f, columns=cols)
                for c in df.select_dtypes('float64').columns: df[c] = df[c].astype('float32')
                dfs.append(df)
            except: pass
        return pd.concat(dfs, ignore_index=True)

    def run(self, plan: List[Dict], auto: bool) -> bool:
        try: df = self.load_data()
        except Exception as e: self.log.error(e); return auto
        
        valid = [f for f in self.features if f in df.columns]
        df.dropna(subset=valid + [self.target], inplace=True)
        
        splitter = TemporalSplitter(test_size_years=2)
        try:
            train, test = splitter.split_holdout(df, self.year)
            X_tr, y_tr = train[valid], train[self.target]
            X_te, y_te = test[valid], test[self.target]
            self.log.info(f"Split: Tr={len(train)} Te={len(test)}")
            del df, train, test; gc.collect()
        except Exception as e: self.log.error(e); return auto

        for item in plan:
            m, st = item['type'], item['settings']
            variation_folder = self._get_folder(m, st)
            print(f"\n    >> [MODELO] {variation_folder} @ {self.scenario_key}")
            
            trainer = None
            if m == 'logistic': trainer = LogisticTrainer(self.scenario_folder)
            elif m == 'xgboost': trainer = XGBoostTrainer(self.scenario_folder)
            elif 'dummy' in m: trainer = DummyTrainer(self.scenario_folder, m.split('_')[1])
            
            if trainer:
                try:
                    # AQUI A MÁGICA: O runner passa o nome da variação (Subpasta)
                    # O trainer já sabe o ModelType (Pasta Mãe)
                    trainer.set_custom_folder_name(variation_folder)
                    
                    # Checagem de segurança de arquivo DEPOIS de definir o path correto
                    if not auto and trainer.output_dir.exists() and any(trainer.output_dir.iterdir()):
                        print(f"       [AVISO] Já existe: .../{trainer.model_type}/{variation_folder}/{self.scenario_folder}")
                        r = input("       >> Sobrescrever? [y/N/all]: ").strip().lower()
                        if r == 'all': auto = True
                        elif r not in ['y', 'yes']: continue

                    trainer.train(X_tr, y_tr, **st)
                    metrics = trainer.evaluate(X_te, y_te)
                    trainer.save_artifacts(metrics)
                    print(f"       >> OK: PR-AUC={metrics.get('pr_auc',0):.4f}")
                except Exception as e:
                    self.log.error(f"Erro {m}: {e}")
                    import traceback; traceback.print_exc()

        del X_tr, y_tr, X_te, y_te; gc.collect()
        return auto

# --- CLI ---
def select(opts, title):
    print(f"\n--- {title} ---")
    for k, v in opts.items(): print(f"[{k}] {v}")
    while True:
        x = input(">> Select (ex: 1,3 or 'all'): ").strip().lower()
        if x == 'all': return list(opts.values())
        try: return [opts[int(i)] for i in x.split(',') if int(i) in opts]
        except: pass

def config_model(m):
    if m in ['xgboost', 'logistic']:
        print(f"\n[CFG] {m.upper()}")
        print("[1] Fast (Base+Weight) | [2] Monster (Grid+SMOTE+Weight) | [3] SMOTE Only | [4] Scale Only")
        x = input(">> Opt [1]: ").strip()
        if x == '2': return {'optimize': True, 'use_smote': True, 'use_scale': True}
        if x == '3': return {'optimize': True, 'use_smote': True, 'use_scale': False}
        if x == '4': return {'optimize': True, 'use_smote': False, 'use_scale': True}
        return {'optimize': False, 'use_smote': False, 'use_scale': True}
    return {}

def main():
    cfg = utils.loadConfig()
    scens = cfg.get('modeling_scenarios', {})
    if not scens: return
    
    bases = select({i+1: k for i, k in enumerate(sorted(scens.keys()))}, "Bases")
    models = select({1: "dummy_stratified", 2: "dummy_prior", 3: "logistic", 4: "xgboost"}, "Modelos")
    
    unique = set(models)
    cfgs = {m: config_model(m) for m in unique}
    plan = [{'type': m, 'settings': cfgs[m]} for m in models]
    
    print(f"\n{'='*40}\nBATCH START: {len(bases)} Bases x {len(plan)} Models\n{'='*40}")
    auto = False
    for i, b in enumerate(bases):
        print(f"\n>>> [BASE {i+1}/{len(bases)}] {b}")
        try:
            orc = TrainingOrchestrator(b)
            auto = orc.run(plan, auto)
        except Exception as e: print(e)
        gc.collect()

if __name__ == "__main__": main()