# src/train_runner.py
# =============================================================================
# RUNNER DE TREINAMENTO - TCC WILDFIRE PREDICTION
# =============================================================================
# Orquestra o carregamento de dados, split temporal e treinamento de modelos.
# CLI Interativa com Trava de Seguranca contra Sobrescrita.
# =============================================================================

import pandas as pd
import sys
import shutil
from pathlib import Path
from typing import Dict, Optional

# --- CORRECAO DE PATH ---
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
# ------------------------

try:
    import src.utils as utils
    from src.ml.core import TemporalSplitter
    from src.models.logistic import LogisticTrainer
    from src.models.dummy import DummyTrainer
    from src.models.xgboost_model import XGBoostTrainer
except ImportError as e:
    print(f"[ERRO CRITICO] Falha na importacao: {e}")
    sys.exit(1)

class TrainingOrchestrator:
    def __init__(self, scenario_key: str):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)
        
        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg['modeling_scenarios'].get(scenario_key)
        
        if not self.scenario_folder:
            raise ValueError(f"Cenario '{scenario_key}' nao definido no config.yaml")

        # Lista MESTRA de features (Conforme metadados do INMET)
        self.features = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
            'RADIACAO GLOBAL (KJ/m²)',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)',
            'UMIDADE RELATIVA DO AR, HORARIA (%)',
            'VENTO, VELOCIDADE HORARIA (m/s)',
            'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)',
            'VENTO, RAJADA MAXIMA (m/s)'
        ]
        self.target = 'HAS_FOCO'
        self.year_col = 'ANO'

    def _get_custom_folder_name(self, model_type: str, settings: Dict) -> str:
        """Gera nome de pasta específico baseado nas configurações."""
        base_name = "Unknown"
        
        # 1. Nomes Padrão para modelos simples
        if model_type == 'logistic':
            base_name = "LogisticRegression"
            if settings.get('optimize'): base_name += "_Optimized"
            return base_name
        elif 'dummy' in model_type:
            return f"DummyClassifier_{model_type.split('_')[1]}"
            
        # 2. Lógica avançada para XGBoost
        if model_type == 'xgboost':
            base_name = "XGBoost"
            optimize = settings.get('optimize', False)
            smote = settings.get('use_smote', False)
            scale = settings.get('use_scale', False)
            
            if not optimize:
                # Se não otimiza, assume que é o modo rápido (que usa scale por padrão)
                return f"{base_name}_Fast_Base"
            
            # Se otimiza, verificamos os sufixos
            suffixes = []
            if smote: suffixes.append("SMOTE")
            if scale: suffixes.append("Scale")
            
            if not suffixes: 
                suffixes.append("GridSearchOnly") # Caso raro (otimiza sem nada)
            
            return f"{base_name}_{'_'.join(suffixes)}"
            
        return base_name

    def _check_overwrite_permission(self, folder_name: str) -> bool:
        """Verifica se existem resultados anteriores na pasta calculada."""
        results_dir = (Path(self.cfg['paths']['data']['modeling']) / 
                       "results" / 
                       folder_name / 
                       self.scenario_folder)
        
        if results_dir.exists() and any(results_dir.iterdir()):
            print(f"\n{'!'*60}")
            print(f"[ALERTA DE SOBRESCRITA]")
            print(f"Pasta destino: {folder_name}")
            print(f"Ja existem resultados salvos em:\n-> {results_dir}")
            print(f"{'!'*60}")
            
            while True:
                response = input(">> Tem certeza que deseja rodar novamente e SOBRESCREVER? [y/N]: ").strip().lower()
                if response in ['n', 'no', '']:
                    print(">> Operacao cancelada pelo usuario.")
                    return False
                elif response in ['y', 'yes']:
                    print(">> Sobrescrevendo dados antigos...")
                    return True
        return True

    def load_full_scenario(self) -> pd.DataFrame:
        """Carrega dados detectando automaticamente colunas disponiveis e otimizando memoria."""
        base_path = Path(self.cfg['paths']['data']['modeling']) / self.scenario_folder
        self.log.info(f"Carregando dados do cenario: {self.scenario_folder}...")
        
        all_files = sorted(list(base_path.glob("*.parquet")))
        if not all_files:
            raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {base_path}")

        # --- DETECCAO AUTOMATICA DE COLUNAS ---
        try:
            import pyarrow.parquet as pq
            schema = pq.read_schema(all_files[0])
            available_cols = schema.names
            
            valid_features = [f for f in self.features if f in available_cols]
            
            missing_features = set(self.features) - set(valid_features)
            if missing_features:
                self.log.warning(f"[AVISO] As seguintes features NAO estao nesta base e serao ignoradas: {missing_features}")
            
            cols_to_load = valid_features + [self.target, self.year_col]
            
        except Exception as e:
            self.log.error(f"Erro ao inspecionar schema do arquivo: {e}")
            raise e
        # --------------------------------------

        dfs = []
        for fp in all_files:
            try:
                df_year = pd.read_parquet(fp, columns=cols_to_load)
                
                # --- OTIMIZACAO DE MEMORIA ---
                floats = df_year.select_dtypes(include=['float64']).columns
                if len(floats) > 0:
                    df_year[floats] = df_year[floats].astype('float32')
                # -----------------------------

                dfs.append(df_year)
            except Exception as e:
                self.log.warning(f"Erro ao ler {fp.name}: {e}")

        if not dfs:
             raise ValueError("Nenhum dado pode ser carregado.")

        full_df = pd.concat(dfs, ignore_index=True)
        self.log.info(f"Total carregado: {len(full_df)} linhas.")
        return full_df

    def run(self, model_type: str, settings: Dict):
        # 1. Define nome da pasta e checa permissao
        folder_name = self._get_custom_folder_name(model_type, settings)
        if not self._check_overwrite_permission(folder_name):
            return 

        # 2. Carregamento
        df = self.load_full_scenario()
        actual_features = [f for f in self.features if f in df.columns]
        
        initial_rows = len(df)
        df.dropna(subset=actual_features + [self.target], inplace=True)
        if len(df) < initial_rows:
            self.log.warning(f"Dropados {initial_rows - len(df)} registros nulos.")

        # 3. Split Temporal
        splitter = TemporalSplitter(test_size_years=2)
        try:
            train_df, test_df = splitter.split_holdout(df, year_col=self.year_col)
        except ValueError as e:
            self.log.error(f"Erro no split temporal: {e}")
            return

        self.log.info(f"Treino: {len(train_df)} | Teste: {len(test_df)}")
        X_train = train_df[actual_features]
        y_train = train_df[self.target]
        X_test = test_df[actual_features]
        y_test = test_df[self.target]

        # 4. Selecao e Treinamento
        trainer = None
        if model_type == 'logistic':
            trainer = LogisticTrainer(self.scenario_folder)
        elif model_type == 'xgboost':
            trainer = XGBoostTrainer(self.scenario_folder)
        elif 'dummy' in model_type:
            strategy = model_type.split('_')[1]
            trainer = DummyTrainer(self.scenario_folder, strategy=strategy)
        else:
            self.log.error(f"Modelo desconhecido: {model_type}")
            return

        if trainer:
            # Atualiza caminhos de saida com o nome da pasta calculado
            trainer.model_name = folder_name
            trainer.output_dir = (Path(trainer.cfg['paths']['data']['modeling']) / 
                                  "results" / folder_name / trainer.scenario)
            utils.ensure_dir(trainer.output_dir)
            
            self.log.info(f"Salvando resultados em: {trainer.output_dir}")

            # Treina passando o dicionario de settings expandido
            # O XGBoost vai ler use_smote/use_scale. Logistic/Dummy vao ignorar os extras via **kwargs
            trainer.train(X_train, y_train, **settings)
            
            metrics = trainer.evaluate(X_test, y_test)
            trainer.save_artifacts(metrics)

# --- CLI ---
def print_menu(options: Dict[int, str], title: str):
    print(f"\n--- {title} ---")
    for key, value in options.items():
        print(f"[{key}] {value}")

def get_user_choice(options: Dict[int, str]) -> int:
    while True:
        try:
            choice = int(input(">> Selecione uma opcao: "))
            if choice in options:
                return choice
            print("Opcao invalida.")
        except ValueError:
            print("Por favor, digite um numero.")

def main():
    cfg = utils.loadConfig()
    scenarios = cfg.get('modeling_scenarios', {})
    if not scenarios:
        print("Erro: Nenhum cenario no config.yaml")
        return
    
    # 1. Menu Cenarios
    s_map = {i+1: k for i, k in enumerate(sorted(scenarios.keys()))}
    print_menu({k: f"{v} ({scenarios[v]})" for k, v in s_map.items()}, "Escolha a Base")
    s_choice = get_user_choice(s_map)
    selected_scenario = s_map[s_choice]

    # 2. Menu Modelos
    model_map = {1: "dummy_stratified", 2: "dummy_prior", 3: "logistic", 4: "xgboost"}
    print("\n--- Escolha o Modelo ---")
    print("[1] Dummy (Estratificado)")
    print("[2] Dummy (Prior)")
    print("[3] Regressao Logistica")
    print("[4] XGBoost (Gradient Boosting)")
    
    m_choice = get_user_choice(model_map)
    selected_model = model_map[m_choice]

    # 3. Configuracao Especifica (Settings)
    settings = {'optimize': False} # Default
    
    if selected_model == 'xgboost':
        print("\n--- Configuracao XGBoost ---")
        print("[1] Rapido (Base + Scale Weight) - Sem GridSearch")
        print("[2] Turbo Completo (SMOTE + Scale Weight) - Double Penalty 'Monster'")
        print("[3] So SMOTE (GridSearch + SMOTE) - Peso=1.0")
        print("[4] So Scale (GridSearch + Scale Weight) - Sem SMOTE")
        
        try:
            xg_opt = input(">> Opcao [1-4]: ").strip()
        except:
            return
        
        if xg_opt == '1':
            # Rápido: Sem otimização, sem smote, com peso
            settings = {'optimize': False, 'use_smote': False, 'use_scale': True}
        elif xg_opt == '2':
            # Monster: Otimiza, com smote, com peso
            settings = {'optimize': True, 'use_smote': True, 'use_scale': True}
        elif xg_opt == '3':
            # Smote Puro: Otimiza, com smote, SEM peso (peso=1)
            settings = {'optimize': True, 'use_smote': True, 'use_scale': False}
        elif xg_opt == '4':
            # Scale Puro: Otimiza, SEM smote, com peso
            settings = {'optimize': True, 'use_smote': False, 'use_scale': True}
        else:
            print("Opcao invalida, usando Rapido.")
            settings = {'optimize': False, 'use_smote': False, 'use_scale': True}
            
    elif 'dummy' not in selected_model:
        # Para Logística, mantem pergunta simples
        print("\n--- Modo de Execucao ---")
        print("[0] Rapido")
        print("[1] Turbo (GridSearch)")
        if input(">> Opcao [0/1]: ").strip() == '1':
            settings['optimize'] = True

    print(f"\n[INICIANDO] {selected_scenario} | Config: {settings}")
    
    try:
        runner = TrainingOrchestrator(scenario_key=selected_scenario)
        runner.run(model_type=selected_model, settings=settings)
    except Exception as e:
        print(f"[ERRO FATAL] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()