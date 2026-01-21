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

    def _get_model_folder_name(self, model_type: str, optimize: bool = False) -> str:
        """Define o nome da pasta de resultados baseada no modelo e modo de execucao."""
        base_name = "Unknown"
        
        if model_type == 'logistic':
            base_name = "LogisticRegression"
        elif model_type == 'xgboost':
            base_name = "XGBoost"
        elif model_type == 'dummy_stratified':
            return "DummyClassifier_stratified" # Dummies ignoram otimizacao
        elif model_type == 'dummy_prior':
            return "DummyClassifier_prior"
        
        # Se estiver no modo Turbo, adiciona sufixo para nao misturar arquivos
        if optimize:
            base_name += "_SMOTE_GridSearch"
            
        return base_name

    def _check_overwrite_permission(self, model_type: str, optimize: bool) -> bool:
        """Verifica se existem resultados anteriores na pasta especifica."""
        # Pega o nome correto (com ou sem sufixo _SMOTE_GridSearch)
        model_folder = self._get_model_folder_name(model_type, optimize)
        
        results_dir = (Path(self.cfg['paths']['data']['modeling']) / 
                       "results" / 
                       model_folder / 
                       self.scenario_folder)
        
        if results_dir.exists() and any(results_dir.iterdir()):
            print(f"\n{'!'*60}")
            print(f"[ALERTA DE SOBRESCRITA]")
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
            # Le apenas o schema do primeiro arquivo para validar colunas
            schema = pq.read_schema(all_files[0])
            available_cols = schema.names
            
            # Filtra: So carrega features que existem no arquivo
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
                
                # --- OTIMIZACAO DE MEMORIA (CRITICO PARA MODO TURBO) ---
                # Converte float64 (8 bytes) para float32 (4 bytes)
                # Reduz o consumo de RAM pela metade, evitando crash no SMOTE
                floats = df_year.select_dtypes(include=['float64']).columns
                if len(floats) > 0:
                    df_year[floats] = df_year[floats].astype('float32')
                # -------------------------------------------------------

                dfs.append(df_year)
            except Exception as e:
                self.log.warning(f"Erro ao ler {fp.name}: {e}")

        if not dfs:
             raise ValueError("Nenhum dado pode ser carregado. Verifique os arquivos parquet.")

        full_df = pd.concat(dfs, ignore_index=True)
        self.log.info(f"Total carregado: {len(full_df)} linhas.")
        return full_df

    def run(self, model_type: str, optimize: bool = False):
        # Verifica permissao na pasta correta (com ou sem sufixo)
        if not self._check_overwrite_permission(model_type, optimize):
            return 

        # 1. Carregamento
        df = self.load_full_scenario()
        
        # --- FILTRAGEM DE COLUNAS REAIS ---
        actual_features = [f for f in self.features if f in df.columns]
        
        # 2. Pre-processamento Rapido
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
        elif model_type == 'dummy_stratified':
            trainer = DummyTrainer(self.scenario_folder, strategy='stratified')
        elif model_type == 'dummy_prior':
            trainer = DummyTrainer(self.scenario_folder, strategy='prior')
        else:
            self.log.error(f"Modelo desconhecido: {model_type}")
            return

        if trainer:
            # --- AJUSTE DE PASTA PARA MODO TURBO ---
            if optimize and not model_type.startswith('dummy'):
                # Altera o nome do modelo e o diretorio de saida dinamicamente
                new_model_name = self._get_model_folder_name(model_type, optimize=True)
                
                trainer.model_name = new_model_name
                
                # Recalcula path de saida no objeto trainer
                trainer.output_dir = (Path(trainer.cfg['paths']['data']['modeling']) / 
                                      "results" / 
                                      trainer.model_name / 
                                      trainer.scenario)
                utils.ensure_dir(trainer.output_dir)
                self.log.info(f"Modo Otimizado Ativo: Resultados serao salvos em {trainer.output_dir}")

            # Treina
            trainer.train(X_train, y_train, optimize=optimize)
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
    
    # Menu Cenarios
    scenarios = cfg.get('modeling_scenarios', {})
    if not scenarios:
        print("Erro: Nenhum cenario no config.yaml")
        return
    
    scenario_map = {i+1: k for i, k in enumerate(sorted(scenarios.keys()))}
    print_menu({k: f"{v} ({scenarios[v]})" for k, v in scenario_map.items()}, "Escolha a Base")
    s_choice = get_user_choice(scenario_map)
    selected_scenario = scenario_map[s_choice]

    # Menu Modelos
    model_map = {
        1: "dummy_stratified",
        2: "dummy_prior",
        3: "logistic",
        4: "xgboost"
    }
    model_labels = {
        1: "Dummy (Estratificado) - Baseline Aleatorio",
        2: "Dummy (Prior) - Baseline Conservador",
        3: "Regressao Logistica",
        4: "XGBoost (Gradient Boosting)"
    }
    
    print_menu(model_labels, "Escolha o Modelo")
    m_choice = get_user_choice(model_labels)
    selected_model = model_map[m_choice]

    # Menu Otimizacao (Turbo)
    optimize_choice = False
    if "dummy" not in selected_model:
        print("\n--- Modo de Execucao ---")
        print("[0] Rapido (Parametros Padrao)")
        print("[1] Turbo (GridSearch + SMOTE - Demorado!)")
        opt_input = input(">> Selecione [0/1] (Default 0): ").strip()
        if opt_input == '1':
            optimize_choice = True
            print(">> [MODO TURBO ATIVADO] Resultados serao salvos com sufixo _SMOTE_GridSearch")
        else:
            print(">> Modo Rapido selecionado.")

    print(f"\n[INICIANDO] {selected_scenario} + {selected_model}")
    
    try:
        runner = TrainingOrchestrator(scenario_key=selected_scenario)
        runner.run(model_type=selected_model, optimize=optimize_choice)
    except Exception as e:
        print(f"[ERRO FATAL] {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()