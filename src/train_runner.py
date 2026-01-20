# src/train_runner.py
# =============================================================================
# RUNNER DE TREINAMENTO - TCC WILDFIRE PREDICTION
# =============================================================================
# Orquestra o carregamento de dados, split temporal e treinamento de modelos.
# CLI Interativa com Trava de Segurança contra Sobrescrita.
# =============================================================================

import pandas as pd
import sys
import shutil
from pathlib import Path
from typing import Dict, Optional

# --- CORREÇÃO DE PATH ---
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
except ImportError as e:
    print(f"Erro crítico de importação: {e}")
    sys.exit(1)

class TrainingOrchestrator:
    def __init__(self, scenario_key: str):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)
        
        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg['modeling_scenarios'].get(scenario_key)
        
        if not self.scenario_folder:
            raise ValueError(f"Cenário '{scenario_key}' não definido no config.yaml")

        # Variáveis preditoras (Features)
        self.features = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
            'RADIACAO GLOBAL (KJ/m²)',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)',
            'UMIDADE RELATIVA DO AR, HORARIA (%)',
            'VENTO, VELOCIDADE HORARIA (m/s)'
        ]
        self.target = 'HAS_FOCO'
        self.year_col = 'ANO'

    def _get_model_folder_name(self, model_type: str) -> str:
        """
        Mapeia a escolha do CLI para o nome real da pasta criada pelo Trainer.
        Isso permite verificar existência antes de instanciar a classe.
        """
        if model_type == 'logistic':
            return "LogisticRegression"
        elif model_type == 'dummy_stratified':
            return "DummyClassifier_stratified"
        elif model_type == 'dummy_prior':
            return "DummyClassifier_prior"
        else:
            return "Unknown"

    def _check_overwrite_permission(self, model_type: str) -> bool:
        """
        Verifica se já existem resultados e pede confirmação do usuário.
        Retorna True se pode prosseguir, False se deve abortar.
        """
        model_folder = self._get_model_folder_name(model_type)
        
        # Caminho: data/modeling/results/{Modelo}/{Cenario}
        results_dir = (Path(self.cfg['paths']['data']['modeling']) / 
                       "results" / 
                       model_folder / 
                       self.scenario_folder)
        
        # Se a pasta existe e não está vazia
        if results_dir.exists() and any(results_dir.iterdir()):
            print(f"\n{'!'*60}")
            print(f"⚠  ALERTA DE SOBRESCRITA  ⚠")
            print(f"Já existem resultados salvos em:\n-> {results_dir}")
            print(f"{'!'*60}")
            
            while True:
                response = input(">> Tem certeza que deseja rodar novamente e SOBRESCREVER? [y/N]: ").strip().lower()
                if response in ['n', 'no', '']:
                    print(">> Operação cancelada pelo usuário.")
                    return False
                elif response in ['y', 'yes']:
                    print(">> Sobrescrevendo dados antigos...")
                    # Opcional: Limpar pasta antes (os Trainers já sobrescrevem arquivos, 
                    # mas limpar garante que não sobram arquivos velhos de timestamps diferentes)
                    # shutil.rmtree(results_dir) 
                    # utils.ensure_dir(results_dir)
                    return True
        
        return True

    def load_full_scenario(self) -> pd.DataFrame:
        """Carrega e concatena todos os anos do cenário para memória."""
        base_path = Path(self.cfg['paths']['data']['modeling']) / self.scenario_folder
        self.log.info(f"Carregando dados do cenário: {self.scenario_folder}...")
        
        all_files = sorted(list(base_path.glob("*.parquet")))
        if not all_files:
            raise FileNotFoundError(f"Nenhum arquivo .parquet encontrado em {base_path}")

        dfs = []
        for fp in all_files:
            try:
                cols_to_load = self.features + [self.target, self.year_col]
                df_year = pd.read_parquet(fp, columns=cols_to_load)
                dfs.append(df_year)
            except Exception as e:
                self.log.warning(f"Erro ao ler {fp.name}: {e}")

        full_df = pd.concat(dfs, ignore_index=True)
        self.log.info(f"Total carregado: {len(full_df)} linhas.")
        return full_df

    def run(self, model_type: str):
        # --- PASSO 0: TRAVA DE SEGURANÇA ---
        # Verifica antes de carregar dados pesados
        if not self._check_overwrite_permission(model_type):
            return  # Sai da função se o usuário negar

        # 1. Carregamento
        df = self.load_full_scenario()
        
        # 2. Pré-processamento Rápido
        initial_rows = len(df)
        df.dropna(subset=self.features + [self.target], inplace=True)
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

        X_train = train_df[self.features]
        y_train = train_df[self.target]
        X_test = test_df[self.features]
        y_test = test_df[self.target]

        # 4. Seleção e Treinamento
        trainer = None
        if model_type == 'logistic':
            trainer = LogisticTrainer(self.scenario_folder, C=1.0, max_iter=2000)
        elif model_type == 'dummy_stratified':
            trainer = DummyTrainer(self.scenario_folder, strategy='stratified')
        elif model_type == 'dummy_prior':
            trainer = DummyTrainer(self.scenario_folder, strategy='prior')
        else:
            self.log.error(f"Modelo desconhecido: {model_type}")
            return

        if trainer:
            trainer.train(X_train, y_train)
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
            choice = int(input(">> Selecione uma opção: "))
            if choice in options:
                return choice
            print("Opção inválida.")
        except ValueError:
            print("Por favor, digite um número.")

def main():
    cfg = utils.loadConfig()
    
    # Menu Cenários
    scenarios = cfg.get('modeling_scenarios', {})
    if not scenarios:
        print("Erro: Nenhum cenário no config.yaml")
        return
    
    # Cria mapa reverso ordenado (1: base_A, 2: base_B...)
    scenario_map = {i+1: k for i, k in enumerate(sorted(scenarios.keys()))}
    
    print_menu({k: f"{v} ({scenarios[v]})" for k, v in scenario_map.items()}, "Escolha a Base")
    s_choice = get_user_choice(scenario_map)
    selected_scenario = scenario_map[s_choice]

    # Menu Modelos
    model_map = {
        1: "dummy_stratified",
        2: "dummy_prior",
        3: "logistic"
    }
    model_labels = {
        1: "Dummy (Estratificado) - Baseline Aleatório",
        2: "Dummy (Prior) - Baseline Conservador",
        3: "Regressão Logística"
    }
    
    print_menu(model_labels, "Escolha o Modelo")
    m_choice = get_user_choice(model_labels)
    selected_model = model_map[m_choice]

    print(f"\n Iniciando: {selected_scenario} + {selected_model}")
    
    try:
        runner = TrainingOrchestrator(scenario_key=selected_scenario)
        runner.run(model_type=selected_model)
    except Exception as e:
        print(f" Erro: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()