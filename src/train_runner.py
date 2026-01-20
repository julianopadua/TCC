# src/train_runner.py
# =============================================================================
# RUNNER DE TREINAMENTO - TCC WILDFIRE PREDICTION
# =============================================================================
# Orquestra o carregamento de dados, split temporal e treinamento de modelos.
# CLI Interativa para seleção de Bases e Algoritmos.
# =============================================================================

import pandas as pd
import sys
import os
from pathlib import Path
from typing import List, Dict, Type

# --- CORREÇÃO DE PATH ---
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))
# ------------------------

# Importa módulos internos
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
        # Resolve o nome da pasta real (ex: "base_E_with_rad_knn")
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
                # Lê apenas as colunas necessárias para economizar memória
                cols_to_load = self.features + [self.target, self.year_col]
                df_year = pd.read_parquet(fp, columns=cols_to_load)
                dfs.append(df_year)
            except Exception as e:
                self.log.warning(f"Erro ao ler {fp.name}: {e}")

        full_df = pd.concat(dfs, ignore_index=True)
        self.log.info(f"Total carregado: {len(full_df)} linhas.")
        return full_df

    def run(self, model_type: str):
        # 1. Carregamento
        df = self.load_full_scenario()
        
        # 2. Pré-processamento Rápido (Sanity Check)
        initial_rows = len(df)
        df.dropna(subset=self.features + [self.target], inplace=True)
        dropped = initial_rows - len(df)
        if dropped > 0:
            self.log.warning(f"Dropados {dropped} registros com valores nulos para o treino.")

        # 3. Split Temporal (Treino vs Teste)
        splitter = TemporalSplitter(test_size_years=2)
        try:
            train_df, test_df = splitter.split_holdout(df, year_col=self.year_col)
        except ValueError as e:
            self.log.error(f"Erro no split temporal: {e}")
            return

        self.log.info(f"Split Temporal Realizado:")
        self.log.info(f"  > Treino: {train_df[self.year_col].min()} a {train_df[self.year_col].max()} ({len(train_df)} linhas)")
        self.log.info(f"  > Teste : {test_df[self.year_col].min()} a {test_df[self.year_col].max()} ({len(test_df)} linhas)")

        X_train = train_df[self.features]
        y_train = train_df[self.target]
        X_test = test_df[self.features]
        y_test = test_df[self.target]

        # 4. Seleção e Treinamento do Modelo
        trainer = None
        
        if model_type == 'logistic':
            trainer = LogisticTrainer(
                scenario_name=self.scenario_folder,
                C=1.0,
                max_iter=2000
            )
        elif model_type == 'dummy_stratified':
            trainer = DummyTrainer(
                scenario_name=self.scenario_folder,
                strategy='stratified' # Respeita a distribuição (chuta 0.4% de fogo)
            )
        elif model_type == 'dummy_prior':
            trainer = DummyTrainer(
                scenario_name=self.scenario_folder,
                strategy='prior' # Sempre chuta a classe majoritária (Zero Fogo) - Bom para Brier Score
            )
        else:
            self.log.error(f"Modelo desconhecido: {model_type}")
            return

        if trainer:
            trainer.train(X_train, y_train)
            
            # 5. Avaliação e Salvamento
            metrics = trainer.evaluate(X_test, y_test)
            trainer.save_artifacts(metrics)

# --- FUNÇÕES DE INTERFACE (CLI) ---

def print_menu(options: Dict[int, str], title: str):
    print(f"\n--- {title} ---")
    for key, value in options.items():
        print(f"[{key}] {value}")

def get_user_choice(options: Dict[int, str]) -> str:
    while True:
        try:
            choice = int(input(">> Selecione uma opção: "))
            if choice in options:
                return options[choice]
            print("Opção inválida.")
        except ValueError:
            print("Por favor, digite um número.")

def main():
    # Carrega config para ler os cenários disponíveis
    cfg = utils.loadConfig()
    
    # 1. Menu de Cenários (Bases)
    scenarios = cfg.get('modeling_scenarios', {})
    if not scenarios:
        print("Nenhum cenário encontrado no config.yaml.")
        return

    scenario_options = {i+1: key for i, key in enumerate(scenarios.keys())}
    print_menu(scenario_options, "Escolha a Base de Dados (Cenário)")
    selected_scenario_key = get_user_choice(scenario_options)
    
    # 2. Menu de Modelos
    model_options = {
        1: "dummy_stratified", # Chute aleatório ponderado (Baseline de Acurácia/PR)
        2: "dummy_prior",      # Chute 'Sempre Zero' (Baseline de Brier Score)
        3: "logistic"          # Regressão Logística
    }
    
    # Labels amigáveis para exibir no menu
    model_labels = {
        1: "Dummy (Estratificado) - Baseline Aleatório",
        2: "Dummy (Prior/Most Frequent) - Baseline Conservador",
        3: "Regressão Logística (Baseline ML)"
    }
    
    print_menu(model_labels, "Escolha o Modelo para Treinar")
    # Mapeia a escolha numérica de volta para a string interna (ex: 3 -> 'logistic')
    user_model_choice_idx = int(input(">> Selecione uma opção: ")) # Simplificado, ideal usar validação aqui tbm
    selected_model_type = model_options.get(user_model_choice_idx, "logistic")

    print(f"\n\n{'='*60}")
    print(f"INICIANDO EXECUÇÃO")
    print(f"Cenário: {selected_scenario_key}")
    print(f"Modelo : {selected_model_type}")
    print(f"{'='*60}\n")

    try:
        runner = TrainingOrchestrator(scenario_key=selected_scenario_key)
        runner.run(model_type=selected_model_type)
    except Exception as e:
        print(f"❌ Erro na execução: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()