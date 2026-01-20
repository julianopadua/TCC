# src/train_runner.py
# =============================================================================
# RUNNER DE TREINAMENTO - TCC WILDFIRE PREDICTION
# =============================================================================
# Orquestra o carregamento de dados, split temporal e treinamento de modelos.
# Exemplo configurado para: Regressão Logística.
# =============================================================================

import pandas as pd
import sys
from pathlib import Path
from typing import List

current_file = Path(__file__).resolve()
project_root = current_file.parent.parent

if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# Importa módulos internos
try:
    import utils
    from src.ml.core import TemporalSplitter
    from src.models.logistic import LogisticTrainer
except ImportError:
    # Fallback para execução direta da pasta src/
    import src.utils as utils
    from src.ml.core import TemporalSplitter
    from src.models.logistic import LogisticTrainer

class TrainingOrchestrator:
    def __init__(self, scenario_key: str = "base_E"):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)
        
        self.scenario_key = scenario_key
        # Resolve o nome da pasta real (ex: "base_E_with_rad_knn")
        self.scenario_folder = self.cfg['modeling_scenarios'].get(scenario_key)
        
        if not self.scenario_folder:
            raise ValueError(f"Cenário '{scenario_key}' não definido no config.yaml")

        # Variáveis preditoras (Features)
        # Definidas com base na literatura do seu TCC
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
        """
        Carrega e concatena todos os anos do cenário para memória.
        NOTA: Para 300KB de dados, isso é seguro. Para Big Data, usaríamos Dask ou batching.
        """
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

    def run(self):
        # 1. Carregamento
        df = self.load_full_scenario()
        
        # 2. Pré-processamento Rápido (Sanity Check)
        # Regressão Logística não aceita NaNs. 
        # Se o cenário for 'base_F' (original), precisamos dropar nulos aqui para não quebrar.
        initial_rows = len(df)
        df.dropna(subset=self.features + [self.target], inplace=True)
        dropped = initial_rows - len(df)
        if dropped > 0:
            self.log.warning(f"Dropados {dropped} registros com valores nulos para o treino.")

        # 3. Split Temporal (Treino vs Teste)
        # Reservamos os últimos 2 anos para teste (ex: 2023, 2024)
        splitter = TemporalSplitter(test_size_years=2)
        train_df, test_df = splitter.split_holdout(df, year_col=self.year_col)
        
        self.log.info(f"Split Temporal Realizado:")
        self.log.info(f"  > Treino: {train_df[self.year_col].min()} a {train_df[self.year_col].max()} ({len(train_df)} linhas)")
        self.log.info(f"  > Teste : {test_df[self.year_col].min()} a {test_df[self.year_col].max()} ({len(test_df)} linhas)")

        # Separa X e y
        X_train = train_df[self.features]
        y_train = train_df[self.target]
        X_test = test_df[self.features]
        y_test = test_df[self.target]

        # 4. Treinamento (Regressão Logística)
        trainer = LogisticTrainer(
            scenario_name=self.scenario_folder,
            C=1.0,      # Regularização padrão
            max_iter=2000 # Margem de segurança para convergência
        )
        
        trainer.train(X_train, y_train)

        # 5. Avaliação e Salvamento
        metrics = trainer.evaluate(X_test, y_test)
        trainer.save_artifacts(metrics)

if __name__ == "__main__":
    # Teste inicial com a Base E (que já tem tratamento KNN e Radiação)
    # Alterar para 'base_F' se quiser testar a original (com dropna automático)
    try:
        runner = TrainingOrchestrator(scenario_key="base_E") 
        runner.run()
    except Exception as e:
        print(f"Erro fatal na execução: {e}")