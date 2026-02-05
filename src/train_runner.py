# src/train_runner.py
# =============================================================================
# RUNNER DE TREINAMENTO (BATCH MODE) - TCC WILDFIRE PREDICTION
# =============================================================================
# Permite rodar múltiplas bases e múltiplos modelos em sequência.
# Gerencia memória limpando DataFrames entre iterações.
# =============================================================================

import pandas as pd
import sys
import gc  # Garbage Collector para limpar RAM entre bases
from pathlib import Path
from typing import Dict, List, Any, Tuple

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
        # O log agora é append=True para não sobrescrever se rodar várias vezes no mesmo dia
        self.log = utils.get_logger("runner.train", kind="train", per_run_file=True)
        
        self.scenario_key = scenario_key
        self.scenario_folder = self.cfg['modeling_scenarios'].get(scenario_key)
        
        if not self.scenario_folder:
            raise ValueError(f"Cenario '{scenario_key}' nao definido no config.yaml")

        # Lista MESTRA de features (Conforme metadados do INMET + Features Calculadas)
        self.features = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
            'RADIACAO GLOBAL (KJ/m²)',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)',
            'UMIDADE RELATIVA DO AR, HORARIA (%)',
            'VENTO, VELOCIDADE HORARIA (m/s)',
            'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)',
            'VENTO, RAJADA MAXIMA (m/s)',
            # Features Calculadas (Physics)
            'precip_ewma', 
            'dias_sem_chuva', 
            'risco_temp_max', 
            'risco_umid_critica', 
            'risco_umid_alerta', 
            'fator_propagacao'
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
                return f"{base_name}_Fast_Base"
            
            suffixes = []
            if smote: suffixes.append("SMOTE")
            if scale: suffixes.append("Scale")
            if not suffixes: suffixes.append("GridSearchOnly")
            
            return f"{base_name}_{'_'.join(suffixes)}"
            
        return base_name

    def _check_overwrite_permission(self, folder_name: str, auto_approve: bool = False) -> bool:
        """
        Verifica se existem resultados.
        auto_approve: Se True, sobrescreve sem perguntar (útil para batch grande).
        """
        if auto_approve:
            return True

        results_dir = (Path(self.cfg['paths']['data']['modeling']) / 
                       "results" / 
                       folder_name / 
                       self.scenario_folder)
        
        if results_dir.exists() and any(results_dir.iterdir()):
            print(f"\n[ALERTA] Resultados já existem em: {folder_name}/{self.scenario_folder}")
            # Em modo batch, talvez queiramos pular ou sobrescrever tudo. 
            # Por segurança, perguntamos.
            while True:
                response = input(">> Sobrescrever? [y/N/all]: ").strip().lower()
                if response in ['n', 'no', '']:
                    return False
                elif response in ['y', 'yes']:
                    return True
                elif response == 'all':
                    return 'ALL' # Código especial para aprovar todos os próximos
        return True

    def load_full_scenario(self) -> pd.DataFrame:
        """Carrega dados com otimização de memória."""
        base_path = Path(self.cfg['paths']['data']['modeling']) / self.scenario_folder
        self.log.info(f"Carregando cenario: {self.scenario_folder}...")
        
        all_files = sorted(list(base_path.glob("*.parquet")))
        if not all_files:
            raise FileNotFoundError(f"Nenhum arquivo .parquet em {base_path}")

        # Detecção de colunas via Schema do primeiro arquivo
        try:
            import pyarrow.parquet as pq
            schema = pq.read_schema(all_files[0])
            available_cols = schema.names
            
            valid_features = [f for f in self.features if f in available_cols]
            cols_to_load = valid_features + [self.target, self.year_col]
        except Exception as e:
            self.log.error(f"Erro lendo schema: {e}")
            raise e

        dfs = []
        for fp in all_files:
            try:
                # Carrega apenas colunas necessárias
                df_year = pd.read_parquet(fp, columns=cols_to_load)
                
                # Downcast de float64 para float32
                floats = df_year.select_dtypes(include=['float64']).columns
                if len(floats) > 0:
                    df_year[floats] = df_year[floats].astype('float32')
                
                dfs.append(df_year)
            except Exception as e:
                self.log.warning(f"Erro ao ler {fp.name}: {e}")

        if not dfs: raise ValueError("Nenhum dado carregado.")
        full_df = pd.concat(dfs, ignore_index=True)
        return full_df

    def run_cycle(self, model_configs: List[Dict], auto_approve_overwrite: bool = False) -> bool:
        """
        Executa um ciclo completo de carregamento da base + execução de N modelos.
        Retorna flag se o 'auto_approve' foi ativado pelo usuário.
        """
        # 1. Carrega a base UMA VEZ para todos os modelos desta rodada
        try:
            df = self.load_full_scenario()
        except Exception as e:
            self.log.error(f"Falha ao carregar base {self.scenario_key}: {e}")
            return auto_approve_overwrite

        # Limpeza inicial de nulos
        actual_features = [f for f in self.features if f in df.columns]
        initial_rows = len(df)
        df.dropna(subset=actual_features + [self.target], inplace=True)
        
        if len(df) < initial_rows:
            self.log.info(f"Removidos {initial_rows - len(df)} nulos pré-split.")

        # 2. Split Temporal (Holdout)
        splitter = TemporalSplitter(test_size_years=2)
        try:
            train_df, test_df = splitter.split_holdout(df, year_col=self.year_col)
        except ValueError as e:
            self.log.error(f"Erro no split temporal: {e}")
            return auto_approve_overwrite

        self.log.info(f"Dados prontos. Treino: {len(train_df)} | Teste: {len(test_df)}")
        
        X_train = train_df[actual_features]
        y_train = train_df[self.target]
        X_test = test_df[actual_features]
        y_test = test_df[self.target]

        # Limpa o dfzão da memória, já temos X_train/y_train
        del df, train_df, test_df
        gc.collect()

        # 3. Itera sobre os modelos configurados
        for config in model_configs:
            model_type = config['type']
            settings = config['settings']
            
            folder_name = self._get_custom_folder_name(model_type, settings)
            print(f"\n    >> [MODELO] {folder_name} em {self.scenario_key}")

            # Checa permissão
            perm = self._check_overwrite_permission(folder_name, auto_approve=auto_approve_overwrite)
            if perm == 'ALL':
                auto_approve_overwrite = True
                perm = True
            
            if not perm:
                print("    >> Pulado pelo usuário.")
                continue

            # Instancia e Treina
            trainer = None
            if model_type == 'logistic':
                trainer = LogisticTrainer(self.scenario_folder)
            elif model_type == 'xgboost':
                trainer = XGBoostTrainer(self.scenario_folder)
            elif 'dummy' in model_type:
                strategy = model_type.split('_')[1]
                trainer = DummyTrainer(self.scenario_folder, strategy=strategy)
            
            if trainer:
                # Setup Paths
                trainer.model_name = folder_name
                trainer.output_dir = (Path(trainer.cfg['paths']['data']['modeling']) / 
                                      "results" / folder_name / trainer.scenario)
                utils.ensure_dir(trainer.output_dir)
                
                # Executa
                try:
                    trainer.train(X_train, y_train, **settings)
                    metrics = trainer.evaluate(X_test, y_test)
                    trainer.save_artifacts(metrics)
                    print(f"    >> Sucesso! PR-AUC: {metrics.get('pr_auc', 0):.4f}")
                except Exception as e:
                    self.log.error(f"Erro treinando {model_type}: {e}")
                    import traceback
                    traceback.print_exc()

        # Limpa dados de treino/teste desta base
        del X_train, y_train, X_test, y_test
        gc.collect()
        
        return auto_approve_overwrite

# --- CLI UTILS ---
def get_multi_choice(options: Dict[int, str], prompt: str) -> List[str]:
    print(f"\n--- {prompt} ---")
    for key, val in options.items():
        print(f"[{key}] {val}")
    
    while True:
        raw = input(">> Selecione (ex: 1,3 ou 'all'): ").strip().lower()
        if raw == 'all':
            return list(options.values())
        
        try:
            # Separa por vírgula e remove espaços
            choices = [int(x) for x in raw.split(',') if x.strip()]
            valid_choices = [options[c] for c in choices if c in options]
            
            if not valid_choices:
                print("Nenhuma opção válida selecionada.")
                continue
            
            return valid_choices
        except ValueError:
            print("Entrada inválida. Use números separados por vírgula.")

def configure_model_settings(model_key: str) -> Dict:
    """Retorna o dicionário de settings para um tipo de modelo (perguntado uma vez)."""
    settings = {'optimize': False}
    
    if model_key == 'xgboost':
        print(f"\n[CONFIG] XGBoost detectado na fila.")
        print("[1] Rápido (Base + Peso)")
        print("[2] Monster (GridSearch + SMOTE + Peso)")
        print("[3] SMOTE Puro (GridSearch + SMOTE)")
        print("[4] Scale Puro (GridSearch + Peso)")
        opt = input(">> Escolha configuração para TODAS as bases [1]: ").strip()
        
        if opt == '2': return {'optimize': True, 'use_smote': True, 'use_scale': True}
        if opt == '3': return {'optimize': True, 'use_smote': True, 'use_scale': False}
        if opt == '4': return {'optimize': True, 'use_smote': False, 'use_scale': True}
        return {'optimize': False, 'use_smote': False, 'use_scale': True} # Default

    elif model_key == 'logistic':
        print(f"\n[CONFIG] Regressão Logística detectada.")
        opt = input(">> Ativar Otimização (GridSearch)? [y/N]: ").strip().lower()
        if opt in ['y', 'yes']: settings['optimize'] = True
    
    return settings

def main():
    cfg = utils.loadConfig()
    scenarios_map = cfg.get('modeling_scenarios', {})
    
    if not scenarios_map:
        print("Nenhum cenário no config.yaml!")
        return

    # 1. Seleção de Bases (Múltipla)
    # Mapeia 1..N -> Keys do YAML
    s_keys = sorted(scenarios_map.keys())
    s_options = {i+1: k for i, k in enumerate(s_keys)}
    
    selected_scenarios = get_multi_choice(s_options, "Escolha as Bases para Rodar")
    print(f">> Bases selecionadas: {selected_scenarios}")

    # 2. Seleção de Modelos (Múltipla)
    m_options = {
        1: "dummy_stratified",
        2: "dummy_prior",
        3: "logistic",
        4: "xgboost"
    }
    selected_models_types = get_multi_choice(m_options, "Escolha os Modelos")
    print(f">> Modelos selecionados: {selected_models_types}")

    # 3. Configuração dos Modelos (Uma vez por tipo)
    # Ex: Se escolheu xgboost, configura ele agora.
    execution_plan = [] # Lista de {type: str, settings: dict}
    
    # Agrupa por 'familia' para não perguntar settings de dummy
    # (Dummy não tem config extra, logistic tem, xgboost tem)
    unique_types = set(selected_models_types)
    
    type_settings_map = {}
    for m_type in unique_types:
        # Se for dummy, settings vazio
        if 'dummy' in m_type:
            type_settings_map[m_type] = {}
        else:
            # Pergunta configuração
            type_settings_map[m_type] = configure_model_settings(m_type)
    
    # Monta o plano ordenado pela seleção do usuário? 
    # Melhor: o usuário selecionou tipos. Vamos criar a lista final.
    for m_type in selected_models_types:
        execution_plan.append({
            'type': m_type,
            'settings': type_settings_map[m_type]
        })

    # 4. EXECUÇÃO EM LOTE
    print(f"\n{'='*60}")
    print(f"INICIANDO EXECUÇÃO EM LOTE")
    print(f"Bases: {len(selected_scenarios)} | Modelos por Base: {len(execution_plan)}")
    print(f"{'='*60}")

    global_auto_approve = False

    for idx, scenario in enumerate(selected_scenarios):
        print(f"\n>>> [BASE {idx+1}/{len(selected_scenarios)}] Processando: {scenario}")
        
        try:
            orchestrator = TrainingOrchestrator(scenario)
            # Roda todos os modelos para essa base
            global_auto_approve = orchestrator.run_cycle(execution_plan, global_auto_approve)
        except Exception as e:
            print(f"[ERRO] Falha crítica na base {scenario}: {e}")
        
        # Coleta forçada de lixo ao mudar de base
        gc.collect()

    print(f"\n{'='*60}")
    print("BATCH FINALIZADO.")

if __name__ == "__main__":
    main()