# src/feature_engineering_physics.py
# =============================================================================
# ENGENHARIA DE FEATURES (PHYSICS-INFORMED)
# Baseado no documento: RiscoFogo_Sucinto_v11_2019.pdf (INPE)
# =============================================================================
# Estratégia:
# 1. Usa a Base E (KNN) como "Mestra" para calcular histórico (sem buracos).
# 2. Mantém estado de memória entre anos (para 01/Jan não zerar a contagem de seca).
# 3. Distribui as novas features para todas as bases (A..F) via Merge.
# =============================================================================

import sys
import gc
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm

# Boilerplate de Path
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

try:
    import src.utils as utils
except ImportError:
    print("[ERRO] Falha ao importar src.utils")
    sys.exit(1)

# Configuração de Colunas (Mapeamento INMET)
COL_PRECIP = "PRECIPITAÇÃO TOTAL, HORÁRIO (mm)"
COL_TEMP   = "TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)"
COL_UMID   = "UMIDADE RELATIVA DO AR, HORARIA (%)"
COL_VENTO  = "VENTO, VELOCIDADE HORARIA (m/s)"

# Lista de Cenários para processar
SCENARIOS_TO_PROCESS = ["base_A", "base_B", "base_C", "base_D", "base_E", "base_F"]
# Mapeamento para nomes de pasta reais (confira seu config.yaml ou estrutura)
SCENARIO_FOLDERS = {
    "base_A": "base_A_no_rad",
    "base_B": "base_B_no_rad_knn",
    "base_C": "base_C_no_rad_drop_rows",
    "base_D": "base_D_with_rad_drop_rows",
    "base_E": "base_E_with_rad_knn",
    "base_F": "base_F_full_original"
}

class PhysicsFeatureEngineer:
    def __init__(self):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("feature_eng.physics")
        self.modeling_dir = Path(self.cfg['paths']['data']['modeling'])
        
        # Estado de memória entre anos (para dias sem chuva não zerarem em 01/Jan)
        # Formato: {cidade: {'last_days_dry': int, 'last_pse': float}}
        self.memory_state = {}

    def _calculate_features_for_chunk(self, df_base_e: pd.DataFrame, year: int) -> pd.DataFrame:
        """
        Calcula as features físicas usando a Base E (sem buracos) como referência.
        Retorna apenas as colunas chaves + novas features.
        """
        # Garante ordenação temporal para cálculos cumulativos
        df = df_base_e.sort_values(['cidade_norm', 'ts_hour']).copy()
        
        # Identifica colunas (lida com possíveis variações de nome se houver)
        # Assume que a Base E já está padronizada pelo build_datasets
        
        # --- 1. PRECIPITAÇÃO PONDERADA (PSE) ---
        # Fórmula INPE Aprox: P_hoje + 0.5*P_ontem + 0.25*P_anteontem...
        # Isso é uma Média Móvel Exponencial (EWMA) com com decay=0.5
        # Precisamos agrupar por cidade
        
        self.log.info(f"[{year}] Calculando PSE (Chuva Ponderada)...")
        
        # Função auxiliar para aplicar EWMA grupo a grupo
        def calc_ewm(x):
            return x.ewm(alpha=0.5, adjust=False).mean()

        # Nota: O EWMA do pandas é otimizado em C, é rápido.
        # Ajustamos para soma ponderada aproximada multiplicando por fator se necessário, 
        # mas a média ponderada captura a tendência de decaimento igual.
        # Para ser fiel à "Soma Ponderada", usamos adjust=False.
        
        # Agrupa e calcula
        df['precip_ewma'] = df.groupby('cidade_norm')[COL_PRECIP].transform(
            lambda x: x.ewm(alpha=0.5, adjust=False).mean()
        )

        # --- 2. DIAS SEM CHUVA (Lógica Vetorizada + Memória Anual) ---
        self.log.info(f"[{year}] Calculando Dias Sem Chuva (com memória)...")
        
        # Define limiar de chuva (INPE usa algo como < 1mm ou < 2mm para considerar "seco")
        RAIN_THRESHOLD = 1.0 
        
        # Cria booleano: 1 se Choveu, 0 se Seco
        df['is_rain'] = (df[COL_PRECIP] >= RAIN_THRESHOLD).astype(int)
        
        # Logica vetorizada para contador reiniciavel:
        # Cria grupos a cada chuva. 
        # Cumsum reinicia a cada chuva.
        
        results = []
        # Infelizmente para memória entre anos, iterar cidades é mais seguro que vetorizar tudo
        # Mas vamos otimizar fazendo apenas operações vetorizadas DENTRO de cada cidade
        
        for cidade, group in tqdm(df.groupby('cidade_norm'), desc=f"Processando Cidades ({year})", leave=False):
            # Recupera estado anterior
            initial_dry = 0
            if cidade in self.memory_state:
                initial_dry = self.memory_state[cidade]['last_dry']
            
            rain_mask = group[COL_PRECIP] >= RAIN_THRESHOLD
            
            # Identifica mudanças de estado (Chuva -> Seco ou Seco -> Chuva)
            # Mas queremos "Dias Consecutivos Secos".
            # Truque do Cumsum:
            # 1. Marca onde choveu (True)
            # 2. Faz soma cumulativa da chuva. Cada período seco terá o mesmo ID.
            # 3. Agrupa por esse ID e faz cumcount (contagem sequencial).
            
            # Ajuste para carregar o saldo do ano anterior:
            # Se a primeira linha é seca, ela deve somar ao initial_dry.
            # Se chover, zera.
            
            # Vamos iterar numpy puro que é muito rápido para 8760 horas
            vals = group[COL_PRECIP].values
            dry_days = np.zeros(len(vals), dtype=np.float32)
            
            current_counter = initial_dry
            
            for i in range(len(vals)):
                # Como é horário, vamos dividir por 24 para ter "dias" ou manter em "horas sem chuva"?
                # O INPE fala em DIAS. Vamos manter contador de HORAS e dividir por 24 na feature final.
                if vals[i] < RAIN_THRESHOLD:
                    current_counter += 1 # Soma 1 hora
                else:
                    current_counter = 0
                dry_days[i] = current_counter
            
            # Salva estado para proximo ano
            self.memory_state[cidade] = {'last_dry': current_counter}
            
            # Atribui de volta (usando indice original para garantir alinhamento)
            group_res = pd.DataFrame({'dias_sem_chuva': dry_days / 24.0}, index=group.index)
            results.append(group_res)

        df_dry = pd.concat(results)
        df = df.join(df_dry) # Join pelo índice

        # --- 3. LIMIARES DE RISCO (Tabelas 2.2 e 2.3) ---
        # Temperatura > 30 é critico
        # Umidade < 15 é critico
        
        df['risco_temp_max'] = (df[COL_TEMP] > 30).astype(int)
        df['risco_umid_critica'] = (df[COL_UMID] < 15).astype(int)
        df['risco_umid_alerta']  = ((df[COL_UMID] >= 15) & (df[COL_UMID] < 30)).astype(int)

        # --- 4. FATOR DE PROPAGAÇÃO (Vento * Secura) ---
        # Fogo corre mais se tiver vento E estiver seco
        # (Vento * Temp) / (Umidade + 1)
        df['fator_propagacao'] = (df[COL_VENTO] * df[COL_TEMP]) / (df[COL_UMID] + 1.0)
        
        # Seleciona apenas colunas chaves e novas features para merge
        cols_to_keep = [
            'cidade_norm', 'ts_hour', 
            'precip_ewma', 'dias_sem_chuva', 
            'risco_temp_max', 'risco_umid_critica', 'risco_umid_alerta', 
            'fator_propagacao'
        ]
        
        return df[cols_to_keep]

    def run(self):
        # Descobre anos disponíveis na Base E (nossa fonte de verdade física)
        base_e_path = self.modeling_dir / self.cfg['modeling_scenarios']['base_E']
        files = sorted(base_e_path.glob("inmet_bdq_*_cerrado.parquet"))
        
        if not files:
            self.log.error("Nenhum arquivo encontrado na Base E para gerar features!")
            return

        years = []
        for f in files:
            try:
                y = int(f.stem.split('_')[2])
                years.append(y)
            except: pass
        
        years = sorted(years)
        self.log.info(f"Anos detectados para processamento: {years}")

        # Loop Ano a Ano (Respeitando Memória RAM e Continuidade)
        for year in tqdm(years, desc="Processando Anos"):
            
            # 1. Carrega BASE E (Fonte)
            src_file = base_e_path / f"inmet_bdq_{year}_cerrado.parquet"
            try:
                df_base_e = pd.read_parquet(src_file)
            except Exception as e:
                self.log.error(f"Erro lendo Base E ({year}): {e}")
                continue

            # 2. Calcula Features Físicas (Cria df_features com chaves e novas cols)
            df_features = self._calculate_features_for_chunk(df_base_e, year)
            
            # Limpa RAM da base E completa
            del df_base_e
            gc.collect()

            # 3. Distribui para TODAS as bases (A, B, C, D, E, F)
            for scenario_key in SCENARIOS_TO_PROCESS:
                folder_name = SCENARIO_FOLDERS.get(scenario_key)
                if not folder_name: continue
                
                # Define caminhos
                original_dir = self.modeling_dir / folder_name
                input_parquet = original_dir / f"inmet_bdq_{year}_cerrado.parquet"
                
                # Novo diretório: ex: data/modeling/base_F_calculated_features/
                new_folder_name = f"{folder_name}_calculated"
                target_dir = self.modeling_dir / new_folder_name
                utils.ensure_dir(target_dir)
                
                output_parquet = target_dir / f"inmet_bdq_{year}_cerrado.parquet"
                
                if not input_parquet.exists():
                    continue

                # Carrega Base Alvo
                try:
                    df_target = pd.read_parquet(input_parquet)
                    
                    # MERGE (Left Join na Base Alvo)
                    # A base alvo pode ter linhas a menos (Drop Rows), então left join preserva isso.
                    # A base alvo pode ter NaNs. As novas features virão preenchidas (pois vieram da E).
                    
                    # Garante chaves string/datetime compatíveis
                    if 'ts_hour' not in df_target.columns and 'DATA' in df_target.columns:
                         # Caso precise converter (geralmente parquets já estao ok)
                         pass
                    
                    # O Merge
                    df_enriched = df_target.merge(
                        df_features, 
                        on=['cidade_norm', 'ts_hour'], 
                        how='left'
                    )
                    
                    # Salva
                    df_enriched.to_parquet(output_parquet, index=False)
                    # self.log.info(f"Salvo: {output_parquet.name} em {new_folder_name}")
                    
                except Exception as e:
                    self.log.error(f"Erro ao enriquecer {scenario_key} ({year}): {e}")

            # Limpa RAM das features do ano
            del df_features
            gc.collect()

        self.log.info("Processamento de Engenharia de Features Concluído.")

if __name__ == "__main__":
    PhysicsFeatureEngineer().run()