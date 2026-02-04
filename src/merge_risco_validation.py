# src/merge_risco_validation.py
# =============================================================================
# MERGE E VALIDAÇÃO DE RISCO DE FOGO (INPE NETCDF + PARQUET)
# =============================================================================

import sys
import os
import requests
import xarray as xr
import pandas as pd
import numpy as np
from pathlib import Path
from tqdm import tqdm # Barra de progresso

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

class RiskMerger:
    def __init__(self):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("merger.risk")
        
        # Configurações de Caminhos
        self.base_scenario = "base_F" # Usando a Full Original como acordado
        self.target_year = 2024
        self.folder_name = self.cfg['modeling_scenarios'][self.base_scenario]
        
        self.parquet_path = (Path(self.cfg['paths']['data']['modeling']) / 
                             self.folder_name / 
                             f"inmet_bdq_{self.target_year}_cerrado.parquet")
        
        self.nc_raw_path = Path(self.cfg['paths']['providers']['risco_fogo']['raw'])
        utils.ensure_dir(self.nc_raw_path)

        # URL Base do INPE
        self.url_template = "https://dataserver-coids.inpe.br/queimadas/queimadas/riscofogo_meteorologia/observado/risco_fogo/{year}/INPE_FireRiskModel_2.2_FireRisk_{date_str}.nc"

    def download_day(self, date_obj) -> Path:
        """Baixa o NC do dia específico se não existir."""
        date_str = date_obj.strftime("%Y%m%d")
        year = date_obj.year
        filename = f"INPE_FireRiskModel_2.2_FireRisk_{date_str}.nc"
        local_file = self.nc_raw_path / filename
        
        if local_file.exists():
            return local_file
            
        url = self.url_template.format(year=year, date_str=date_str)
        try:
            # self.log.info(f"Baixando: {filename}")
            response = requests.get(url, stream=True, timeout=30)
            if response.status_code == 200:
                with open(local_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return local_file
            else:
                self.log.warning(f"Arquivo não encontrado no servidor: {filename} (Status {response.status_code})")
                return None
        except Exception as e:
            self.log.error(f"Erro download {filename}: {e}")
            return None

    def run(self):
        # 1. Carregar Parquet Original
        self.log.info(f"Carregando base: {self.parquet_path}")
        if not self.parquet_path.exists():
            self.log.error("Arquivo Parquet não encontrado.")
            return

        df = pd.read_parquet(self.parquet_path)
        
        # Garante datetime
        # Assumindo que você tem uma coluna de data. Se for 'ts_hour', converte.
        if 'Data' in df.columns:
            df['dt_ref'] = pd.to_datetime(df['Data'], format='%Y-%m-%d', errors='coerce')
        elif 'ts_hour' in df.columns:
            df['dt_ref'] = pd.to_datetime(df['ts_hour']).dt.normalize()
        else:
            self.log.error("Coluna de data não identificada (Data ou ts_hour).")
            return

        # Prepara coluna nova
        df['RISCO_FOGO_NOVO'] = np.nan
        
        # Identificar dias únicos para iterar (muito mais rápido que iterar linhas)
        unique_dates = df['dt_ref'].dropna().unique()
        unique_dates = sorted(unique_dates)
        
        self.log.info(f"Processando {len(unique_dates)} dias únicos...")

        # 2. Loop por Dias
        for date_val in tqdm(unique_dates):
            ts = pd.Timestamp(date_val)
            
            # Baixa arquivo do dia
            nc_file = self.download_day(ts)
            if not nc_file:
                continue
                
            try:
                # Abre o NetCDF
                ds = xr.open_dataset(nc_file)
                
                # Descobre o nome da variável de risco (pode variar, pega a primeira var de dados)
                var_name = list(ds.data_vars)[0] 
                
                # Filtra o DataFrame apenas para este dia
                mask_day = df['dt_ref'] == ts
                day_points = df.loc[mask_day, ['LATITUDE', 'LONGITUDE']]
                
                if day_points.empty:
                    continue

                # --- EXTRAÇÃO ESPACIAL (A MÁGICA) ---
                # Cria arrays xarray para as coordenadas dos pontos do parquet
                lat_target = xr.DataArray(day_points['LATITUDE'].values, dims="points")
                lon_target = xr.DataArray(day_points['LONGITUDE'].values, dims="points")
                
                # .sel com method='nearest' busca o pixel mais próximo para cada ponto
                risk_values = ds[var_name].sel(
                    lat=lat_target, 
                    lon=lon_target, 
                    method='nearest',
                    tolerance=0.1 # Tolerância de ~10km (se a estação estiver muito longe da grade, vira NaN)
                ).values
                
                # Atribui de volta ao DataFrame
                df.loc[mask_day, 'RISCO_FOGO_NOVO'] = risk_values
                
                ds.close()
                
            except Exception as e:
                self.log.error(f"Erro ao processar dia {ts}: {e}")

        # 3. Validação e Comparação
        self.log.info("Gerando relatório de validação...")
        
        # Filtra onde temos os DOIS valores (o antigo vazado e o novo extraído)
        validation_set = df.dropna(subset=['RISCO_FOGO', 'RISCO_FOGO_NOVO'])
        
        if not validation_set.empty:
            corr = validation_set['RISCO_FOGO'].corr(validation_set['RISCO_FOGO_NOVO'])
            mae = (validation_set['RISCO_FOGO'] - validation_set['RISCO_FOGO_NOVO']).abs().mean()
            
            print("\n" + "="*50)
            print("RELATÓRIO DE VALIDAÇÃO (2024)")
            print("="*50)
            print(f"Linhas comparáveis (Onde existia Foco): {len(validation_set)}")
            print(f"Correlação (Old vs New): {corr:.4f}")
            print(f"Erro Médio Absoluto (MAE): {mae:.4f}")
            print("-" * 50)
            print(validation_set[['LATITUDE', 'LONGITUDE', 'RISCO_FOGO', 'RISCO_FOGO_NOVO']].head(10).to_markdown(index=False))
            print("="*50 + "\n")
            
            if corr > 0.8:
                self.log.info("SUCESSO: Forte correlação encontrada. A extração espacial está correta.")
            else:
                self.log.warning("ATENÇÃO: Correlação baixa. Verifique se as coordenadas ou dataframes estão alinhados.")
        else:
            self.log.warning("Não foi possível validar: A coluna antiga 'RISCO_FOGO' parece estar toda vazia ou sem interseção.")

        # 4. Salvar Novo Parquet
        output_filename = f"inmet_bdq_{self.target_year}_cerrado_risco_fogo.parquet"
        output_path = self.parquet_path.parent / output_filename
        
        df.to_parquet(output_path)
        self.log.info(f"Arquivo salvo: {output_path}")
        self.log.info(f"Novas colunas: {list(df.columns)}")

if __name__ == "__main__":
    RiskMerger().run()