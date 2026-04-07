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

class RiskMerger:
    def __init__(self):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("merger.risk")
        
        # Configurações
        self.base_scenario = "base_F" # Usando Full Original
        self.target_year = 2024
        self.folder_name = self.cfg['modeling_scenarios'][self.base_scenario]
        
        self.parquet_path = (Path(self.cfg['paths']['data']['modeling']) / 
                             self.folder_name / 
                             f"inmet_bdq_{self.target_year}_cerrado.parquet")
        
        self.nc_raw_path = Path(self.cfg['paths']['providers']['risco_fogo']['raw'])
        utils.ensure_dir(self.nc_raw_path)

        # URL do INPE
        self.url_template = "https://dataserver-coids.inpe.br/queimadas/queimadas/riscofogo_meteorologia/observado/risco_fogo/{year}/INPE_FireRiskModel_2.2_FireRisk_{date_str}.nc"

    def download_day(self, date_obj, force=False) -> Path:
        """
        Baixa o arquivo do dia.
        Se force=False e o arquivo já existir, NÃO baixa de novo (economiza tempo/banda).
        """
        date_str = date_obj.strftime("%Y%m%d")
        year = date_obj.year
        filename = f"INPE_FireRiskModel_2.2_FireRisk_{date_str}.nc"
        local_file = self.nc_raw_path / filename
        
        # Se já existe e não estamos forçando, usa o local
        if local_file.exists() and not force:
            return local_file
            
        url = self.url_template.format(year=year, date_str=date_str)
        try:
            response = requests.get(url, stream=True, timeout=60)
            if response.status_code == 200:
                with open(local_file, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return local_file
            return None
        except Exception:
            return None

    def run(self):
        self.log.info(f"Carregando base: {self.parquet_path}")
        if not self.parquet_path.exists():
            self.log.error("Arquivo Parquet não encontrado.")
            return

        df = pd.read_parquet(self.parquet_path)
        
        # Normalização de Datas
        if 'Data' in df.columns:
            df['dt_ref'] = pd.to_datetime(df['Data'], format='%Y-%m-%d', errors='coerce')
        elif 'ts_hour' in df.columns:
            df['dt_ref'] = pd.to_datetime(df['ts_hour']).dt.normalize()
        
        df['RISCO_FOGO_NOVO'] = np.nan
        
        unique_dates = sorted(df['dt_ref'].dropna().unique())
        self.log.info(f"Processando {len(unique_dates)} dias únicos...")

        for date_val in tqdm(unique_dates):
            ts = pd.Timestamp(date_val)
            
            # 1. Filtra os pontos do dia (Estações)
            mask_day = df['dt_ref'] == ts
            day_points = df.loc[mask_day, ['LATITUDE', 'LONGITUDE']]
            
            if day_points.empty:
                continue

            # 2. Obtém arquivo NC (Sem baixar se já existir)
            nc_file = self.download_day(ts, force=False)
            if not nc_file:
                continue
            
            ds = None
            try:
                # Tenta abrir o dataset
                ds = xr.open_dataset(nc_file)
            
            except Exception as e:
                # --- TRATAMENTO DE ARQUIVO CORROMPIDO ---
                # Se der erro de HDF/NetCDF, assume corrupção: Deleta e Baixa de Novo
                err_msg = str(e)
                if "HDF" in err_msg or "NetCDF" in err_msg or "truncate" in err_msg:
                    self.log.warning(f"Arquivo corrompido detectado ({nc_file.name}). Tentando recuperar...")
                    try:
                        ds.close() if ds else None
                        os.remove(nc_file) # Deleta o podre
                        
                        # Baixa forçado
                        nc_file = self.download_day(ts, force=True)
                        if nc_file:
                            ds = xr.open_dataset(nc_file) # Tenta abrir de novo
                        else:
                            continue # Se não conseguiu baixar de novo, pula
                    except Exception as e2:
                        self.log.error(f"Falha na recuperação do dia {ts}: {e2}")
                        continue
                else:
                    self.log.error(f"Erro genérico dia {ts}: {e}")
                    continue

            # Se chegamos aqui, o ds está aberto e válido
            try:
                # Pega a primeira variável de dados
                var_name = list(ds.data_vars)[0]
                
                # Correção de Dimensão de Tempo
                if 'time' in ds.dims:
                    ds_slice = ds[var_name].isel(time=0)
                else:
                    ds_slice = ds[var_name]

                # Prepara coordenadas
                target_lats = xr.DataArray(day_points['LATITUDE'].values, dims="points")
                target_lons = xr.DataArray(day_points['LONGITUDE'].values, dims="points")
                
                # Extração espacial
                raw_values = ds_slice.sel(
                    lat=target_lats, 
                    lon=target_lons, 
                    method='nearest',
                    tolerance=0.1
                )
                
                risk_values_flat = raw_values.values.flatten()

                # Atribuição Segura
                if len(risk_values_flat) == len(day_points):
                    df.loc[mask_day, 'RISCO_FOGO_NOVO'] = risk_values_flat
                
                ds.close()

            except Exception as e:
                self.log.error(f"Erro processamento lógico dia {ts}: {e}")
                if ds: ds.close()

        # --- FASE DE VALIDAÇÃO E RELATÓRIO ---
        self.log.info("Calculando métricas de validação...")

        # Correção de Tipos (String -> Float) para evitar TypeError
        # Remove vírgulas se existirem e converte para numérico
        if df['RISCO_FOGO'].dtype == 'object' or df['RISCO_FOGO'].dtype == 'string':
            df['RISCO_FOGO'] = df['RISCO_FOGO'].astype(str).str.replace(',', '.')
        
        df['RISCO_FOGO'] = pd.to_numeric(df['RISCO_FOGO'], errors='coerce')

        # Cria subset apenas onde temos os dois dados para comparar
        validation_set = df.dropna(subset=['RISCO_FOGO', 'RISCO_FOGO_NOVO'])
        
        print("\n" + "="*60)
        print(f"RELATÓRIO DE VALIDAÇÃO DE RISCO DE FOGO ({self.target_year})")
        print("="*60)
        
        if not validation_set.empty:
            corr = validation_set['RISCO_FOGO'].corr(validation_set['RISCO_FOGO_NOVO'])
            mae = (validation_set['RISCO_FOGO'] - validation_set['RISCO_FOGO_NOVO']).abs().mean()
            
            print(f"Total de Pontos Cruzados (Focos): {len(validation_set)}")
            print(f"Correlação de Pearson:          {corr:.4f}")
            print(f"Erro Médio Absoluto (MAE):      {mae:.4f}")
            print("-" * 60)
            print("AMOSTRA COMPARATIVA (TOP 10):")
            try:
                print(validation_set[['LATITUDE', 'LONGITUDE', 'RISCO_FOGO', 'RISCO_FOGO_NOVO']].head(10).to_markdown(index=False))
            except:
                print(validation_set[['LATITUDE', 'LONGITUDE', 'RISCO_FOGO', 'RISCO_FOGO_NOVO']].head(10))
        else:
            self.log.warning("AVISO: Nenhuma interseção encontrada entre o Risco Original (Focos) e o Risco Novo (Grade).")
            self.log.warning("Verifique se a coluna 'RISCO_FOGO' original não está totalmente vazia.")

        print("="*60 + "\n")

        # Salva o arquivo final
        output_filename = f"inmet_bdq_{self.target_year}_cerrado_risco_fogo.parquet"
        output_path = self.parquet_path.parent / output_filename
        
        df.to_parquet(output_path)
        self.log.info(f"Arquivo enriquecido salvo em: {output_path}")

if __name__ == "__main__":
    RiskMerger().run()