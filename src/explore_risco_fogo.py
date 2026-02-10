# src/explore_risco_fogo.py
# =============================================================================
# EXPLORADOR DE DADOS DE RISCO DE FOGO (INPE - NetCDF)
# =============================================================================

import sys
import os
import requests
import xarray as xr
import pandas as pd
from pathlib import Path

# --- BOILERPLATE DE PATH ---
# Adiciona a raiz do projeto ao path para importar src.utils
current_file = Path(__file__).resolve()
project_root = current_file.parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

try:
    import src.utils as utils
except ImportError:
    print("[ERRO] Não foi possível importar src.utils. Verifique o path.")
    sys.exit(1)

class FireRiskExplorer:
    def __init__(self):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("explorer.riscofogo")
        
        # Pega o caminho configurado no yaml
        self.raw_path = Path(self.cfg['paths']['providers']['risco_fogo']['raw'])
        utils.ensure_dir(self.raw_path)

        # Configuração da URL do INPE (Hardcoded para teste de 2024)
        self.base_url = "https://dataserver-coids.inpe.br/queimadas/queimadas/riscofogo_meteorologia/observado/risco_fogo/2024/"
        # Vamos pegar o dia 01/01/2024 como amostra
        self.target_file = "INPE_FireRiskModel_2.2_FireRisk_20240101.nc"

    def download_sample(self) -> Path:
        """Baixa um arquivo de amostra se não existir."""
        local_file = self.raw_path / self.target_file
        remote_url = f"{self.base_url}{self.target_file}"

        if local_file.exists():
            self.log.info(f"Arquivo já existe em: {local_file}")
            return local_file

        self.log.info(f"Iniciando download de: {self.target_file}")
        self.log.info(f"URL: {remote_url}")
        
        try:
            response = requests.get(remote_url, stream=True, timeout=60)
            response.raise_for_status()
            
            with open(local_file, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            self.log.info("Download concluído com sucesso!")
            return local_file
        except Exception as e:
            self.log.error(f"Falha no download: {e}")
            sys.exit(1)

    def inspect_data(self, file_path: Path):
        """Abre o NetCDF e converte uma fatia para Pandas."""
        self.log.info(f"Abrindo arquivo NetCDF com Xarray...")
        
        try:
            # Abre o dataset (engine netcdf4 é necessária)
            ds = xr.open_dataset(file_path)
            
            print("\n" + "="*60)
            print(f"ESTRUTURA DO CUBO DE DADOS ({file_path.name})")
            print("="*60)
            print(ds)
            print("\nCOORDENADAS:")
            print(f" > Latitude: {ds.lat.min().values:.2f} a {ds.lat.max().values:.2f} (Tam: {len(ds.lat)})")
            print(f" > Longitude: {ds.lon.min().values:.2f} a {ds.lon.max().values:.2f} (Tam: {len(ds.lon)})")
            print(f" > Tempo: {ds.time.values}")

            print("\n" + "="*60)
            print("VARIÁVEIS DE DADOS (DATA VARIABLES)")
            print("="*60)
            for var in ds.data_vars:
                long_name = ds[var].attrs.get('long_name', 'N/A')
                units = ds[var].attrs.get('units', 'N/A')
                print(f" >> {var:<15} | Desc: {long_name} | Unid: {units}")

            # --- CONVERSÃO PARA PANDAS ---
            print("\n" + "="*60)
            print("AMOSTRA TABULAR (PANDAS)")
            print("="*60)
            self.log.info("Convertendo fatia para DataFrame (removendo NaNs de oceano)...")
            
            # ATENÇÃO: Convertemos apenas o primeiro dia (isel time=0) para não estourar a RAM
            # O .to_dataframe() cria um índice MultiIndex (time, lat, lon)
            df = ds.isel(time=0).to_dataframe().reset_index()
            
            # Filtra onde o risco é NaN (normalmente oceano ou fora da máscara do Brasil)
            # O nome da variável geralmente é 'risco_fogo' ou 'risk', vamos descobrir no print acima.
            # Vou assumir que seja a primeira variável de dados encontrada para ser genérico
            first_var = list(ds.data_vars)[0]
            df_clean = df.dropna(subset=[first_var])
            
            print(f"Total de Pixels Válidos (Terra): {len(df_clean):,}")
            print("\nHEAD (5 primeiras linhas):")
            # Usa tabulate se disponível, senão print normal
            try:
                print(df_clean.head(5).to_markdown(index=False))
            except:
                print(df_clean.head(5))

            print("\nDESCRIBE (Estatísticas do Risco):")
            print(df_clean[[first_var]].describe())
            
            return df_clean

        except Exception as e:
            self.log.error(f"Erro ao ler NetCDF: {e}")
            import traceback
            traceback.print_exc()

def main():
    explorer = FireRiskExplorer()
    nc_path = explorer.download_sample()
    explorer.inspect_data(nc_path)

if __name__ == "__main__":
    main()