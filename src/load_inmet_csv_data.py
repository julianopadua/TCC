import os
import yaml
import pandas as pd

script_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(script_dir, "config.yaml")

# Carrega configuração
with open(config_dir, 'r') as config_file:
    config = yaml.safe_load(config_file)

# Caminhos
raw_data_path = config["paths"]["data_raw"]
processed_data_path = config["paths"]["data_processed"]
csv_base_path = os.path.join(script_dir, raw_data_path, "INMET", "csv")
processed_path = os.path.join(script_dir, processed_data_path, "INMET")
os.makedirs(processed_path, exist_ok=True)

# Colunas que devem ser removidas
colunas_remover = [
    "PRESSÃO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)",
    "PRESSÃO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)",
    "TEMPERATURA MÁXIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA MÍNIMA NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)",
    "TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)",
    "UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)",
    "UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)"
]

def processar_ano(year_int):
    year_folder = str(year_int)
    year_path = os.path.join(csv_base_path, year_folder, year_folder)

    if not os.path.isdir(year_path):
        print(f"[⚠] Pasta do ano {year_int} não encontrada.")
        return

    arquivos = [f for f in os.listdir(year_path) if f.endswith(".CSV")]
    dfs = []

    for file in arquivos:
        file_path = os.path.join(year_path, file)
        try:
            with open(file_path, "r", encoding="latin1") as f:
                lines = f.readlines()
                header_line = lines[8].strip()
                header = [h.strip() for h in header_line.split(";") if h.strip() != ""]

                cidade = lines[2].strip().split(";")[1] if len(lines[2].split(";")) > 1 else None
                latitude = lines[4].strip().split(";")[1] if len(lines[4].split(";")) > 1 else None
                longitude = lines[5].strip().split(";")[1] if len(lines[5].split(";")) > 1 else None

            df = pd.read_csv(file_path, sep=";", skiprows=9, encoding="latin1", engine="python", on_bad_lines='skip')

            # Ajusta o header caso o número de colunas não bata
            if df.shape[1] > len(header):
                print(f"[⚠] Colunas extras detectadas em: {file_path}")
                while len(header) < df.shape[1]:
                    header.append(f"COLUNA_EXTRA_{len(header)+1}")
            elif df.shape[1] < len(header):
                print(f"[⚠] Header maior que colunas de dados em: {file_path}. Pulando...")
                continue

            df.columns = header

            # Adiciona colunas fixas
            df["ANO"] = year_int
            df["CIDADE"] = cidade
            df["LATITUDE"] = latitude
            df["LONGITUDE"] = longitude

            # Remove colunas indesejadas
            colunas_existentes = [col for col in colunas_remover if col in df.columns]
            df.drop(columns=colunas_existentes, inplace=True)

            # Remove qualquer coluna extra adicionada automaticamente
            df = df.loc[:, ~df.columns.str.startswith("COLUNA_EXTRA")]

            dfs.append(df)
            print(f"[✔] Lido: {file_path}")

        except Exception as e:
            print(f"[ERRO] {file_path} -> {e}")

    if dfs:
        df_final = pd.concat(dfs, ignore_index=True)
        out_file = os.path.join(processed_path, f"inmet_{year_int}.csv")
        df_final.to_csv(out_file, index=False)
        print(f"[💾] Dados de {year_int} salvos em: {out_file}")
    else:
        print(f"[⚠] Nenhum dado válido encontrado para {year_int}.")

def load_and_process_by_year():
    for year_int in range(2000, 2026):
        out_file = os.path.join(processed_path, f"inmet_{year_int}.csv")

        if os.path.exists(out_file):
            print(f"[⏩] inmet_{year_int}.csv já existe. Pulando...")
            continue

        resposta = input(f"Deseja processar o ano {year_int}? (s/n): ").strip().lower()
        if resposta == "s":
            processar_ano(year_int)
        else:
            print(f"[⏩] Pulando o ano {year_int} por escolha do usuário.")

if __name__ == "__main__":
    load_and_process_by_year()
