import os
import requests
from bs4 import BeautifulSoup
import zipfile
from datetime import datetime
import yaml

script_dir = os.path.dirname(os.path.abspath(__file__))
config_dir = os.path.join(script_dir, "config.yaml")

# load configuration
with open(config_dir, 'r') as config_file:
    config = yaml.safe_load(config_file)

# Get raw data path from config
raw_data_path = config["paths"]["data_raw"]

# Create INMET directory if it doesn't exist
inmet_path = os.path.join(script_dir, raw_data_path, "INMET")
os.makedirs(inmet_path, exist_ok=True)

csv_output_path = os.path.join(script_dir, inmet_path, "csv")
os.makedirs(csv_output_path, exist_ok=True)

# Base URL for INMET historical data
inmet_base_url = "https://portal.inmet.gov.br/dadoshistoricos"

def download_inmet_data():
    # Acessa a página com os links dos arquivos ZIP
    response = requests.get("https://portal.inmet.gov.br/dadoshistoricos")
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")

    # Encontra todos os links para arquivos .zip
    zip_links = soup.find_all("a", href=True)
    zip_links = [link["href"] for link in zip_links if link["href"].endswith(".zip")]

    print(f"{len(zip_links)} arquivos encontrados para download.")

    for link in zip_links:
        filename = os.path.basename(link)
        file_path = os.path.join(inmet_path, filename)

        # Pula se o arquivo já existe
        if os.path.exists(file_path):
            print(f"[SKIP] {filename} já existe.")
            continue

        print(f"[DOWNLOADING] {filename}...")
        try:
            zip_response = requests.get(link, stream=True)
            zip_response.raise_for_status()

            # Salva o arquivo .zip
            with open(file_path, "wb") as f:
                for chunk in zip_response.iter_content(chunk_size=1024):
                    if chunk:
                        f.write(chunk)

            print(f"[SUCCESS] {filename} salvo em {file_path}")
        except Exception as e:
            print(f"[ERROR] Falha ao baixar {filename}: {e}")

def unzip_inmet_files():
    for filename in os.listdir(inmet_path):
        if filename.endswith(".zip"):
            zip_path = os.path.join(inmet_path, filename)
            extract_dir = os.path.join(csv_output_path, filename.replace(".zip", ""))

            if os.path.exists(extract_dir):
                print(f"[SKIP] Arquivos de {filename} já extraídos.")
                continue

            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                print(f"[SUCCESS] {filename} extraído para {extract_dir}")
            except zipfile.BadZipFile:
                print(f"[ERROR] {filename} está corrompido ou não é um ZIP válido.")

def unzip_br_focos_files():
    brasil_zip_path = os.path.join(script_dir, config["paths"]["data_raw"], "Brasil_sat_ref")
    brasil_zip_path = os.path.abspath(brasil_zip_path)

    csv_output_path = os.path.join(brasil_zip_path, "csv")
    os.makedirs(csv_output_path, exist_ok=True)

    for filename in os.listdir(brasil_zip_path):
        if filename.endswith(".zip"):
            zip_path = os.path.join(brasil_zip_path, filename)
            extract_dir = os.path.join(csv_output_path, filename.replace(".zip", ""))

            if os.path.exists(extract_dir):
                print(f"[SKIP] {filename} já extraído.")
                continue

            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_dir)
                print(f"[SUCCESS] {filename} extraído em {extract_dir}")
            except zipfile.BadZipFile:
                print(f"[ERROR] {filename} corrompido ou não é um ZIP válido.")


if __name__ == "__main__":
    download_inmet_data() 
    unzip_inmet_files()
    unzip_br_focos_files()