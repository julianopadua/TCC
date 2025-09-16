# TCC — Previsão de Queimadas (BDQueimadas + INMET)

Projeto de ETL/Análise para construir séries anuais de focos de calor (BDQueimadas/INPE) e variáveis climáticas (INMET), visando modelagem e previsão.

## Sumário
- [1. Visão geral](#1-visão-geral)
- [2. Estrutura de pastas](#2-estrutura-de-pastas)
- [3. Pré-requisitos](#3-pré-requisitos)
- [4. Ambiente Python (venv)](#4-ambiente-python-venv)
  - [4.1 Windows](#41-windows)
  - [4.2 Linux/macOS](#42-linuxmacos)
- [5. Configuração do projeto](#5-configuração-do-projeto)
- [6. Execução rápida (exemplos)](#6-execução-rápida-exemplos)
- [7. Convenções e logs](#7-convenções-e-logs)
- [8. Solução de problemas](#8-solução-de-problemas)

---

## 1. Visão geral
- **Objetivo:** consolidar dados anuais de focos de calor do BDQueimadas (satélite de referência Aqua/Tarde) e séries climáticas do INMET para análise exploratória e modelagem do risco de queimadas.
- **Escopo atual:** ingestão e padronização de CSVs; organização por partições anuais; utilitários centralizados em `src/utils.py`; decisões registradas em `doc/followup_decisions.md`.

## 2. Estrutura de pastas
```text
.
├─ addons/
├─ data/
│  ├─ raw/
│  │  ├─ BDQUEIMADAS/
│  │  └─ INMET/
│  └─ processed/
│     ├─ BDQUEIMADAS/
│     └─ INMET/
├─ doc/
│  └─ followup_decisions.md
├─ images/
├─ logs/
├─ src/
│  ├─ inmet_scraper.py
│  ├─ load_inmet_csv_data.py
│  ├─ projeto_queimadas_scrapper.ipynb
│  ├─ TCC.py
│  └─ utils.py
├─ config.yaml
├─ README.md
└─ requirements.txt
```


## 3. Pré-requisitos

* Python 3.10+ (recomendado 3.11).
* Acesso à internet para baixar dependências e dados.
* Permissão de escrita no diretório do projeto (para `logs/` e `data/`).

## 4. Ambiente Python (venv)

> Use sempre um ambiente virtual isolado para garantir reprodutibilidade.

### 4.1 Windows

```powershell
# na raiz do projeto
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# se a ativação for bloqueada:
# Set-ExecutionPolicy -Scope CurrentUser RemoteSigned

pip install --upgrade pip
pip install -r requirements.txt
```

### 4.2 Linux/macOS

```bash
python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Para sair do venv:

```bash
deactivate
```

## 5. Configuração do projeto

* O arquivo `config.yaml` fica na **raiz** e centraliza caminhos e parâmetros.
* `src/utils.py` expõe `loadConfig()` e auxiliares; todos os scripts devem importar a config por ele.

Exemplo mínimo:

```python
# qualquer script em src/
from utils import loadConfig, get_logger, get_path

cfg = loadConfig()  # lê e valida config.yaml
log = get_logger("inmet")

raw_inmet = get_path("paths", "providers", "inmet", "raw")
proc_inmet = get_path("paths", "providers", "inmet", "processed")

log.info(f"INMET raw -> {raw_inmet}")
log.info(f"INMET processed -> {proc_inmet}")
```

## 6. Execução rápida (exemplos)

```bash
# executar um script diretamente
python src/inmet_scraper.py

# carregar um CSV processado específico (exemplo)
python src/load_inmet_csv_data.py
```

> Observação: parâmetros de linha de comando (anos, janelas, etc.) podem ser adicionados conforme a evolução do pipeline. Os scripts assumem que `utils.loadConfig()` encontra `config.yaml` na raiz.

## 7. Convenções e logs

* **Nomes de arquivo:**

  * BDQueimadas: `exportador_{export_date}_ref_{ref_year}.csv`
  * INMET: `inmet_{year}.csv`
* **Logs:** configurados via `logging` no `config.yaml` (padrão: `logs/app.log`, `RotatingFileHandler`).

## 8. Solução de problemas

* **Ativação do venv no Windows falha:** execute o PowerShell como usuário e rode `Set-ExecutionPolicy -Scope CurrentUser RemoteSigned` uma única vez.
* **Permissões ao criar diretórios:** verifique se a opção `io.create_missing_dirs` está `true` no `config.yaml`.
* **Conflitos de versão:** atualize `pip` e reinstale `-r requirements.txt`. Se usar Python 3.12+, mantenha as versões pinadas deste repositório.

---
