# src/audit_databases.py
# =============================================================================
# AUDITORIA DE BASES DE MODELAGEM (AGREGADO POR CENÁRIO)
# =============================================================================

import pandas as pd
import numpy as np
import re
from pathlib import Path
from typing import Dict, Any, List
from collections import defaultdict

# Importa utils do projeto
try:
    import utils
except ImportError:
    import src.utils as utils

class ScenarioAuditor:
    """
    Responsável por varrer uma pasta de cenário, ler todos os parquets
    e acumular estatísticas globais e anuais.
    """
    def __init__(self, log):
        self.log = log
        # Variáveis críticas citadas no TCC
        self.target_col = "HAS_FOCO"
        self.climatic_cols = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
            'RADIACAO GLOBAL (KJ/m²)',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)',
            'UMIDADE RELATIVA DO AR, HORARIA (%)'
        ]

    def audit_scenario(self, scenario_path: Path) -> Dict[str, Any]:
        """Processa todos os arquivos .parquet de um cenário."""
        
        # Encontrar arquivos que batem com o padrão (inmet_bdq_YYYY_cerrado.parquet)
        files = sorted(list(scenario_path.glob("*.parquet")))
        
        if not files:
            return None

        # Estruturas para acumular dados
        global_stats = {
            "total_rows": 0,
            "target_counts": defaultdict(int),
            "missing_counts": defaultdict(lambda: {"nulls": 0, "sentinels": 0}),
            "years_covered": []
        }
        
        yearly_breakdown = {}

        for file_path in files:
            # Extrair ano do nome do arquivo (ex: inmet_bdq_2003_cerrado.parquet)
            match = re.search(r"(\d{4})", file_path.name)
            year = int(match.group(1)) if match else "Unknown"
            
            self.log.debug(f"Processando {file_path.name}...")
            
            try:
                df = pd.read_parquet(file_path)
            except Exception as e:
                self.log.error(f"Erro lendo {file_path.name}: {e}")
                continue

            # --- Estatísticas do Ano ---
            n_rows = len(df)
            global_stats["total_rows"] += n_rows
            global_stats["years_covered"].append(year)

            # Target Distribution
            if self.target_col in df.columns:
                vc = df[self.target_col].value_counts().to_dict()
                t_counts = {k: int(v) for k, v in vc.items()}
                
                # Acumula no global
                for k, v in t_counts.items():
                    global_stats["target_counts"][k] += v
            else:
                t_counts = {}

            # Qualidade das Colunas (Nulos e Sentinelas)
            cols_stats = {}
            for col in self.climatic_cols:
                if col in df.columns:
                    nulls = int(df[col].isna().sum())
                    
                    # Sentinelas (-999, -9999) - Apenas se numérico
                    sentinels = 0
                    if pd.api.types.is_numeric_dtype(df[col]):
                        sentinels = int(((df[col] <= -999)).sum())

                    # Salva para o ano
                    cols_stats[col] = {"nulls": nulls, "sentinels": sentinels}
                    
                    # Acumula no global
                    global_stats["missing_counts"][col]["nulls"] += nulls
                    global_stats["missing_counts"][col]["sentinels"] += sentinels

            # Registra snapshot do ano
            yearly_breakdown[year] = {
                "rows": n_rows,
                "target_balance": t_counts,
                "columns_quality": cols_stats
            }

        global_stats["years_covered"] = sorted(global_stats["years_covered"])
        
        return {
            "global": global_stats,
            "yearly": yearly_breakdown
        }


class MarkdownReporter:
    """
    Gera o relatório .md formatado para a pasta doc/databases.
    """
    def __init__(self, output_root: Path):
        self.output_dir = utils.ensure_dir(output_root)

    def generate_report(self, scenario_name: str, data: Dict[str, Any]):
        if not data:
            return

        filename = self.output_dir / f"{scenario_name}.md"
        g = data["global"]
        y_data = data["yearly"]
        
        total_rows = g["total_rows"]
        
        with open(filename, "w", encoding="utf-8") as f:
            # Cabeçalho
            f.write(f"# Auditoria Consolidada: {scenario_name}\n\n")
            f.write(f"**Arquivos Processados:** {min(g['years_covered'])} a {max(g['years_covered'])}\n")
            f.write(f"**Total de Registros (Agregado):** {total_rows:,}\n\n")

            # 1. Distribuição Global do Target
            f.write("## 1. Distribuição Global da Variável Alvo (HAS_FOCO)\n")
            f.write("A tabela abaixo soma as ocorrências de todos os anos processados.\n\n")
            f.write("| Classe | Contagem Absoluta | Proporção (%) |\n")
            f.write("| :--- | :---: | :---: |\n")
            
            # Ordenar chaves (0, 1)
            target_counts = g["target_counts"]
            for cls in sorted(target_counts.keys()):
                count = target_counts[cls]
                pct = (count / total_rows * 100) if total_rows > 0 else 0
                label = "**Fogo (1)**" if cls == 1 else "Sem Fogo (0)"
                f.write(f"| {label} | {count:,} | {pct:.4f}% |\n")

            # 2. Qualidade Global dos Dados
            f.write("\n## 2. Qualidade Global (Variáveis Climáticas)\n")
            f.write("Acumulado de nulos e códigos sentinela (-999, -9999).\n\n")
            f.write("| Variável | Nulos (NaN) | Sentinelas (<= -999) | Total 'Ausente' | % da Base |\n")
            f.write("| :--- | :---: | :---: | :---: | :---: |\n")

            for col, stats in g["missing_counts"].items():
                n = stats["nulls"]
                s = stats["sentinels"]
                total_bad = n + s
                pct_bad = (total_bad / total_rows * 100) if total_rows > 0 else 0
                f.write(f"| `{col}` | {n:,} | {s:,} | {total_bad:,} | {pct_bad:.2f}% |\n")

            # 3. Análise Temporal (Ano a Ano) - CRUCIAL
            f.write("\n## 3. Detalhamento Temporal (Ano a Ano)\n")
            f.write("Permite identificar anos com falhas massivas de coleta ou quebras de padrão.\n\n")
            f.write("| Ano | Linhas | Focos (#) | Focos (%) | Temp. (Nulos+Sent.) | Rad. (Nulos+Sent.) |\n")
            f.write("| :--- | :---: | :---: | :---: | :---: | :---: |\n")

            sorted_years = sorted(y_data.keys())
            for yr in sorted_years:
                ys = y_data[yr]
                rows = ys["rows"]
                
                # Target info
                focos = ys["target_balance"].get(1, 0)
                focos_pct = (focos / rows * 100) if rows > 0 else 0
                
                # Variaveis chave para tabela resumo (Temp e Radiação)
                # Helper interno para pegar total de falhas de uma coluna
                def get_bad(cname):
                    if cname in ys["columns_quality"]:
                        return ys["columns_quality"][cname]["nulls"] + ys["columns_quality"][cname]["sentinels"]
                    return 0

                bad_temp = get_bad('TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)')
                bad_rad = get_bad('RADIACAO GLOBAL (KJ/m²)')
                
                # Formatação condicional simples (Bold se > 0 falhas)
                str_temp = f"**{bad_temp:,}**" if bad_temp > 0 else f"{bad_temp}"
                str_rad = f"**{bad_rad:,}**" if bad_rad > 0 else f"{bad_rad}"

                f.write(f"| {yr} | {rows:,} | {focos:,} | {focos_pct:.4f}% | {str_temp} | {str_rad} |\n")

        print(f"✅ Relatório gerado: {filename}")


class Orchestrator:
    def __init__(self):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("audit.main", kind="audit", per_run_file=True)
        self.auditor = ScenarioAuditor(self.log)
        self.reporter = MarkdownReporter(Path(self.cfg['paths']['doc']) / "databases")

    def run(self):
        modeling_root = utils.get_path("paths", "data", "modeling")
        scenarios = self.cfg.get("modeling_scenarios", {})

        self.log.info("Iniciando auditoria completa de todas as bases...")

        for scenario_key, folder_name in scenarios.items():
            scenario_path = modeling_root / folder_name
            
            if not scenario_path.exists():
                self.log.warning(f"Pasta não encontrada: {scenario_path}. Pulando.")
                continue

            self.log.info(f"Auditando cenário: {scenario_key} ({folder_name})")
            
            # 1. Computa estatísticas
            stats = self.auditor.audit_scenario(scenario_path)
            
            # 2. Gera relatório
            if stats:
                self.reporter.generate_report(f"{folder_name}", stats)
            else:
                self.log.warning(f"Nenhum parquet válido encontrado em {folder_name}")

if __name__ == "__main__":
    app = Orchestrator()
    app.run()