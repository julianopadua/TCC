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
        self.target_col = "HAS_FOCO"
        
        # LISTA COMPLETA conforme metadados do INMET e texto do TCC
        self.climatic_cols = [
            'PRECIPITAÇÃO TOTAL, HORÁRIO (mm)',
            'RADIACAO GLOBAL (KJ/m²)',
            'TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)',
            'UMIDADE RELATIVA DO AR, HORARIA (%)',
            'PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)',
            'VENTO, VELOCIDADE HORARIA (m/s)',
            'VENTO, DIREÇÃO HORARIA (gr) (° (gr))',
            'VENTO, RAJADA MAXIMA (m/s)'
        ]

    def audit_scenario(self, scenario_path: Path) -> Dict[str, Any]:
        """Processa todos os arquivos .parquet de um cenário."""
        
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
            match = re.search(r"(\d{4})", file_path.name)
            year = int(match.group(1)) if match else "Unknown"
            
            self.log.debug(f"Processando {file_path.name}...")
            
            try:
                # Otimização: ler apenas as colunas necessárias para auditar
                cols_to_read = self.climatic_cols + [self.target_col]
                # Verifica quais colunas realmente existem no arquivo antes de ler
                try:
                    import pyarrow.parquet as pq
                    schema = pq.read_schema(file_path)
                    available = schema.names
                    valid_cols = [c for c in cols_to_read if c in available]
                except ImportError:
                    # Fallback se pyarrow não estiver direto (mas pandas usa ele)
                    valid_cols = None # Pandas vai ler tudo ou tentar filtrar

                df = pd.read_parquet(file_path, columns=valid_cols)
                
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
                for k, v in t_counts.items():
                    global_stats["target_counts"][k] += v
            else:
                t_counts = {}

            # Qualidade das Colunas
            cols_stats = {}
            for col in self.climatic_cols:
                if col in df.columns:
                    nulls = int(df[col].isna().sum())
                    
                    sentinels = 0
                    if pd.api.types.is_numeric_dtype(df[col]):
                        # INMET usa -999 ou -9999
                        sentinels = int(((df[col] <= -999)).sum())

                    cols_stats[col] = {"nulls": nulls, "sentinels": sentinels}
                    
                    global_stats["missing_counts"][col]["nulls"] += nulls
                    global_stats["missing_counts"][col]["sentinels"] += sentinels
                else:
                    # Se a coluna não existe no arquivo (ex: radiação na base sem radiação)
                    # Marcamos como "Ausente Estrutural" (não conta como erro de dado, mas como feature inexistente)
                    cols_stats[col] = {"nulls": n_rows, "sentinels": 0} # Considera tudo nulo pra estatística
                    global_stats["missing_counts"][col]["nulls"] += n_rows

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
            f.write(f"# Auditoria Consolidada: {scenario_name}\n\n")
            f.write(f"**Arquivos Processados:** {min(g['years_covered'])} a {max(g['years_covered'])}\n")
            f.write(f"**Total de Registros:** {total_rows:,}\n\n")

            f.write("## 1. Distribuição do Target (HAS_FOCO)\n")
            f.write("| Classe | Contagem | Proporção |\n| :--- | :---: | :---: |\n")
            target_counts = g["target_counts"]
            for cls in sorted(target_counts.keys()):
                count = target_counts[cls]
                pct = (count / total_rows * 100) if total_rows > 0 else 0
                label = "**Fogo (1)**" if cls == 1 else "Sem Fogo (0)"
                f.write(f"| {label} | {count:,} | {pct:.4f}% |\n")

            f.write("\n## 2. Qualidade Global (Variáveis Climáticas)\n")
            f.write("| Variável | Nulos (NaN) | Sentinelas (<= -999) | Total Ausente | % da Base |\n")
            f.write("| :--- | :---: | :---: | :---: | :---: |\n")

            for col, stats in g["missing_counts"].items():
                n = stats["nulls"]
                s = stats["sentinels"]
                total_bad = n + s
                pct_bad = (total_bad / total_rows * 100) if total_rows > 0 else 0
                f.write(f"| `{col}` | {n:,} | {s:,} | {total_bad:,} | {pct_bad:.2f}% |\n")

            f.write("\n## 3. Detalhamento Temporal (Falhas Críticas)\n")
            f.write("| Ano | Linhas | Focos | Temp (Falhas) | Rad (Falhas) | Pressão (Falhas) | Vento (Falhas) |\n")
            f.write("| :--- | :---: | :---: | :---: | :---: | :---: | :---: |\n")

            for yr in sorted(y_data.keys()):
                ys = y_data[yr]
                rows = ys["rows"]
                focos = ys["target_balance"].get(1, 0)
                
                def get_bad(cname):
                    if cname in ys["columns_quality"]:
                        return ys["columns_quality"][cname]["nulls"] + ys["columns_quality"][cname]["sentinels"]
                    return 0

                bad_temp = get_bad('TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)')
                bad_rad = get_bad('RADIACAO GLOBAL (KJ/m²)')
                bad_press = get_bad('PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)')
                bad_wind = get_bad('VENTO, VELOCIDADE HORARIA (m/s)')
                
                f.write(f"| {yr} | {rows:,} | {focos:,} | {bad_temp:,} | {bad_rad:,} | {bad_press:,} | {bad_wind:,} |\n")

        print(f"! Relatório gerado: {filename}")


class Orchestrator:
    def __init__(self):
        self.cfg = utils.loadConfig()
        self.log = utils.get_logger("audit.main", kind="audit", per_run_file=True)
        self.auditor = ScenarioAuditor(self.log)
        self.reporter = MarkdownReporter(Path(self.cfg['paths']['doc']) / "databases")

    def run(self):
        modeling_root = utils.get_path("paths", "data", "modeling")
        scenarios = self.cfg.get("modeling_scenarios", {})
        self.log.info("Iniciando auditoria completa...")

        for scenario_key, folder_name in scenarios.items():
            scenario_path = modeling_root / folder_name
            if not scenario_path.exists():
                self.log.warning(f"Pasta não encontrada: {scenario_path}. Pulando.")
                continue
            
            self.log.info(f"Auditando: {scenario_key}")
            stats = self.auditor.audit_scenario(scenario_path)
            if stats:
                self.reporter.generate_report(f"{folder_name}", stats)

if __name__ == "__main__":
    Orchestrator().run()