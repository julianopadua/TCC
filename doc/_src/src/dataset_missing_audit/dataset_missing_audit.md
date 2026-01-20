# 📄 Documentação Técnica – `src/dataset_missing_audit.py`

---

## 1. Visão geral e responsabilidade  

Este módulo realiza **auditoria de valores faltantes** nos arquivos consolidados  
`inmet_bdq_{ANO}_cerrado.csv` (dados do INMET + BDQueimadas, região do Cerrado).  
Para cada ano detectado ele:

1. **Conta** valores ausentes por coluna de *feature* (excluindo colunas de ID, data, texto e as colunas‑target).  
2. **Gera** um CSV `missing_by_column.csv` contendo as contagens e proporções de missing.  
3. **Produz** um `README_missing.md` que descreve o CSV e resume o ano.  

Os resultados são armazenados em `data/eda/dataset/{ANO}/`.

---

## 2. Posicionamento na arquitetura  

| Camada / Domínio | Papel |
|------------------|-------|
| **Data / EDA** (Exploratory Data Analysis) | Ferramenta de inspeção de qualidade de dados, utilizada antes de pipelines de modelagem. |
| **Utilitário interno** | Não expõe UI nem API externa; funciona como script de linha de comando e como biblioteca reutilizável por outros notebooks ou pipelines. |
| **Dependência de configuração** | Usa `utils.loadConfig` para obter parâmetros de caminho. |

---

## 3. Interfaces e exports  

| Nome | Tipo | Descrição |
|------|------|-----------|
| `YearMissingSummary` | `@dataclass` | Estrutura de resumo agregado por ano (total de linhas, foco, percentuais). |
| `DatasetMissingAnalyzer` | `@dataclass` | Classe principal que encapsula toda a lógica de auditoria. Métodos públicos: <br>• `discover_year_files()` <br>• `read_year_csv()` <br>• `run_per_year_audit(years: List[int] | None = None)` |
| `main()` | função | Entrypoint CLI (executado quando o módulo é chamado como script). |
| **Exportação implícita** | — | Ao importar o módulo, `YearMissingSummary`, `DatasetMissingAnalyzer` e `main` ficam disponíveis (`from dataset_missing_audit import DatasetMissingAnalyzer`). |

---

## 4. Dependências e acoplamentos  

| Origem | Tipo | Motivo |
|--------|------|--------|
| `__future__` | interno | Compatibilidade de annotations. |
| `dataclasses`, `pathlib`, `typing` | padrão Python | Estruturas de dados e tipagem. |
| `pandas` | externo | Manipulação de CSV e cálculo de missing. |
| `utils.loadConfig`, `utils.get_logger`, `utils.get_path`, `utils.ensure_dir` | interno (pacote `utils`) | Leitura de configuração, logging centralizado e criação de diretórios. |
| **Nenhum** | externo adicional | O módulo não depende de bibliotecas de visualização, machine‑learning ou de outros pacotes do projeto. |

> **Observação:** Não há importações de outros módulos do repositório; o arquivo não é consumido por nenhum outro componente (nenhum import externo registrado).

---

## 5. Leitura guiada do código (top‑down)  

### 5.1 Configuração e constantes  

```python
cfg = loadConfig()
PROJECT_ROOT = get_path("paths", "root")  # fallback para diretório do script
DATASET_DIR   = get_path("paths", "data", "dataset")
DATA_EDA_DIR  = ensure_dir(get_path("paths", "data", "eda"))
DATASET_EDA_DIR = ensure_dir(DATA_EDA_DIR / "dataset")
log = get_logger("eda.missing_dataset", kind="eda", per_run_file=True)

FILENAME_PATTERN = "inmet_bdq_*_cerrado.csv"
MISSING_CODES = {-999, -9999}
MISSING_CODES_STR = {str(v) for v in MISSING_CODES}
EXCLUDE_NON_NUMERIC = {...}   # colunas que não são analisadas
TARGET_COLS = {"RISCO_FOGO", "FRP", "FOCO_ID"}
```

*Invariantes*:  
- `DATASET_DIR` **deve** existir; caso contrário o módulo aborta com `FileNotFoundError`.  
- `MISSING_CODES` define valores numéricos que são tratados como ausentes em colunas numéricas.

### 5.2 Estruturas de dados  

```python
@dataclass
class YearMissingSummary:
    year: int
    rows_total: int
    focos_total: int
    nonfocos_total: int
    rows_with_any_missing: int
    rows_with_any_missing_focus: int
    rows_with_any_missing_nonfocus: int
    pct_rows_with_any_missing: float
    pct_rows_with_missing_focus: float
    pct_rows_with_missing_nonfocus: float
```

*Decisão*: reutilizar a mesma estrutura para dois tipos de resumo (global e por feature), preenchendo campos irrelevantes com zero.

### 5.3 Classe `DatasetMissingAnalyzer`

#### 5.3.1 Inicialização  

```python
@dataclass
class DatasetMissingAnalyzer:
    dataset_dir: Path
    eda_root_dir: Path
    file_pattern: str = FILENAME_PATTERN
    missing_codes: set[int] = field(default_factory=lambda: MISSING_CODES.copy())
    exclude: set[str] = field(default_factory=lambda: EXCLUDE_NON_NUMERIC.copy())

    def __post_init__(self) -> None:
        self.missing_codes_str = {str(v) for v in self.missing_codes}
```

*Decisão*: converte os códigos faltantes para `str` apenas uma vez, evitando recomputação em cada coluna de texto.

#### 5.3.2 Descoberta de arquivos  

```python
def discover_year_files(self) -> Dict[int, Path]:
    mapping = {}
    for fp in self.dataset_dir.glob(self.file_pattern):
        # extrai o primeiro token de 4 dígitos entre 1900‑2100
        ...
    return dict(sorted(mapping.items()))
```

*Invariante*: o padrão de nome contém exatamente um ano de quatro dígitos; caso contrário o arquivo é ignorado.

#### 5.3.3 Harmonização de colunas  

```python
def harmonize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
    # Unifica "RADIACAO GLOBAL (Kj/m²)" → "RADIACAO GLOBAL (KJ/m²)"
    ...
    return df
```

*Motivo*: garantir que colunas com grafia diferente entre anos sejam tratadas como a mesma feature.

#### 5.3.4 Construção da matriz de missing  

```python
def build_missing_matrix(self, df: pd.DataFrame) -> pd.DataFrame:
    missing = pd.DataFrame(index=df.index)
    for c in cols:
        s = df[c]
        if pd.api.types.is_bool_dtype(s):
            mask = s.isna()
        elif pd.api.types.is_numeric_dtype(s):
            mask = s.isna() | s.isin(self.missing_codes)
        else:
            s_str = s.astype("string")
            mask = s_str.isna() | s_str.str.strip().eq("") | s_str.isin(self.missing_codes_str)
        missing[c] = mask.fillna(False)
    return missing
```

*Regra de missing*: NaN, strings vazias (após `strip`) e os códigos especiais definidos.

#### 5.3.5 Resumo por ano (global)  

```python
def compute_year_summary(self, df: pd.DataFrame, year: int) -> YearMissingSummary:
    missing = self.build_missing_matrix(df)
    any_missing = missing.any(axis=1)
    foco_mask = df["HAS_FOCO"] == 1
    ...
    return YearMissingSummary(...)
```

*Dependência crítica*: a coluna `HAS_FOCO` **deve** existir; caso contrário levanta `KeyError`.

#### 5.3.6 Breakdown por coluna (features)  

```python
def compute_feature_breakdown_for_year(self, df: pd.DataFrame, year: int) -> tuple[pd.DataFrame, YearMissingSummary]:
    missing = self.build_missing_matrix(df)
    foco_mask = df["HAS_FOCO"] == 1
    for c in missing.columns:
        if c in TARGET_COLS or c == "HAS_FOCO":
            continue
        ...
    feature_df = pd.DataFrame(records).sort_values("pct_missing_total", ascending=False)
    summary = YearMissingSummary(..., rows_with_any_missing=..., rows_with_any_missing_focus=..., rows_with_any_missing_nonfocus=0, ...)
    return feature_df, summary
```

*Observação*: o `summary` retornado contém campos de contagem geral preenchidos apenas para reutilização posterior; valores de proporção são zero porque não são usados aqui.

#### 5.3.7 Escrita do README  

```python
def write_year_readme(self, year_dir: Path, year: int, summary: YearMissingSummary,
                      feature_df: pd.DataFrame, csv_name: str) -> None:
    # Monta texto Markdown com top‑5 colunas mais ausentes
    ...
    readme_path.write_text("\n".join(lines), encoding="utf-8")
```

*Decisão de UI*: o README é auto‑explicativo e não depende de parâmetros externos.

#### 5.3.8 Pipeline principal  

```python
def run_per_year_audit(self, years: List[int] | None = None) -> None:
    year_files = self.discover_year_files()
    if years is not None:
        year_files = {y: fp for y, fp in year_files.items() if y in years}
    for year, fp in year_files.items():
        df = self.read_year_csv(fp)
        feature_df, summary = self.compute_feature_breakdown_for_year(df, year)
        year_dir = ensure_dir(self.eda_root_dir / str(year))
        csv_path = year_dir / "missing_by_column.csv"
        feature_df.to_csv(csv_path, index=False, encoding="utf-8")
        self.write_year_readme(year_dir, year, summary, feature_df, "missing_by_column.csv")
```

*Fluxo*: descoberta → leitura → cálculo → persistência CSV → geração de README.

### 5.4 Interface de linha de comando  

```python
def main() -> None:
    parser = argparse.ArgumentParser(...)
    parser.add_argument("--pattern", default=FILENAME_PATTERN, ...)
    parser.add_argument("--years", nargs="+", type=int, ...)
    args = parser.parse_args()
    analyzer = DatasetMissingAnalyzer(dataset_dir=DATASET_DIR,
                                      eda_root_dir=DATASET_EDA_DIR,
                                      file_pattern=args.pattern)
    analyzer.run_per_year_audit(years=args.years)
```

*Comportamento*: permite sobrescrever o padrão de nome de arquivo e limitar a auditoria a um subconjunto de anos.

---

## 6. Fluxo de dados / estado / eventos  

1. **Entrada**: arquivos CSV `inmet_bdq_{ANO}_cerrado.csv` em `data/dataset/`.  
2. **Transformação**:  
   - *Harmonização* de nomes de colunas.  
   - *Construção* da matriz booleana `missing` (linha × coluna).  
   - *Agregação* de contagens e proporções por coluna e por ano.  
3. **Saída** (por ano):  
   - `missing_by_column.csv` (dados tabulares).  
   - `README_missing.md` (documentação Markdown).  
4. **Estado interno**: objetos `DatasetMissingAnalyzer` mantêm configurações (códigos missing, colunas excluídas) e caches leves (`missing_codes_str`).  
5. **Eventos de logging**: cada etapa registra mensagens via `log.info`/`log.warning`, facilitando auditoria de execução.

---

## 7. Conexões com outros arquivos do projeto  

| Arquivo | Tipo de relação | Comentário |
|---------|----------------|------------|
| `utils.py` | **dependência** (importa `loadConfig`, `get_logger`, `get_path`, `ensure_dir`) | Fornece configuração centralizada, logger e utilitários de caminho. |
| `data/eda/...` | **destino** (escrita) | Diretório onde os resultados são armazenados. |
| Nenhum outro módulo importa este arquivo (conforme metadados do repositório). |

*Links* (não disponíveis no prompt) seriam inseridos como `[utils](../utils.md)` etc., caso a documentação externa exista.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Área | Risco / Limitação | Recomendações |
|------|-------------------|---------------|
| **Dependência de colunas específicas** | O código aborta se `HAS_FOCO` ou colunas de data/horário não existirem. | Tornar a lista de colunas obrigatórias configurável; gerar aviso em vez de exceção quando ausentes. |
| **Hard‑coded missing codes** | Apenas `-999` e `-9999` são tratados; outros valores (e.g., `9999`) podem ser usados em datasets futuros. | Expor `missing_codes` como parâmetro de CLI ou via configuração (`config.yaml`). |
| **Repetição de cálculo de missing matrix** | `compute_year_summary` e `compute_feature_breakdown_for_year` chamam `self.build_missing_matrix(df)` separadamente, duplicando trabalho. | Calcular a matriz uma única vez e reutilizar nos dois métodos (passar como argumento ou armazenar em atributo temporário). |
| **Escalabilidade** | Leitura completa de CSV em memória pode falhar para arquivos muito grandes. | Avaliar uso de `pandas.read_csv(..., chunksize=…)` ou `dask` para processamento em lote. |
| **Harmonização de colunas** | Atualmente só trata a radiação global; outras divergências de nome podem surgir. | Implementar um mapeamento genérico (ex.: dicionário `COLUMN_ALIASES`) carregado de configuração. |
| **Teste unitário** | Não há cobertura de testes automatizados. | Criar testes para: (a) extração de ano, (b) detecção de missing em tipos diferentes, (c) geração de README. |
| **Internacionalização** | Mensagens de log e README estão em português; pode ser necessário suporte a outros idiomas. | Parametrizar idioma via configuração. |
| **CLI** | Não há opção `--output-dir` para sobrescrever o diretório de destino. | Adicionar argumento CLI para flexibilidade de caminho de saída. |

--- 

*Esta documentação segue as diretrizes solicitadas: escrita em pt‑BR, tom técnico, uso de Markdown estruturado, e inclui apenas trechos de código essenciais.*
