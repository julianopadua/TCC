# build_dataset_hourly.py – Documentação Técnica Interna  

---  

## 1. Visão geral e responsabilidade  

Este módulo gera o **dataset hora‑a‑hora** que combina as medições climáticas do **INMET** com os focos de queimada do **BDQUEIMADAS (BDQ)**.  
Para cada município (normalizado) e hora do dia, produz‑se:  

* arquivos anuais `data/dataset/inmet_bdq_{YYYY}_{biome}.csv` (a partir de 2003);  
* um arquivo consolidado `data/dataset/inmet_bdq_all_years_{biome}.csv`.  

O processo inclui: descoberta de anos disponíveis, leitura e normalização de ambas as fontes, junção por município + timestamp, e gravação dos resultados.

---

## 2. Posicionamento na arquitetura  

| Camada | Descrição |
|--------|-----------|
| **Utilitário / Dados** | O script opera na camada de preparação de dados, sem interação direta com a camada de modelo ou UI. |
| **Domínio** | Relaciona‑se ao domínio *climatologia × queimadas* (bioma). |
| **Interface de linha de comando** | Possui um `if __name__ == "__main__"` que permite execução direta via CLI. |

---

## 3. Interfaces e exports  

| Export | Tipo | Descrição |
|--------|------|-----------|
| `build_hourly_dataset` | função | API pública. Constrói o dataset para um bioma e conjunto de anos, retornando `(anos_processados, caminhos_por_ano, caminho_consolidado)`. |
| `__main__` | bloco CLI | Permite invocação direta com argumentos `--biome`, `--years`, `--overwrite`, `--encoding`. |

Nenhum outro nome é exportado (`__all__` não definido, mas apenas as duas entidades acima são de interesse externo).

---

## 4. Dependências e acoplamentos  

### Bibliotecas externas  
* `pandas` – manipulação de DataFrames.  
* `pathlib`, `re`, `typing` – utilitários padrão.  

### Módulos internos (utils)  
```python
from utils import (
    loadConfig,
    get_logger,
    get_path,
    ensure_dir,
    normalize_key,
)
```
* **loadConfig** – carrega configuração global (efeito colateral).  
* **get_logger** – cria logger nomeado `dataset.build`.  
* **get_path** – resolve diretórios a partir de um arquivo de configuração.  
* **ensure_dir** – cria diretório de saída caso inexistente.  
* **normalize_key** – normaliza strings de municípios (ex.: remoção de acentos, caixa‑baixa).  

Não há dependências circulares conhecidas; o módulo é autônomo exceto pelos utilitários acima.

---

## 5. Leitura guiada do código (top‑down)  

### 5.1. Constantes e expressões regulares  

```python
_INMET_RE = re.compile(r"^inmet_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)
_BDQ_RE   = re.compile(r"^bdq_targets_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)
```
* Usadas para extrair **ano** e **bioma** dos nomes de arquivos consolidados.  

### 5.2. Descoberta de arquivos  

* `_list_inmet_years_for_biome(biome)` e `_list_bdq_years_for_biome(biome)` percorrem os diretórios `data/consolidated/INMET` e `.../BDQUEIMADAS`, retornando listas de anos disponíveis.  

### 5.3. Detecção de esquema de data/hora (INMET)  

```python
def _detect_inmet_schema(df):
    has_new = (DATE_COL_NEW in df.columns) and (HOUR_COL_NEW in df.columns)
    has_old = (DATE_COL_OLD in df.columns) and (HOUR_COL_OLD in df.columns)
    ...
```
* Garante compatibilidade com arquivos pré‑2019 (`DATA (YYYY-MM-DD)`, `HORA (UTC)`) e pós‑2019 (`Data`, `Hora UTC`).  

### 5.4. Normalização de hora  

* `_parse_hour_to_hh` aceita formatos como `"0100 UTC"`, `"7"`, `"07:00"` e devolve um inteiro 0‑23.  

### 5.5. Construção de timestamps  

* `_build_ts_from_inmet_row` gera a string `YYYY-MM-DD HH:00:00` usando a coluna de data e a hora normalizada.  
* `_build_ts_from_bdq` converte a coluna `DATAHORA` (formato completo) para o mesmo padrão horário.  

### 5.6. Leitura de dados por ano  

* `_read_inmet_year` lê o CSV do INMET como **strings**, normaliza a coluna `CIDADE` (`cidade_norm`) e cria `ts_hour`.  
* `_read_bdq_year_reduced` lê o CSV de BDQ, normaliza `MUNICIPIO` (`municipio_norm`), cria `ts_hour` e **reduz** múltiplos focos ao de maior `FRP` por município‑hora.  

### 5.7. Fusão anual  

```python
merged = inmet.merge(
    bdq,
    left_on=["cidade_norm", "ts_hour"],
    right_on=["municipio_norm", "ts_hour"],
    how="left",
    suffixes=("", "_bdq"),
)
merged["HAS_FOCO"] = merged["FOCO_ID"].notna().astype("int64")
```
* `left` join garante que todas as linhas do INMET sejam preservadas.  
* Campo `HAS_FOCO` indica presença de foco (0/1).  

### 5.8. Pipeline principal – `build_hourly_dataset`  

1. **Carrega configuração** (`loadConfig`).  
2. **Descobre anos comuns** entre INMET e BDQ (≥ 2003).  
3. **Filtra** pelos anos solicitados (argumento `years`).  
4. **Itera** sobre cada ano:  
   * Se o arquivo de saída já existir e `overwrite=False`, pula.  
   * Caso contrário, chama `_fuse_inmet_bdq_year` e grava CSV anual.  
5. **Consolida** todos os arquivos anuais em `inmet_bdq_all_years_{biome}.csv` (sobrescreve se necessário).  
6. **Retorna** a lista de anos processados, caminhos dos arquivos anuais e o caminho consolidado.  

### 5.9. Interface de linha de comando  

O bloco `if __name__ == "__main__":` cria um parser `argparse` que repassa os argumentos para `build_hourly_dataset` e registra o progresso via logger.

---

## 6. Fluxo de dados / estado  

```
[Config] → get_path → diretórios (INMET, BDQ, dataset)
   │
   ├─> _list_*_years_for_biome → conjunto de anos disponíveis
   │
   └─> build_hourly_dataset
          │
          ├─> _read_inmet_year (df_inmet)
          │        └─> normalize_key → cidade_norm
          │        └─> _detect_inmet_schema → colunas de data/hora
          │        └─> _build_ts_from_inmet_row → ts_hour
          │
          ├─> _read_bdq_year_reduced (df_bdq)
          │        └─> normalize_key → municipio_norm
          │        └─> _build_ts_from_bdq → ts_hour
          │        └─> groupby max FRP → linha única por município‑hora
          │
          └─> merge (left) → df_merged
                 └─> HAS_FOCO, RISCO_FOGO, FRP, FOCO_ID
                 └─> grava CSV anual
```

O estado interno é mantido apenas em DataFrames temporários; não há efeitos colaterais além da escrita de arquivos.

---

## 7. Conexões com outros arquivos do projeto  

* **utils.py** – fornece funções de configuração, logging e normalização usadas em todo o código‑base.  
* **Nenhum outro módulo importa este script** (não há dependências reversas).  

> **Observação:** Caso existam módulos que consumam os CSV gerados (`inmet_bdq_*.csv`), eles deverão ser atualizados para reconhecer a coluna `ts_hour` no formato `YYYY-MM-DD HH:00:00`.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Descrição | Recomendações |
|------|-----------|---------------|
| **Validação de esquema** | `_detect_inmet_schema` lança `KeyError` se ambas as combinações de colunas estiverem ausentes. | Documentar explicitamente o requisito de colunas nos arquivos de origem; considerar fallback para tentativa de inferência automática (ex.: buscar padrões de data). |
| **Performance de `apply`** | `_read_inmet_year` usa `df.apply(..., axis=1)` para gerar `ts_hour`. Em arquivos grandes isso pode ser custoso. | Substituir por vetorização usando `pd.to_datetime` e `Series.str.extract` quando possível. |
| **Uso de `attrs`** | As colunas de data/hora são armazenadas em `df.attrs`. Essa informação pode ser perdida ao salvar/recuperar CSV. | Avaliar a necessidade de persistir essa metadata ou recalculá‑la na leitura posterior. |
| **Gerenciamento de memória** | A consolidação final lê todos os CSV anuais em memória (`pd.concat`). | Para biomas com muitos anos, usar `chunksize` ou `pd.concat` incremental com escrita em modo append. |
| **Tratamento de timezone** | Timestamps são tratados como *naive* (sem fuso). | Verificar se a ausência de timezone pode gerar inconsistências ao cruzar com outras fontes que utilizam UTC. |
| **Testes unitários** | Não há cobertura de teste explícita. | Implementar testes para: (i) detecção de esquema, (ii) parsing de hora, (iii) redução de BDQ por FRP, (iv) fusão correta de chaves. |
| **CLI – mensagens de erro** | Exceções são capturadas e logadas, mas o processo termina com código de saída 0. | Propagar `sys.exit(1)` em caso de falha para sinalizar erro ao usuário/CI. |
| **Documentação de parâmetros** | O parâmetro `biome` aceita qualquer string, mas os arquivos disponíveis são limitados. | Validar contra lista de biomas suportados (ex.: `["cerrado", "amazonia", ...]`). |

---  

*Esta documentação deve ser mantida sincronizada com alterações de código e com a configuração de caminhos em `utils.py`.*
