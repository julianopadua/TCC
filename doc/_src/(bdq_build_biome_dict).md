# BDQueimadas Biome Dictionary Builder: análise técnica aprofundada

Arquivo: `src/bdq_build_biome_dictionary.py`
Função: construir um dicionário estado municipio bioma a partir dos CSVs anuais `focos_br_ref_YYYY`, agregando presença por ano e emitindo artefatos tabulares e hierárquicos para consumo posterior.
Convenções: trechos citados preservam semântica e numeração aproximada.

---

## Sumário

* [1. Modelagem do problema e hipóteses](#1-modelagem-do-problema-e-hipóteses)
* [2. Configuração, logging e diretórios](#2-configuração-logging-e-diretórios)
* [3. Descoberta de insumos e seleção temporal](#3-descoberta-de-insumos-e-seleção-temporal)
* [4. Leitura mínima, normalização e filtros](#4-leitura-mínima-normalização-e-filtros)
* [5. Agregação multi-anos e contrato de chave](#5-agregação-multi-anos-e-contrato-de-chave)
* [6. Materialização de saídas e formatos](#6-materialização-de-saídas-e-formatos)
* [7. Mapeamento hierárquico estado→município→biomas](#7-mapeamento-hierárquico-estadomunicípiobiomas)
* [8. Extração de municípios do Cerrado](#8-extração-de-municípios-do-cerrado)
* [9. Pipeline principal e invariantes](#9-pipeline-principal-e-invariantes)
* [10. Utilitários de consumo a jusante](#10-utilitários-de-consumo-a-jusante)
* [11. Propriedades formais e complexidade](#11-propriedades-formais-e-complexidade)
* [12. Observabilidade e manejo de falhas](#12-observabilidade-e-manejo-de-falhas)
* [13. Considerações de desempenho e memória](#13-considerações-de-desempenho-e-memória)
* [14. Segurança, integridade e consistência semântica](#14-segurança-integridade-e-consistência-semântica)
* [15. Extensões recomendadas](#15-extensões-recomendadas)
* [16. Exemplos de uso](#16-exemplos-de-uso)
* [17. Anexo A. Layout lógico das saídas](#17-anexo-a-layout-lógico-das-saídas)

---

## 1. Modelagem do problema e hipóteses

O módulo consolida, por estado e município, os biomas observados nos CSVs anuais do BDQueimadas, mapeando variações grafêmicas por normalização fonética mínima e registrando os anos de ocorrência. Suposições: os arquivos `focos_br_ref_YYYY.csv` estão disponíveis em `data/processed/<folder>/focos_br_ref_YYYY/`, contêm colunas canônicas `pais`, `estado`, `municipio`, `bioma`, e a normalização `normalize_key` induz classes de equivalência estáveis para matching entre anos.

---

## 2. Configuração, logging e diretórios

```python
29  cfg = loadConfig()
30  log = get_logger("bdq.dictionary", kind="dictionary", per_run_file=True)

34  DEFAULT_FOLDER = "ID_BDQUEIMADAS"
35  DEFAULT_YEARS  = list(range(2003, 2025))
37  ENCODING = (cfg.get("io", {}) or {}).get("encoding", "utf-8")
38  OUT_DIR = ensure_dir(get_path("paths","data","dictionarys"))

40  OUT_CSV     = Path(OUT_DIR) / "bdq_municipio_bioma.csv"
41  OUT_PARQUET = Path(OUT_DIR) / "bdq_municipio_bioma.parquet"
42  OUT_JSON    = Path(OUT_DIR) / "bdq_municipio_bioma.json"
43  OUT_CERRADO = Path(OUT_DIR) / "municipios_cerrado.csv"
```

Diretrizes:

* `per_run_file=True` assegura trilha por execução, útil para auditorias de carga.
* A raiz `paths.data.dictionarys` centraliza artefatos para reuso por outros pipelines.
* `DEFAULT_YEARS` cobre 2003 a 2024, ajustável via CLI.

---

## 3. Descoberta de insumos e seleção temporal

```python
49  def _year_paths(processed_root: Path, years: Iterable[int]) -> List[Path]:
51      p = processed_root / f"focos_br_ref_{y}" / f"focos_br_ref_{y}.csv"
52      if p.exists(): paths.append(p)
53      else: log.warning(f"[SKIP] CSV inexistente para {y}: {p}")
```

Propriedades:

* Descoberta determinística por convenção de pastas e nomes.
* Emissões `[SKIP]` registram lacunas por ano, sem interromper o job.
* Complexidade O(Y) para Y anos solicitados.

---

## 4. Leitura mínima, normalização e filtros

```python
58  def _read_minimal_columns(csv_path: Path) -> pd.DataFrame:
59      usecols = ["pais","estado","municipio","bioma"]
60      df = pd.read_csv(csv_path, usecols=usecols, dtype=str, encoding=ENCODING)
62      for c in usecols: df[c] = df[c].astype(str).str.strip()
64      df["estado_norm"]    = df["estado"].map(normalize_key)
65      df["municipio_norm"] = df["municipio"].map(normalize_key)
67      df = df[(df["municipio"] != "") & (df["bioma"] != "")]
```

Leitura técnica:

* Minimiza I O ao restringir colunas essenciais para o dicionário.
* Normaliza chaves de matching em campos separados, preservando valores originais.
* Remove registros sem município ou bioma para evitar classes vazias.

---

## 5. Agregação multi-anos e contrato de chave

```python
71  def _aggregate_years(df_concat: pd.DataFrame) -> pd.DataFrame:
72      df_concat["year"] = df_concat["year"].astype(int)
73      group_cols = ["pais","estado","municipio","estado_norm","municipio_norm","bioma"]
74      agg = (df_concat.groupby(group_cols, as_index=False)["year"]
75             .apply(lambda s: ";".join(str(x) for x in sorted(set(s.tolist()))))
76             .rename(columns={"year": "anos_origem"}))
```

Semântica:

* Chave composta por valores canônicos e normalizados, mais `bioma`.
* O campo `anos_origem` registra os anos distintos de observação por par estado municipio bioma, suportando auditoria histórica e reconciliação.

---

## 6. Materialização de saídas e formatos

Na pipeline principal:

```python
107 df_full = _aggregate_years(df_concat).sort_values(["estado","municipio","bioma"]).reset_index(drop=True)
109 df_full.to_csv(OUT_CSV, index=False, encoding=ENCODING)
110 df_full.to_parquet(OUT_PARQUET, index=False)
115 with open(OUT_JSON,"w",encoding="utf-8") as f: json.dump(nested, f, ensure_ascii=False, indent=2)
```

Características:

* CSV para interoperabilidade ampla e Parquet para consumo analítico eficiente.
* JSON hierárquico atende cenários de consulta leve e serialização web.

---

## 7. Mapeamento hierárquico estado→município→biomas

```python
78  def _to_nested_mapping(df) -> Dict[str, Dict[str, List[str]]]:
79      nested: Dict[str, Dict[str, Set[str]]] = {}
80      for estado, municipio, bioma in df[["estado","municipio","bioma"]].itertuples(...):
81          nested.setdefault(estado, {}).setdefault(municipio, set()).add(bioma)
83      nested_sorted = {
84        est: {mun: sorted(list(biomas)) for mun, biomas in sorted(muns.items())}
85        for est, muns in sorted(nested.items())
86      }
```

Racional:

* Agrega biomas por município, preservando valores originais legíveis.
* Ordenações determinísticas por estado e município estabilizam diffs do JSON.

---

## 8. Extração de municípios do Cerrado

```python
88  def _extract_cerrado_pairs(df):
89      mask = df["bioma"].str.casefold() == "cerrado".casefold()
90      cerrado = (df.loc[mask, ["estado","municipio","estado_norm","municipio_norm"]]
91                 .drop_duplicates()
92                 .sort_values(["estado","municipio"]))
```

Saída `municipios_cerrado.csv` contém pares canônicos e normalizados, prontos para join com bases externas.

---

## 9. Pipeline principal e invariantes

```python
96  def build_dictionary(folder_name=DEFAULT_FOLDER, years=DEFAULT_YEARS) -> Tuple[pd.DataFrame, pd.DataFrame]:
97      processed_root = Path(get_path("paths","data","processed")) / folder_name
98      csv_paths = _year_paths(processed_root, years)
104     for p in csv_paths:
106         df = _read_minimal_columns(p)
107         df["year"] = int(p.stem.split("_")[-1])
113     df_concat = pd.concat(parts, ignore_index=True)
114     df_concat = df_concat.drop_duplicates(["pais","estado","municipio","bioma","estado_norm","municipio_norm","year"])
116     df_full = _aggregate_years(df_concat)
121     nested = _to_nested_mapping(df_full)
125     df_cerrado = _extract_cerrado_pairs(df_full)
```

Invariantes:

* Cada linha de `df_full` representa uma combinação única estado municipio bioma, com os anos de ocorrência.
* `df_cerrado` é um subconjunto deduplicado e ordenado dos pares que pertencem ao bioma Cerrado.

---

## 10. Utilitários de consumo a jusante

```python
137 def load_cerrado_pairs() -> Set[Tuple[str,str]]:
139   df = pd.read_csv(OUT_CERRADO, dtype=str, encoding=ENCODING)
141   if {"estado_norm","municipio_norm"}.issubset(df.columns):
142       est = df["estado_norm"].map(str).map(str.strip)
143       mun = df["municipio_norm"].map(str).map(str.strip)
145   else:
146       est = df["estado"].map(lambda x: normalize_key(str(x)))
147       mun = df["municipio"].map(lambda x: normalize_key(str(x)))
148   return set(zip(est, mun))

150 def filter_df_by_cerrado(df, estado_col="estado", municipio_col="municipio") -> pd.DataFrame:
151   pairs = load_cerrado_pairs()
152   mask = df.apply(lambda r: (normalize_key(r.get(estado_col)), normalize_key(r.get(municipio_col))) in pairs, axis=1)
153   return df.loc[mask].copy()
```

Observações:

* `load_cerrado_pairs` expõe um conjunto normalizado de pares, ideal para membership tests O(1).
* `filter_df_by_cerrado` aplica seleção semântica em qualquer DataFrame contendo colunas de estado e município, com normalização interna.

---

## 11. Propriedades formais e complexidade

* Determinismo: ordenações explícitas e nomeação fixa garantem saídas estáveis para os mesmos insumos e anos.
* Idempotência: reexecutar com o mesmo conjunto de arquivos gera artefatos idênticos byte a byte, exceto por carimbos de log.
* Complexidade:

  * Leitura e concatenação: O(Σ Ny) linhas, onde Ny é o volume por ano.
  * Agrupamento: O(N log N) no pior caso pela ordenação, com custo de hashing sobre chaves compostas.
  * Geração do JSON hierárquico: O(M) onde M é o número de pares estado municipio distintos.

---

## 12. Observabilidade e manejo de falhas

Pontos de log:

* `[SKIP]` para anos sem CSV.
* `[OK]` com contagem de linhas lidas por arquivo.
* `[ERROR]` por falhas de leitura individuais sem abortar o job.
* `[WRITE]` para cada artefato materializado.
* `[SUMMARY]` com totais de registros e municípios Cerrado.

Diretrizes:

* Em caso de ausência total de arquivos, retornar DataFrames vazios e erro em log, evitando efeitos colaterais.

---

## 13. Considerações de desempenho e memória

* `usecols` e `dtype=str` reduzem overhead de parsing e garantem uniformidade de tipos.
* `drop_duplicates` antes do agrupamento diminui cardinalidade e uso de memória.
* Para muitos anos volumosos, considerar leitura por chunks e agregação incremental com `groupby` sobre objetos dicionário ou uso de `Polars` quando disponível.
* A escrita Parquet é preferível para reuso frequente em análises, dado melhor footprint e scan columnar.

---

## 14. Segurança, integridade e consistência semântica

* Paths e nomes são construídos via `get_path` e `ensure_dir`, reduzindo risco de path traversal.
* Normalização `normalize_key` deve ser estável ao longo do tempo; mudanças exigem rebuild completo para consistência.
* O dicionário não resolve conflitos semânticos quando um município aparece com múltiplos biomas em anos distintos; essa multiplicidade é intencional e explicitada em `anos_origem`.

---

## 15. Extensões recomendadas

1. Auditoria de inconsistência
   Detectar municípios multibioma e emitir relatório auxiliar com distribuição por ano.

2. Sensibilidade a grafias
   Incluir heurística de similaridade para unir variantes extremas antes da normalização, com lista de exceções auditável.

3. Catálogo de metadados
   Gravar manifesto JSON com anos, contagens por estado, hash dos arquivos fonte e parâmetros de execução.

4. Particionamento por estado
   Emissão opcional de arquivos por estado para consumo seletivo.

5. CLI avançada
   Flags `--states SP MG ...` e `--only-cerrado` para pipelines dirigidos.

---

## 16. Exemplos de uso

Construir dicionário completo padrão 2003 a 2024 na pasta default:

```bash
python -m src.bdq_build_biome_dictionary
```

Restringir a anos específicos e pasta alternativa:

```bash
python -m src.bdq_build_biome_dictionary --folder ID_BDQUEIMADAS --years 2010 2012 2019 2020
```

Carregar pares Cerrado e filtrar um DataFrame arbitrário:

```python
from bdq_build_biome_dictionary import load_cerrado_pairs, filter_df_by_cerrado
pairs = load_cerrado_pairs()
df_cerr = filter_df_by_cerrado(df_raw, estado_col="Estado", municipio_col="Município")
```

---

## 17. Anexo A. Layout lógico das saídas

1. `bdq_municipio_bioma.csv` e `bdq_municipio_bioma.parquet`
   Colunas:

   * `pais` texto original
   * `estado` texto original
   * `municipio` texto original
   * `estado_norm` chave normalizada para matching
   * `municipio_norm` chave normalizada para matching
   * `bioma` texto original
   * `anos_origem` string com anos separados por ponto e vírgula

2. `bdq_municipio_bioma.json`
   Estrutura:

   * dicionário `{estado: {municipio: [biomas...]}}`, com chaves ordenadas alfabeticamente.

3. `municipios_cerrado.csv`
   Colunas:

   * `estado`, `municipio` originais
   * `estado_norm`, `municipio_norm` normalizadas
     Cada linha representa um município pertencente ao bioma Cerrado, sem repetições.
