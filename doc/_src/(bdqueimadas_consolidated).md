# BDQueimadas Consolidation: análise técnica aprofundada

Arquivo: `src/consolidated_bdqueimadas.py`
Função: consolidar, por ano e multi-anos, alvos BDQueimadas a partir de dois insumos sincronizados no tempo: arquivo MANUAL `exportador_*_ref_YYYY.csv` e arquivo PROCESSADO `focos_br_ref_YYYY.csv`. A saída padroniza colunas textuais, unifica granularidade temporal na hora cheia e aplica junção 1:1 por chave composta hora+local, com filtro opcional por Bioma.
Convenções: trechos citados preservam semântica e numeração aproximada.

---

## Sumário

* [1. Modelagem do problema e hipóteses](#1-modelagem-do-problema-e-hipóteses)
* [2. Configuração, logging e paths canônicos](#2-configuração-logging-e-paths-canônicos)
* [3. Normalização de strings, encoding e datas](#3-normalização-de-strings-encoding-e-datas)
* [4. Descoberta de insumos por ano](#4-descoberta-de-insumos-por-ano)
* [5. Nomeação determinística de saídas](#5-nomeação-determinística-de-saídas)
* [6. Carregamento e saneamento do MANUAL](#6-carregamento-e-saneamento-do-manual)
* [7. Carregamento e saneamento do PROCESSADO](#7-carregamento-e-saneamento-do-processado)
* [8. Estratégia de matching 1-para-1](#8-estratégia-de-matching-1-para-1)
* [9. Materialização da saída e ordenação](#9-materialização-da-saída-e-ordenação)
* [10. Pipeline anual e política de sobrescrita](#10-pipeline-anual-e-política-de-sobrescrita)
* [11. Orquestração multi-anos e agregação all_years](#11-orquestração-multi-anos-e-agregação-all_years)
* [12. Propriedades formais e complexidade](#12-propriedades-formais-e-complexidade)
* [13. Observabilidade e métricas de qualidade](#13-observabilidade-e-métricas-de-qualidade)
* [14. Considerações de desempenho e memória](#14-considerações-de-desempenho-e-memória)
* [15. Segurança, integridade e contratos](#15-segurança-integridade-e-contratos)
* [16. Extensões recomendadas](#16-extensões-recomendadas)
* [17. Exemplos de uso](#17-exemplos-de-uso)
* [18. Anexo A. Invariantes e riscos conhecidos](#18-anexo-a-invariantes-e-riscos-conhecidos)

---

## 1. Modelagem do problema e hipóteses

O módulo realiza uma consolidação sem perda a partir de dois universos de dados do BDQueimadas com granularidades e esquemas distintos. O objetivo é produzir um dataset alvo com linhas horárias por localidade, contendo atributos textuais canônicos, medidas principais e identificadores quando presentes no PROCESSADO. Suposições centrais: o MANUAL e o PROCESSADO referem-se ao mesmo universo temporal, a coluna temporal de ambos é coerente e representa o mesmo fuso de referência, e a junção por hora cheia e chaves de localização normalizadas é suficiente para realizar um mapeamento m:1 do MANUAL ao PROCESSADO após deduplicação.

---

## 2. Configuração, logging e paths canônicos

```python
38  cfg = loadConfig()
39  log = get_logger("bdqueimadas.consolidate", kind="load", per_run_file=True)

44  RAW_BDQ_DIR  = Path(get_path("paths","data","raw")) / "BDQUEIMADAS"
45  PROC_BDQ_DIR = Path(get_path("paths","data","processed")) / "ID_BDQUEIMADAS"
46  OUT_DIR      = ensure_dir(Path(get_path("paths","data","external")) / "BDQUEIMADAS")
```

Leitura técnica:

* `get_logger(..., per_run_file=True)` provê trilha temporal por execução, apropriada para auditoria por ano e por fase.
* O layout separa insumos MANUAL e PROCESSADO por raízes distintas, e consolida artefatos em `external/BDQUEIMADAS`, isolando o produto final do espaço de staging.

---

## 3. Normalização de strings, encoding e datas

### Strings e caracteres de controle

```python
50  _CTRL_RE = re.compile(r"[\x00-\x1F\x7F-\x9F]")
51  _WS_RE   = re.compile(r"[\u00A0\u200B\u200C\u200D\uFEFF]")

53  def _strip_controls(x): ...
62  def _ascii_upper_no_diacritics(x):
68      s = unicodedata.normalize("NFKD", s)
69      s = "".join(ch for ch in s if not unicodedata.combining(ch))
71      return s.upper()
```

Racional:

* `_strip_controls` remove controles ASCII e whitespace invisível, mitigando quebras silenciosas de CSV e chaves.
* `_ascii_upper_no_diacritics` preserva letras, remove diacríticos e consolida múltiplos espaços antes de elevar a caixa alta, garantindo campos PAIS, ESTADO e MUNICIPIO visualmente padronizados.

### Encoding robusto

```python
74  def _read_csv_smart(path):
75      for enc in ("utf-8-sig","utf-8","latin1"):
77          return pd.read_csv(..., encoding=enc, on_bad_lines="skip")
```

O leitor tenta encodings comuns em cascata e ignora linhas malformadas, mantendo o fluxo. `low_memory=False` evita inferência de tipos por chunk que pode criar heterogeneidade de dtypes.

### Datas e granularidade temporal

```python
83  def _parse_manual_datetime(s): return pd.to_datetime(s, format="%Y/%m/%d %H:%M:%S", errors="coerce")
86  def _parse_proc_datetime(s):   return pd.to_datetime(s, format="%Y-%m-%d %H:%M:%S", errors="coerce")
89  def _floor_hour(ts):           return ts.dt.floor("h")
```

Ambos os insumos são convertidos a timestamps e reduzidos à hora cheia, eliminando minutos e segundos para compatibilizar granularidade no matching.

---

## 4. Descoberta de insumos por ano

```python
94  _MANUAL_FILE_RE = re.compile(r"^exportador_(\d{4}-\d{2}-\d{2})_ref_(\d{4})\.csv$", re.IGNORECASE)
95  def _year_from_manual(p): ...
98  def list_manual_year_files(raw_dir=RAW_BDQ_DIR) -> List[Tuple[int,Path]]:
100     for p in sorted(raw_dir.glob("exportador_*_ref_*.csv")):
101         y = _year_from_manual(p); ...
106  def processed_file_for_year(year, proc_root=PROC_BDQ_DIR) -> Optional[Path]:
107     subdir = proc_root / f"focos_br_ref_{year}"
108     p = subdir / f"focos_br_ref_{year}.csv"
```

Propriedades:

* Regex estrita captura o ano de referência a partir do sufixo `_ref_YYYY`.
* Em caso de múltiplos MANUAL por ano, a política `sorted(...)[-1]` na etapa anual seleciona o mais recente.

---

## 5. Nomeação determinística de saídas

```python
114 def _resolve_output_filename(years, biome, prefix="bdq_targets"):
124   if not years: return f"{prefix}_all_years{b}.csv"
128   if len(yrs)==1: return f"{prefix}_{yrs[0]}{b}.csv"
129   return f"{prefix}_{yrs[0]}_{yrs[-1]}{b}.csv"
```

A estratégia produz nomes estáveis e informativos, incorporando intervalo temporal e, quando aplicável, o sufixo do bioma em snake case.

---

## 6. Carregamento e saneamento do MANUAL

```python
136 MANUAL_DT_COL = "DataHora"
145 def load_manual(path, biome=None, validation=False) -> Tuple[pd.DataFrame,int]:
146   df = _read_csv_smart(path)
151   df["__DT"]   = _parse_manual_datetime(df[MANUAL_DT_COL])
152   df["__DT_H"] = _floor_hour(df["__DT"])
155   df["__PAIS_KEY"] = df["Pais"].map(normalize_key)
156   df["__UF_KEY"]   = df["Estado"].map(normalize_key)
157   df["__MUN_KEY"]  = df["Municipio"].map(normalize_key)
158   df["__BIO_KEY"]  = df["Bioma"].map(normalize_key) if "Bioma" in df.columns else ""
161   df["PAIS_OUT"]      = df["Pais"].map(_ascii_upper_no_diacritics)
162   df["ESTADO_OUT"]    = df["Estado"].map(_ascii_upper_no_diacritics)
163   df["MUNICIPIO_OUT"] = df["Municipio"].map(_ascii_upper_no_diacritics)
166   for c in ("Latitude","Longitude","FRP"): df[c] = pd.to_numeric(df[c], errors="coerce")
169   df, expected_rows = _maybe_filter_biome(df, biome)
173   df["__KEY"] = (df["__DT_H"].astype("int64").astype("string")+"|"
174                 +df["__PAIS_KEY"].astype(str)+"|"+df["__UF_KEY"].astype(str)+"|"+df["__MUN_KEY"].astype(str))
181   keep = ["__KEY","__DT_H","PAIS_OUT","ESTADO_OUT","MUNICIPIO_OUT","RiscoFogo","FRP"]
182   df = df[keep].copy()
```

Leitura técnica:

* Chaves de junção são produzidas com `normalize_key` para robustez a acentos e caixa.
* A chave temporal é a hora cheia convertida a int64, garantindo ordenação e unicidade por hora.
* O filtro por bioma, quando solicitado, atua no MANUAL antes da junção, reduzindo o custo do merge.

Validação:

* `validation=True` reduz tamanho para acelerar rotas de teste sem alterar a semântica do fluxo.

---

## 7. Carregamento e saneamento do PROCESSADO

```python
186 PROC_DT_COL = "data_pas"
188 def load_processed(path, restrict_pairs=None, validation=False) -> pd.DataFrame:
191   df["__DT"]   = _parse_proc_datetime(df[PROC_DT_COL])
192   df["__DT_H"] = _floor_hour(df["__DT"])
195   df["__PAIS_KEY"] = df["pais"].map(normalize_key)
196   df["__UF_KEY"]   = df["estado"].map(normalize_key)
197   df["__MUN_KEY"]  = df["municipio"].map(normalize_key)
200   df["__KEY"] = (df["__DT_H"].astype("int64").astype("string")+"|"
201                +df["__PAIS_KEY"].astype(str)+"|"+df["__UF_KEY"].astype(str)+"|"+df["__MUN_KEY"].astype(str))
205   keep = ["__KEY","foco_id","id_bdq","lat","lon"]
206   df = df[keep].copy()
210   df = df.drop_duplicates(subset="__KEY", keep="first")
```

Propriedades:

* O PROCESSADO é deduplicado por `__KEY` para respeitar o contrato de junção m:1. Mantém a primeira ocorrência, o que deve ser documentado como convenção.

---

## 8. Estratégia de matching 1-para-1

```python
217 def merge_manual_processed(df_m, df_p):
219   cols_p = ["__KEY","id_bdq","foco_id","lat","lon"]
220   merged = df_m.merge(df_p[cols_p], on="__KEY", how="left", validate="m:1", copy=False)
```

Semântica:

* Merge left garante preservação integral do MANUAL após filtro. `validate="m:1"` detecta violações se o PROCESSADO não for único por chave, protegendo a integridade.
* A cardinalidade efetiva é m:1 por desenho da deduplicação.

---

## 9. Materialização da saída e ordenação

```python
227 def build_output(merged):
228   out = pd.DataFrame({
229     "DATAHORA": merged["__DT_H"].dt.strftime("%Y-%m-%d %H:%M:%S"),
230     "PAIS": merged["PAIS_OUT"], "ESTADO": merged["ESTADO_OUT"], "MUNICIPIO": merged["MUNICIPIO_OUT"],
231     "RISCO_FOGO": merged["RiscoFogo"], "FRP": merged["FRP"],
232     "ID_BDQ": merged.get("id_bdq"), "FOCO_ID": merged.get("foco_id"),
233   })
234   out = out.sort_values(["DATAHORA","ESTADO","MUNICIPIO"], kind="stable").reset_index(drop=True)
```

A saída é canônica, com tipagem textual explícita para `DATAHORA` e ordenação estável por tempo e localização, favorecendo diffs e reprodutibilidade.

---

## 10. Pipeline anual e política de sobrescrita

```python
242 def consolidate_year(year, overwrite=False, validation=False, biome=None, encoding="utf-8") -> Optional[Path]:
243   manual_files = [p for (y,p) in list_manual_year_files(RAW_BDQ_DIR) if y==year]
244   proc_file = processed_file_for_year(year, PROC_BDQ_DIR)
250   manual_path = sorted(manual_files)[-1]
256   out_name = _resolve_output_filename([year], biome, prefix="bdq_targets")
257   out_path = OUT_DIR / out_name
258   if out_path.exists() and not overwrite: return out_path
261   df_m, expected_rows = load_manual(manual_path, biome=biome, validation=validation)
266   df_p = load_processed(proc_file, restrict_pairs=None, validation=validation)
269   merged = merge_manual_processed(df_m, df_p)
272   matched_rows   = int(merged["id_bdq"].notna().sum())
273   unmatched_rows = int((merged["id_bdq"].isna()).sum())
279   out_df = build_output(merged)
280   return write_output(out_df, out_path, encoding=encoding)
```

Fluxo:

1. Seleciona o MANUAL mais recente do ano e o PROCESSADO correspondente.
2. Respeita idempotência via `overwrite`.
3. Emite métricas de correspondência pós-merge antes de materializar.

---

## 11. Orquestração multi-anos e agregação all_years

```python
286 def run(years=None, overwrite=False, validation=False, biome=None, output_filename=None, encoding="utf-8") -> Optional[Path]:
287   years = sorted({int(y) for y in years}) if years else sorted({y for (y,_) in list_manual_year_files(RAW_BDQ_DIR)})
297   for y in years:
301       p = consolidate_year(...)
308   if len(outs)>1:
309       final_name = output_filename or _resolve_output_filename(years, biome, prefix="bdq_targets")
312       if final_path.exists() and not overwrite: return final_path
314       frames = [pd.read_csv(p, encoding=encoding, low_memory=False) for p in outs]
315       all_df = pd.concat(frames, ignore_index=True)
316       all_df.sort_values(["DATAHORA","ESTADO","MUNICIPIO"], kind="stable", inplace=True)
317       all_df.to_csv(final_path, index=False, encoding=encoding)
```

Comportamento:

* Descobre anos automaticamente quando `--years` é omitido, a partir da presença de MANUAL.
* Emite arquivo agregado `all_years` ou intervalo `Y1_YN` quando múltiplos anos foram produzidos.
* O agregado é construído por concatenação em memória. Ver seção 14 para recomendações de operação com muitos anos.

---

## 12. Propriedades formais e complexidade

* Determinismo: garantido por ordenações explícitas de anos e linhas e por convenção determinística de nomeação.
* Idempotência: respeitada ao checar existência de saídas e ao reusar MANUAL mais recente por ano. A escrita é atômica por contrato de `pandas.to_csv` no mesmo filesystem quando antecedida de `ensure_dir`.
* Complexidade:

  * Leitura por ano: O(Rm + Rp) linhas, com custo dominante na E S de CSV.
  * Merge m:1: O(N) sobre o MANUAL com hashing de `__KEY`.
  * Agregado multi-anos: O(Σ Ny) memória proporcional ao total de linhas ao concatenar em RAM.

---

## 13. Observabilidade e métricas de qualidade

Pontos de medição:

* Linhas lidas e tempo por insumo.
* Deduplicação no PROCESSADO com contagem de removidas.
* Filtro de bioma com total antes e depois.
* Pós-merge: `EXPECTED`, `RESULT len(merge)`, `MATCHED`, `UNMATCHED`.
* Escrita de arquivo com contagem de linhas.

Recomenda-se consolidar no rodapé por execução um resumo com anos processados, linhas totais, taxa de match e duração total.

---

## 14. Considerações de desempenho e memória

* Leitura robusta: `_read_csv_smart` é conveniente, mas `on_bad_lines="skip"` pode ocultar problemas sistêmicos. Considerar audit trail de linhas descartadas.
* Tipagem explícita: definir `dtype` para colunas numéricas e categóricas reduz memória e acelera merge.
* Agregado multi-anos: para grandes períodos, prefira escrita incremental por chunk ou `mode="a"` com header controlado, evitando `pd.concat` de muitos frames volumosos.
* Indexação: definir `__KEY` como índice antes do merge pode reduzir overhead interno em cenários muito grandes.
* Paralelismo: a consolidação anual é independente. Um executor de processos ou threads pode paralelizar por ano com controle de I O do disco.

---

## 15. Segurança, integridade e contratos

* Paths: construção de arquivos derivada de diretórios canônicos via `get_path` e `ensure_dir`, mitigando path traversal.
* Encoding: fallback para `latin1` evita abortos, porém exige atenção a perda de fidelidade em caracteres. A normalização de saída remove diacríticos por design para uso analítico.
* Contrato da chave: `__KEY` empacota hora cheia e localização normalizada. É imperativo que MANUAL e PROCESSADO usem o mesmo fuso de referência; caso contrário, ocorrerão mismatches sistemáticos.
* Unicidade no PROCESSADO: a deduplicação por `__KEY` define a primeira ocorrência como vencedora. Divergências devem ser tratadas a montante.

---

## 16. Extensões recomendadas

1. Validação de fuso
   Verificar alinhamento temporal por amostragem de chaves e emitir alerta se houver deslocamentos sistemáticos de 1 hora.

2. Checagem de esquemas
   Validar existência de colunas críticas em ambos insumos e emitir diffs estruturais por ano.

3. Join reforçado
   Expandir `__KEY` com centroids quantizados de lat lon ou com identificadores de satélite quando disponíveis, reduzindo colisões de hora+local.

4. Auditoria de linhas descartadas
   Registrar amostras de linhas ignoradas em `_read_csv_smart` e contagens por motivo.

5. Escrita incremental do agregado
   Gerar `all_years` por append com `chunksize` para reduzir picos de memória.

6. Manifesto por execução
   Persistir JSON com anos, tamanhos, tempos, taxas de match, hash das saídas e parâmetros de CLI.

7. Métrica de consistência espacial
   Confrontar `PAIS ESTADO MUNICIPIO` com `lat lon` quando disponíveis e rotular outliers.

---

## 17. Exemplos de uso

Consolidar 2012 e 2013, emitindo arquivos por ano:

```bash
python -m src.consolidated_bdqueimadas --years 2012 2013
```

Consolidar todos os anos com filtro por Cerrado e sobrescrever saídas existentes:

```bash
python -m src.consolidated_bdqueimadas --biome Cerrado --overwrite
```

Gerar agregado multi-anos com nome específico:

```bash
python -m src.consolidated_bdqueimadas --years 2010 2011 2012 --output-filename bdq_targets_2010_2012_cerrado.csv --biome Cerrado
```

Executar em modo de validação rápida:

```bash
python -m src.consolidated_bdqueimadas --validation
```

---

## 18. Anexo A. Invariantes e riscos conhecidos

Invariantes:

* A primeira coluna temporal de ambos insumos é convertida a hora cheia e concatenada com chaves normalizadas de localização para compor `__KEY`.
* O PROCESSADO é único por `__KEY` após `drop_duplicates`.
* O MANUAL, após filtro de bioma, é integralmente preservado pelo merge left.

Riscos e pontos de atenção:

* Fuso horário e horário de verão podem induzir erros de uma hora no matching se os insumos divergem no padrão de referência. Recomenda-se validação proativa.
* Colisões de `__KEY` podem ocorrer quando múltiplos eventos distintos compartilham a mesma hora e localidade ao nível de município. Nesses casos, a deduplicação no PROCESSADO seleciona uma única linha, reduzindo a cobertura de identificadores.
* `on_bad_lines="skip"` evita interrupções, mas pode mascarar violações de formato em lotes inteiros. Deve-se instrumentar contadores e amostras das linhas descartadas.
* A política de escolher o MANUAL mais recente por ano presume que arquivos mais novos são supersets ou correções. Em cenários onde versões têm recortes distintos, isso pode enviesar a consolidação.
