# INMET Consolidated: análise técnica aprofundada

Arquivo: `src/inmet_consolidated.py`
Função: consolidar incrementalmente arquivos anuais `processed/INMET/inmet_{YYYY}.csv` em `consolidated/INMET`, com suporte a filtro por bioma, normalização de data e remoção de sentinelas.
Convenções: trechos citados mantêm numeração aproximada e semântica original.

---

## Sumário

* [1. Modelagem do problema e hipóteses](#1-modelagem-do-problema-e-hipóteses)
* [2. Mapeamento de paths e inicialização](#2-mapeamento-de-paths-e-inicialização)
* [3. Descoberta e ordenação dos insumos](#3-descoberta-e-ordenação-dos-insumos)
* [4. Primitivas auxiliares de leitura e escrita](#4-primitivas-auxiliares-de-leitura-e-escrita)
* [5. Normalização de datas e política de sentinelas](#5-normalização-de-datas-e-política-de-sentinelas)
* [6. Resolução determinística de nomes de saída](#6-resolução-determinística-de-nomes-de-saída)
* [7. Filtro por bioma via dicionário BDQueimadas](#7-filtro-por-bioma-via-dicionário-bdqueimadas)
* [8. Consolidação por modo: split, combine e both](#8-consolidação-por-modo-split-combine-e-both)
* [9. Propriedades formais: determinismo, idempotência e complexidade](#9-propriedades-formais-determinismo-idempotência-e-complexidade)
* [10. Observabilidade e manejo de falhas](#10-observabilidade-e-manejo-de-falhas)
* [11. Considerações de desempenho e I O](#11-considerações-de-desempenho-e-i-o)
* [12. Segurança, integridade e contratos de esquema](#12-segurança-integridade-e-contratos-de-esquema)
* [13. Extensões recomendadas](#13-extensões-recomendadas)
* [14. Exemplos de uso](#14-exemplos-de-uso)

---

## 1. Modelagem do problema e hipóteses

O módulo materializa uma etapa L dos dados do INMET: transforma insumos anuais já processados em artefatos consolidados prontos para consumo analítico, com três capacidades nucleares: seleção temporal, filtragem semântica por bioma e saneamento estrutural. O pipeline é offline e orientado a arquivos, preservando o schema original dos CSVs e a ordem de colunas. Suposições: os arquivos `processed/INMET/inmet_{YYYY}.csv` existem e são válidos, a primeira coluna é uma data possivelmente no formato `YYYY/MM/DD`, a coluna de município existe com nome configurável, e o dicionário BDQueimadas fornece o mapeamento município para bioma.

---

## 2. Mapeamento de paths e inicialização

```python
27  def get_inmet_processed_dir() -> Path: return get_path("paths","providers","inmet","processed")
30  def get_consolidated_root() -> Path: return get_path("paths","data","external")
33  def get_inmet_consolidated_dir() -> Path: return ensure_dir(Path(get_consolidated_root())/"INMET")
36  def get_dictionary_csv_path() -> Path: return Path(get_path("paths","data","dictionarys"))/"bdq_municipio_bioma.csv"
```

Leitura técnica:

* A consolidação sempre escreve sob `external/INMET` por design, isolando artefatos consolidados da área de `processed`.
* `ensure_dir` no destino garante pré-condição de existência e elimina dependência de ordem de chamadas externas.
* O dicionário `bdq_municipio_bioma.csv` é insumo opcional, requerido apenas quando há filtro por bioma.

Configurações implícitas: `utils.get_path` resolve caminhos absolutos conforme `config.yaml`, preservando portabilidade.

---

## 3. Descoberta e ordenação dos insumos

```python
41  _INMET_FILE_RE = re.compile(r"^inmet_(\d{4})\.csv$", re.IGNORECASE)
43  def parse_year_from_filename(fn: str) -> Optional[int]: ...
46  def list_inmet_year_files(processed_dir: Path) -> List[Tuple[int, Path]]:
47      pairs = []
48      for p in sorted(processed_dir.glob("inmet_*.csv")):
49          y = parse_year_from_filename(p.name)
50          if y is not None and p.is_file(): pairs.append((y, p))
52      pairs.sort(key=lambda x: x[0]); return pairs
```

Propriedades:

* Seleção é estrita por regex de 4 dígitos, mitigando arquivos espúrios no diretório.
* Ordenação crescente por ano impõe determinismo a jusante e melhora previsibilidade na fusão.
* Complexidade O(N log N) para N anos disponíveis.

---

## 4. Primitivas auxiliares de leitura e escrita

```python
57  def _batched(lst, n=3): yield lst[i:i+n] ...
60  def _read_header_line_raw(p): return fh.readline().rstrip("\n\r")
64  def _parse_header_fields_from_line(header_line): return next(csv.reader([header_line]), [])
```

Racional:

* `_read_header_line_raw` preserva o header literal, inclusive aspas e separadores, servindo como fonte de verdade para reemissão do cabeçalho na saída.
* `_parse_header_fields_from_line` aplica semântica CSV apenas para identificar posições de colunas, evitando reescrita estrutural do texto.
* `_batched` admite controle granular de I O no modo `combine`, amortizando custo de abertura de muitos arquivos.

Cópia de linhas:

```python
118 def _append_csv_filtered_by_municipio(...):
125     reader = csv.reader(s); next(reader)  # pula header
128     writer = csv.writer(dst_fh, lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
134     if mun in allowed_municipios: writer.writerow(row); rows += 1

141 def _append_csv_skip_header_raw(src, dst_fh):
145     _ = s.readline()  # skip header
146     for line in s: dst_fh.write(line); rows += 1
```

Duas estratégias:

* Modo filtrado opera em nível de células, reserializando com `csv.writer` para correção sintática garantida.
* Modo bruto preserva bytes da linha, maximizando throughput quando não há filtro por município.

---

## 5. Normalização de datas e política de sentinelas

Normalização da primeira coluna de data:

```python
151 def _normalize_dates_text_inplace(csv_path):
156   first = True
162   idx = line.find(",")
163   if idx > 0:
164       token = line[:idx]
165       if "/" in token: token = token.replace("/", "-")
166       line = token + line[idx:]
```

Características:

* Operação textual e local, exclusiva à primeira coluna, sem parsing CSV completo, otimizando desempenho.
* Preserva header intacto e quaisquer aspas fora da primeira célula.

Remoção de sentinelas:

```python
174 def _drop_rows_with_sentinels_inplace(csv_path, drop_policy="all"):
181   SENTINELS = {"-9999", "-999"}
189   header = next(reader); writer.writerow(header)
192   keep_as_meta = {"DATA (YYYY-MM-DD)","HORA (UTC)","ANO","CIDADE","LATITUDE","LONGITUDE"}
193   meta_idx = {i for i,name in enumerate(header) if name in keep_as_meta}
194   measure_idx = [i for i in range(len(header)) if i not in meta_idx]
198   if drop_policy == "any":
199       if any(v in SENTINELS for v in values): continue
202   else:
203       meas_non_empty = [v for v in values if v != ""]
204       if meas_non_empty and all(v in SENTINELS for v in meas_non_empty): continue
```

Semântica:

* Colunas meta são preservadas de avaliação. Medidas são todas as demais.
* Política `any`: linha descartada se qualquer medida for sentinela.
* Política `all`: linha descartada apenas se todas as medidas não vazias forem sentinelas.

Ambas as rotinas são in place mediante arquivo temporário e `replace`, garantindo atomicidade do swap.

---

## 6. Resolução determinística de nomes de saída

```python
70  def _resolve_combined_output_filename(years, biome):
72     if biome: b = biome.strip().lower().replace(" ","_")
74       if not years: return f"inmet_all_years_{b}.csv"
75       yrs = sorted({int(y) for y in years})
76       return f"inmet_{yrs[0]}_{yrs[-1]}_{b}.csv" if len(yrs)>1 else f"inmet_{yrs[0]}_{b}.csv"
78     else:
79       if not years: return "inmet_all_years.csv"
80       yrs = sorted({int(y) for y in years})
81       return f"inmet_{yrs[0]}_{yrs[-1]}.csv" if len(yrs)>1 else f"inmet_{yrs[0]}.csv"

84  def _resolve_year_output_filename(year, biome):
85     if biome: ...
```

A estratégia garante nomes estáveis e informativos abrangendo intervalo de anos e bioma quando aplicável.

---

## 7. Filtro por bioma via dicionário BDQueimadas

```python
87  def _load_allowed_municipios_for_biome(biome, encoding="utf-8") -> Set[str]:
90     dic_path = get_dictionary_csv_path()
92     if not dic_path.exists(): raise FileNotFoundError(...)
96     reader = csv.DictReader(fh); fns = set(reader.fieldnames or [])
97     if not {"municipio","bioma"}.issubset(fns): raise ValueError(...)
98     has_norm = "municipio_norm" in fns
100    tgt = biome.casefold()
101    for row in reader:
102        if str(row["bioma"]).casefold() == tgt:
103            mun_n = str(row["municipio_norm"]).strip() if has_norm else normalize_key(row["municipio"])
104            if mun_n: allowed.add(mun_n)
```

Detalhes:

* Comparação casefold robusta para grafias heterogêneas de bioma.
* Suporte opcional a coluna já normalizada `municipio_norm`. Na ausência, aplica `normalize_key` para unificar espaços, acentuação e caixa.

---

## 8. Consolidação por modo: split, combine e both

Assinatura de alto nível:

```python
215 def consolidate_inmet(
216   mode="split",
217   output_filename=None,
218   years=None,
219   overwrite=False,
220   encoding="utf-8",
221   batch_size=3,
222   normalize_dates=True,
223   biome=None,
224   municipio_col="CIDADE",
225   drop_policy="all",
226 ) -> List[Path]:
```

Etapas comuns:

1. Inicialização de logger e config.
2. Resolução de diretórios de entrada e saída.
3. Descoberta e filtro de anos, com `FileNotFoundError` se vazio.
4. Carregamento de `allowed_municipios` quando `biome` é fornecido.

### 8.1 Modo split

```python
243 if mode in {"split","both"}:
246   for y, path in year_files:
247       header_line = _read_header_line_raw(path)
248       header_fields = _parse_header_fields_from_line(header_line)
252       municipio_idx = header_fields.index(municipio_col)  # ValueError tratado
256       out_path = out_dir / _resolve_year_output_filename(y, biome)
258       if out_path.exists() and not overwrite: skip
263       with out_path.open("w") as out_fh:
264           out_fh.write(header_line+"\n")
266           if allowed_municipios is None:
267               _append_csv_skip_header_raw(path, out_fh)
269           else:
270               _append_csv_filtered_by_municipio(path, out_fh, municipio_idx, allowed_municipios)
273       if normalize_dates: _normalize_dates_text_inplace(out_path)
275       _drop_rows_with_sentinels_inplace(out_path, drop_policy=drop_policy)
```

Propriedades:

* Um arquivo por ano, opcionalmente filtrado e normalizado.
* Checagem de existência implementa idempotência quando `overwrite=False`.

### 8.2 Modo combine

```python
279 if mode in {"combine","both"}:
281   auto = _resolve_combined_output_filename([...], biome)
282   out_path = out_dir / (output_filename or auto)
284   if out_path.exists() and not overwrite: skip
288   header_line_raw = _read_header_line_raw(first_path)
289   header_fields = _parse_header_fields_from_line(header_line_raw)
292   municipio_idx_first = header_fields.index(municipio_col)
296   with out_path.open("w") as out_fh:
297       out_fh.write(header_line_raw+"\n")
299       for batch in _batched(year_files, batch_size):
301           for y, path in batch:
303               header_fields_this = _parse_header_fields_from_line(_read_header_line_raw(path))
307               try: municipio_idx = header_fields_this.index(municipio_col)
308               except ValueError: municipio_idx = municipio_idx_first
311               if allowed_municipios is None:
312                   added = _append_csv_skip_header_raw(path, out_fh)
314               else:
315                   added = _append_csv_filtered_by_municipio(path, out_fh, municipio_idx, allowed_municipios)
318               total_rows += added; log.info(f"    +{added} linhas (acumulado: {total_rows})")
321   if normalize_dates: _normalize_dates_text_inplace(out_path)
323   _drop_rows_with_sentinels_inplace(out_path, drop_policy=drop_policy)
```

Destaques:

* Header único emitido a partir do primeiro arquivo. Para heterogeneidades leves, a posição de `municipio_col` é recalculada por arquivo com fallback seguro.
* Processamento em lotes controla uso de recursos e melhora telemetria.

---

## 9. Propriedades formais: determinismo, idempotência e complexidade

* Determinismo: a ordenação de anos e a resolução de nomes de saída garantem artefatos estáveis para conjuntos de entrada fixos e mesmas flags.
* Idempotência: respeitada por arquivo quando `overwrite=False`. Operações in place com arquivo temporário e `replace` evitam estados parciais.
* Complexidade:

  * Descoberta: O(N log N), N número de anos.
  * Split: O(Σ Ri), somatório de linhas por ano, com memória O(1) em streaming.
  * Combine: O(Σ Ri), com overhead O(B) por lote e custo de reabertura de headers por arquivo.
  * Normalização e limpeza: O(L) no total de linhas emitidas.

---

## 10. Observabilidade e manejo de falhas

Pontos de log:

* Detecção de anos e seleção: sucesso e vazio.
* Em split: eventos `[SKIP]`, `[WRITE]`, e avisos sobre headers ausentes ou coluna de município faltante.
* Em combine: emissão por lote `[BATCH] anos=[...]` e contadores incrementais de linhas.
* Exceções são elevadas na CLI com `log.exception`, preservando stack trace.

Diretrizes:

* Relatar totais no rodapé por modo: arquivos gerados, linhas emitidas, linhas descartadas por sentinelas.
* Promover `warning` quando o dicionário de bioma existir mas retornar conjunto vazio para o bioma alvo.

---

## 11. Considerações de desempenho e I O

* Leitura e escrita em streaming evitam materialização integral em memória.
* `_append_csv_skip_header_raw` maximiza throughput em cenários sem filtro por bioma, reduzindo custo de reserialização CSV.
* `batch_size` no combine deve ser ajustado conforme latência de disco e tamanho de arquivos. Valores típicos entre 3 e 10 equilibram contagem de `open` e espaço de cache do SO.
* `csv.field_size_limit` é elevado ao máximo suportado para acomodar campos longos raros.

---

## 12. Segurança, integridade e contratos de esquema

* Atomicidade: as rotinas in place usam arquivo temporário e `replace`. Em falhas, o original permanece íntegro.
* Confiabilidade de schema: o módulo não reordena colunas e reemite o header original no topo da saída.
* Robustez a heterogeneidade: a posição de `municipio_col` é resolvida por arquivo; em falta, há fallback ao índice do primeiro arquivo no combine.
* Dicionário externo: valida existência e colunas mínimas. Em ausência de `municipio_norm` aplica `normalize_key` como função de referência.

---

## 13. Extensões recomendadas

1. Métricas de qualidade
   Contabilizar linhas descartadas por política `any` e `all`, e percentuais por arquivo e agregado.

2. Validação de header
   Checar igualdade estrita de headers antes do combine e emitir diff resumido quando divergirem.

3. Filtros adicionais
   Suporte a seleção de colunas, intervalos de data, e bounding box geográfico.

4. Enriquecimento leve
   Opcionalmente anexar coluna `BIOMA` quando o filtro estiver ativo, para rastreabilidade.

5. Catálogo de saídas
   Escrever manifesto JSON por execução com nomes, anos, opções, total de linhas e checksum.

6. Paralelismo controlado em split
   `ThreadPoolExecutor` para processar anos independentes, com limite de workers proporcional a núcleos e I O.

---

## 14. Exemplos de uso

Consolidar todos os anos, um arquivo por ano, normalizando datas e removendo linhas com todas as medidas sentinelas:

```bash
python -m src.inmet_consolidated --mode split
```

Combinar 2008 a 2015 em um único CSV, forçando sobrescrita e mantendo a política default `all`:

```bash
python -m src.inmet_consolidated --mode combine --years 2008 2009 2010 2011 2012 2013 2014 2015 --overwrite
```

Split com filtro por bioma Cerrado, coluna de município customizada e política de descarte mais agressiva:

```bash
python -m src.inmet_consolidated --mode split --biome Cerrado --municipio-col "MUNICIPIO" --drop-policy any
```

Both com nome de saída definido e lote maior:

```bash
python -m src.inmet_consolidated --mode both --output-filename inmet_1998_2024_cerrado.csv --biome Cerrado --batch-size 8
```

Desativar normalização de datas quando já padronizadas na origem:

```bash
python -m src.inmet_consolidated --mode combine --no-normalize-dates
```

---

### Anexo A. Contratos e invariantes

* A primeira linha de cada insumo deve ser um header válido CSV.
* A primeira coluna de dados representa a data; se contiver barras, é normalizada para `YYYY-MM-DD` quando habilitado.
* A coluna de município informada em `--municipio-col` deve existir em todos os insumos relevantes; caso contrário, arquivos individuais podem ser pulados em split, e no combine há fallback para o índice do primeiro header.
* O dicionário de bioma deve conter `municipio` e `bioma` e, se disponível, `municipio_norm`. Em sua ausência, o filtro por bioma não deve ser ativado.
