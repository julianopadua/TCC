# audit_city_coverage.py – Documentação Técnica Interna  

---

## 1. Visão geral e responsabilidade  

O módulo **audit_city_coverage.py** realiza a auditoria de cobertura de cidades entre duas bases de dados consolidadas: **BDQUEIMADAS** (BDQ) e **INMET**.  
Para cada bioma e ano elegível ele:

1. Carrega os arquivos CSV de origem (`bdq_targets_{YYYY}_{biome}.csv` e `inmet_{YYYY}_{biome}.csv`).  
2. Normaliza os nomes das cidades (função `utils.normalize_key`).  
3. Calcula a interseção e as partições exclusivas entre os conjuntos de cidades.  
4. Gera métricas resumidas (contagens e proporções).  
5. Persiste:
   - Um CSV de resumo geral (`city_coverage_summary_{biome}.csv`).  
   - Um CSV detalhado por ano (`year_{YYYY}_{biome}.csv`).  
   - Um documento Markdown detalhado por ano (`year_{YYYY}_{biome}.md`).  

O objetivo é prover um diagnóstico quantitativo da sobreposição de cidades monitoradas pelos dois sistemas, facilitando análises posteriores de qualidade de dados e cobertura geográfica.

---

## 2. Onde este arquivo se encaixa na arquitetura  

| Camada / Domínio | Papel |
|------------------|-------|
| **Data‑ingestion / Consolidation** | Consome arquivos consolidados já gerados por pipelines externos (BDQUEIMADAS e INMET). |
| **Business Logic** | Implementa a lógica de comparação e cálculo de métricas de cobertura. |
| **Persistência** | Escreve resultados em diretórios de *dictionarys* (artefatos de referência). |
| **Utilitário / Infraestrutura** | Usa helpers genéricos (`utils.loadConfig`, `utils.get_logger`, etc.). |
| **Interface de linha de comando (CLI)** | Permite execução ad‑hoc via `python -m audit_city_coverage`. |

Não há dependência direta de camada de apresentação (UI) nem de camada de modelo de domínio avançado; o módulo atua como um **processador de dataset** autônomo.

---

## 3. Interfaces e exports  

| Export | Tipo | Descrição |
|--------|------|-----------|
| `audit_city_coverage` | Função | API pública. Recebe `biome`, lista opcional de `years`, `min_year` e `encoding`. Executa a auditoria completa e devolve `(summary_df, years_processed)`. |
| `_coverage_for_year` | Função (privada) | Calcula métricas e gera o Markdown detalhado para um único ano. |
| `_read_inmet_cities`, `_read_bdq_cities` | Funções (privadas) | Leitura e normalização de cidades de cada fonte. |
| `_list_inmet_years_for_biome`, `_list_bdq_years_for_biome` | Funções (privadas) | Descobrem anos disponíveis nos diretórios de origem. |
| `_details_dir`, `_dict_root`, `_inmet_consolidated_dir`, `_bdq_consolidated_dir` | Funções (privadas) | Resolução de caminhos de arquivos/diretórios. |

> **Nota:** As funções marcadas como privadas (`_prefixo`) não são destinadas a uso externo, mas podem ser importadas explicitamente se necessário.

---

## 4. Dependências e acoplamentos  

| Tipo | Módulo / Pacote | Motivo |
|------|----------------|--------|
| **Externa** | `pandas` | Manipulação tabular, agrupamento e concatenação de DataFrames. |
| **Externa** | `pathlib`, `re`, `collections`, `typing` | Operações de caminho, regex, contadores e tipagem. |
| **Interna** | `utils` (funções: `loadConfig`, `get_logger`, `get_path`, `ensure_dir`, `normalize_key`) | Configuração, logging, resolução de caminhos de projeto e normalização de chaves. |
| **Acoplamento de dados** | Diretórios `data/external/INMET` e `data/external/BDQUEIMADAS` | O módulo depende da estrutura de arquivos consolidada; mudanças nesses caminhos exigem atualização das funções de *path*. |
| **Acoplamento de saída** | Diretório `data/dictionarys` | Resultados são gravados aqui; a estrutura de subpastas (`city_coverage_details`) é fixa. |

Não há dependências circulares detectáveis a partir do código fornecido.

---

## 5. Leitura guiada do código (top‑down)  

### 5.1. Definições de caminho e padrões  

```python
def _inmet_consolidated_dir() -> Path:
    return Path(get_path("paths", "data", "external")) / "INMET"

def _bdq_consolidated_dir() -> Path:
    return Path(get_path("paths", "data", "external")) / "BDQUEIMADAS"

_INMET_RE = re.compile(r"^inmet_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)
_BDQ_RE   = re.compile(r"^bdq_targets_(\d{4})_(?P<biome>[a-z0-9_]+)\.csv$", flags=re.IGNORECASE)
```

*Invariantes*:  
- Os diretórios são sempre derivados de `utils.get_path("paths", "data", "external")`.  
- Os regex garantem que apenas arquivos com o padrão esperado sejam considerados.

### 5.2. Descoberta de anos disponíveis  

```python
def _list_inmet_years_for_biome(biome: str) -> List[int]:
    root = _inmet_consolidated_dir()
    years = []
    for p in root.glob(f"inmet_*_{biome}.csv"):
        m = _INMET_RE.match(p.name)
        if m:
            years.append(int(m.group(1)))
    return sorted(set(years))
```

*Decisão*: Usa `glob` + regex para filtrar arquivos por bioma, garantindo que o conjunto de anos seja único e ordenado.

### 5.3. Leitura e normalização de cidades  

Ambas as funções (`_read_inmet_cities`, `_read_bdq_cities`) seguem o mesmo padrão:

1. Construção do caminho de arquivo.  
2. Leitura com `pd.read_csv(..., dtype=str, usecols=...)`.  
3. Criação de colunas auxiliares:
   - `city_raw` (valor original).  
   - `city_norm` (resultado de `normalize_key`).  
4. Contagem de ocorrências brutas (`collections.Counter`).  
5. Agrupamento por `(city_raw, city_norm)` para obter a frequência (`count`).  

*Invariantes*:  
- Sempre retorna um DataFrame com colunas fixas `['source','year','city_raw','city_norm','count']`.  
- O `source` é `"INMET"` ou `"BDQ"`.

### 5.4. Cálculo da cobertura por ano  

```python
def _coverage_for_year(year: int, biome: str, encoding: str = "utf-8") -> Tuple[Dict[str, int], pd.DataFrame, str]:
    inmet_df, inmet_raw_counts = _read_inmet_cities(year, biome, encoding=encoding)
    bdq_df,   bdq_raw_counts   = _read_bdq_cities(year, biome, encoding=encoding)

    inmet_set = set(inmet_df["city_norm"].dropna())
    bdq_set   = set(bdq_df["city_norm"].dropna())

    common = sorted(inmet_set & bdq_set)
    bdq_only = sorted(bdq_set - inmet_set)
    inmet_only = sorted(inmet_set - bdq_set)

    # métricas de contagem e proporção
    resumo = {
        "year": year,
        "n_cidades_bdq": len(bdq_set),
        "n_cidades_inmet": len(inmet_set),
        "n_comuns": len(common),
        "n_bdq_exclusivas": len(bdq_only),
        "n_inmet_exclusivas": len(inmet_only),
        "prop_inmet_cobre_bdq": round(len(common) / len(bdq_set) if bdq_set else 0.0, 6),
        "prop_bdq_cobre_inmet": round(len(common) / len(inmet_set) if inmet_set else 0.0, 6),
    }

    detalhes_df = pd.concat([bdq_df, inmet_df], ignore_index=True)

    # montagem do Markdown detalhado (listas de cidades e exemplos raw)
    ...
    return resumo, detalhes_df, details_md
```

*Decisões de implementação*:
- Usa `set` de `city_norm` para garantir unicidade antes da comparação.  
- As proporções são calculadas com proteção contra divisão por zero.  
- O Markdown inclui listas ordenadas e amostras das variantes `city_raw` (útil para diagnóstico de normalização).

### 5.5. Pipeline principal (`audit_city_coverage`)  

```python
def audit_city_coverage(
    biome: str = "cerrado",
    years: Optional[Iterable[int]] = None,
    min_year: int = 2003,
    encoding: str = "utf-8",
) -> Tuple[pd.DataFrame, List[int]]:
    log = get_logger("city_coverage.audit", kind="dataset", per_run_file=True)
    _ = loadConfig()

    inmet_years = _list_inmet_years_for_biome(biome)
    bdq_years   = _list_bdq_years_for_biome(biome)

    candidate_years = sorted(set(inmet_years).intersection(bdq_years))
    candidate_years = [y for y in candidate_years if y >= min_year]

    years_to_run = (sorted({int(y) for y in years}) if years else candidate_years)
    years_to_run = [y for y in years_to_run if y in candidate_years]

    if not years_to_run:
        raise RuntimeError("Sem anos elegíveis ...")

    details_root = _details_dir(biome)
    summary_rows, processed_years = [], []

    for y in years_to_run:
        try:
            log.info(f"[YEAR] Auditando {y} ({biome})...")
            resumo, detalhes_df, details_md = _coverage_for_year(y, biome, encoding=encoding)

            (details_root / f"year_{y}_{biome}.csv").to_csv(detalhes_df, index=False, encoding=encoding)
            (details_root / f"year_{y}_{biome}.md").write_text(details_md, encoding=encoding)

            summary_rows.append(resumo)
            processed_years.append(y)
        except Exception as e:
            log.exception(f"[ERROR] Falha ao auditar ano {y}: {e}")

    summary_df = pd.DataFrame(summary_rows).sort_values("year").reset_index(drop=True)
    out_summary = _dict_root() / f"city_coverage_summary_{biome}.csv"
    summary_df.to_csv(out_summary, index=False, encoding=encoding)
    log.info(f"[WRITE] {out_summary} (linhas: {len(summary_df)})")

    return summary_df, processed_years
```

*Fluxo resumido*:
1. **Descoberta** de anos comuns entre as duas fontes.  
2. **Filtragem** por `min_year` e, opcionalmente, por lista explícita de `years`.  
3. Loop **por ano**:
   - Calcula cobertura (`_coverage_for_year`).  
   - Persiste CSV e MD detalhados.  
   - Acumula linha de resumo.  
4. **Consolidação** das linhas de resumo em um DataFrame único e grava o CSV final.

### 5.6. Interface de linha de comando  

```python
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(
        description="Audita a cobertura de cidades BDQueimadas × INMET por ano e bioma."
    )
    p.add_argument("--biome", type=str, default="cerrado")
    p.add_argument("--years", nargs="*", type=int, default=None)
    p.add_argument("--min-year", type=int, default=2003)
    p.add_argument("--encoding", type=str, default="utf-8")
    args = p.parse_args()

    log = get_logger("city_coverage.audit", kind="dataset", per_run_file=True)
    try:
        summary_df, years_processed = audit_city_coverage(
            biome=args.biome,
            years=args.years,
            min_year=args.min_year,
            encoding=args.encoding,
        )
        log.info(f"[DONE] anos={years_processed}  linhas_resumo={len(summary_df)}")
    except Exception as e:
        log.exception(f"[ERROR] Falha na auditoria: {e}")
```

A CLI expõe os mesmos parâmetros da API, facilitando execuções pontuais em ambientes de desenvolvimento ou CI.

---

## 6. Fluxo de dados / estado / eventos  

| Etapa | Entrada | Transformação | Saída |
|-------|---------|---------------|-------|
| **Descoberta de arquivos** | Diretórios `INMET/` e `BDQUEIMADAS/` | Regex + `glob` → listas de anos | `List[int]` por fonte |
| **Leitura de CSV** | Arquivo `inmet_{Y}_{B}.csv` ou `bdq_targets_{Y}_{B}.csv` | `pd.read_csv` → colunas `city_raw`, `city_norm`, `count` | DataFrames (`source='INMET'` ou `'BDQ'`) |
| **Normalização** | `city_raw` | `utils.normalize_key` → `city_norm` | Chave normalizada usada nos conjuntos |
| **Cálculo de cobertura** | Dois DataFrames | Set operations → `common`, `bdq_only`, `inmet_only`; cálculo de proporções | `resumo` (dict), `detalhes_df` (long), `details_md` (texto) |
| **Persistência** | `detalhes_df`, `details_md`, lista de `resumo` | `to_csv`, `write_text`, `DataFrame.to_csv` | Arquivos CSV/MD em `data/dictionarys/...` |
| **Logging** | Eventos de início/fim/erro | `utils.get_logger` → mensagens estruturadas | Registro em arquivos de log (per‑run). |

Não há estado mutável global; todas as variáveis são locais ao escopo da chamada.

---

## 7. Conexões com outros arquivos do projeto  

- **utils.py** – fornece funções de configuração, logging, resolução de caminhos e normalização de chaves.  
  - Links: *(nenhum)* – a documentação de `utils` deve ser consultada separadamente.  
- Não há importações internas adicionais nem exportações consumidas por outros módulos (conforme análise de dependências).  

> **Observação:** Caso novos módulos precisem consumir a API `audit_city_coverage`, basta importar a função pública.

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| Item | Impacto | Recomendações |
|------|---------|---------------|
| **Dependência de estrutura de arquivos** | Quebra se o padrão de nomeação mudar. | Centralizar padrões de nome em constantes configuráveis ou usar metadados de catálogo. |
| **Normalização de nomes (`normalize_key`)** | Divergências podem gerar falsos negativos/positivos na interseção. | Documentar regras de normalização; considerar fallback para comparação fuzzy quando `city_norm` for idêntico a `city_raw`. |
| **Uso de `collections.Counter` apenas para amostra** | Não afeta métricas, mas pode consumir memória em arquivos muito grandes. | Avaliar streaming de contagem ou limitar ao top‑N já implementado. |
| **Ausência de validação de schema** | Arquivos CSV podem mudar de colunas ou tipos. | Inserir validação leve (ex.: `assert "CIDADE" in df.columns`). |
| **Logs per‑run** | Arquivo de log pode crescer indefinidamente em execuções frequentes. | Implementar rotação ou limite de tamanho. |
| **Teste unitário** | Não há cobertura de testes no repositório. | Criar testes para `_coverage_for_year` usando fixtures de CSV minimalistas. |
| **CLI ergonomia** | Argumentos `--years` aceitam lista livre; não há validação de intervalo. | Adicionar verificação de anos dentro de `candidate_years` e mensagem de erro amigável. |
| **Performance** | Para biomas com milhares de cidades, a operação de `set` e `sorted` pode ser custosa. | Medir tempo de execução; se necessário, usar estruturas mais eficientes (ex.: `numpy` ou `pandas` merge). |

Implementar as recomendações acima aumentará a robustez, a manutenibilidade e a escalabilidade da auditoria de cobertura de cidades.
