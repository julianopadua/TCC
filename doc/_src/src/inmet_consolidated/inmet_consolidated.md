# Documentação – `src/inmet_consolidated.py`

---

## 1. Visão geral e responsabilidade  

O módulo **`inmet_consolidated.py`** consolida os arquivos CSV gerados pelo processo *INMET* (dados meteorológicos) que se encontram em `processed/INMET`. Ele produz, sob demanda, arquivos de saída no diretório `consolidated/INMET` nos seguintes modos:

| modo | descrição |
|------|------------|
| **split** | um CSV por ano |
| **combine** | um único CSV contendo todos os anos selecionados |
| **both** | gera os dois tipos acima |

Funcionalidades adicionais:

* **Filtragem opcional por bioma** – utiliza o dicionário `bdq_municipio_bioma.csv` para manter apenas municípios pertencentes ao bioma informado.  
* **Normalização da coluna de data** – converte datas no formato `DD/MM/YYYY` para `YYYY-MM-DD`.  
* **Remoção de linhas com valores sentinela** (`-9999`, `-999`) nas colunas de medidas, com política *all* ou *any*.  

---

## 2. Posicionamento na arquitetura  

| camada | descrição |
|--------|-----------|
| **Utilitário / Backend** | O módulo não contém lógica de apresentação nem de API. Ele opera como um **processamento batch** de dados, sendo invocado por scripts de linha‑comando ou por pipelines de ETL. |
| **Domínio** | Relacionado ao domínio *climatologia / meteorologia* (provedor INMET). |
| **Dependência** | Depende exclusivamente do pacote interno `utils` (configuração, logging, manipulação de caminhos). Não há dependências externas além da biblioteca padrão. |

---

## 3. Interfaces e exports  

O módulo exporta apenas duas entidades públicas:

```python
def get_inmet_consolidated_dir() -> Path:
    """Retorna o diretório onde os arquivos consolidados são gravados."""

def consolidate_inmet(
    mode: str = "split",
    output_filename: Optional[str] = None,
    years: Optional[Iterable[int]] = None,
    overwrite: bool = False,
    encoding: str = "utf-8",
    batch_size: int = 3,
    normalize_dates: bool = True,
    biome: Optional[str] = None,
    municipio_col: str = "CIDADE",
    drop_policy: str = "all",
) -> List[Path]:
    """Realiza a consolidação conforme os parâmetros descritos."""
```

Além disso, o bloco `if __name__ == "__main__":` fornece uma **CLI** (interface de linha de comando) que expõe os mesmos parâmetros.

---

## 4. Dependências e acoplamentos  

| tipo | módulo / pacote | motivo |
|------|----------------|--------|
| **Interna** | `utils` (`loadConfig`, `get_logger`, `get_path`, `ensure_dir`, `normalize_key`) | Configuração, logging, resolução de caminhos e normalização de chaves. |
| **Padrão** | `pathlib`, `typing`, `re`, `csv`, `sys` | Manipulação de arquivos, tipos, expressões regulares e CSV. |
| **Externa** | *nenhuma* | O código não depende de bibliotecas de terceiros. |

Acoplamento: **fraco** – a única ligação externa é ao módulo `utils`, que fornece funções genéricas. Caso `utils` seja alterado, apenas as assinaturas usadas precisam ser mantidas.

---

## 5. Leitura guiada do código (top‑down)

1. **Configuração inicial**  
   ```python
   csv.field_size_limit(sys.maxsize)
   ```
   Garante suporte a campos CSV muito extensos.

2. **Seção 1 – Paths**  
   Funções auxiliares (`get_inmet_processed_dir`, `get_consolidated_root`, `get_inmet_consolidated_dir`, `get_dictionary_csv_path`) encapsulam a lógica de localização de diretórios e do dicionário de municípios/biomas. Elas delegam a `utils.get_path` e `ensure_dir`.

3. **Seção 2 – Descoberta**  
   * `_INMET_FILE_RE` identifica arquivos `inmet_<ano>.csv`.  
   * `parse_year_from_filename` extrai o ano.  
   * `list_inmet_year_files` devolve uma lista ordenada de tuplas `(ano, Path)`.

4. **Seção 3 – Helpers**  
   * `_batched` – particiona a lista de arquivos em lotes (usado no modo *combine*).  
   * `_read_header_line_raw` / `_parse_header_fields_from_line` – leitura e parsing do cabeçalho preservando aspas e vírgulas internas.  
   * `_resolve_*_output_filename` – construção determinística dos nomes de arquivos de saída, levando em conta anos e bioma.  
   * `_load_allowed_municipios_for_biome` – lê o dicionário CSV e devolve o conjunto de municípios normalizados que pertencem ao bioma solicitado. Levanta exceções claras caso o arquivo não exista ou as colunas esperadas estejam ausentes.  
   * `_append_csv_filtered_by_municipio` – copia linhas cujo município (normalizado) está no conjunto permitido.  
   * `_append_csv_skip_header_raw` – cópia “bruta” de linhas, ignorando o cabeçalho.  
   * `_normalize_dates_text_inplace` – substitui `/` por `-` na primeira coluna (data) em‑place.  
   * `_drop_rows_with_sentinels_inplace` – elimina linhas contendo valores sentinela nas colunas de medida, obedecendo à política *all* ou *any*.

5. **Seção 4 – Consolidação (`consolidate_inmet`)**  
   * Validação do parâmetro `mode`.  
   * Resolução dos diretórios de entrada/saída.  
   * Filtragem opcional de anos (`years`).  
   * Carregamento do conjunto de municípios permitidos quando `biome` é informado.  
   * **Modo *split*** – para cada arquivo anual:  
     - Leitura do cabeçalho e localização da coluna de município.  
     - Criação (ou sobrescrita) do CSV de saída, aplicando filtro por bioma se necessário.  
     - Normalização de datas e remoção de sentinelas.  
   * **Modo *combine*** – cria um único CSV:  
     - Usa o cabeçalho do primeiro arquivo como base.  
     - Processa arquivos em lotes (`batch_size`) para limitar uso de memória.  
     - Recalcula o índice da coluna de município para cada arquivo (fallback para o índice do primeiro).  
     - Aplica as mesmas transformações de filtro, normalização e limpeza.  
   * Retorna a lista de `Path` dos arquivos gerados.

6. **Seção 5 – CLI**  
   Utiliza `argparse` para expor todos os parâmetros da função `consolidate_inmet`. Em caso de exceção, registra o erro via logger.

---

## 6. Fluxo de dados / estado / eventos  

1. **Entrada** – arquivos CSV `processed/INMET/inmet_<ano>.csv`.  
2. **Transformação** – (a) leitura de cabeçalho, (b) filtragem por município/bioma, (c) normalização de datas, (d) remoção de linhas com sentinelas.  
3. **Saída** – arquivos CSV em `consolidated/INMET/` conforme o modo escolhido.  
4. **Estado** – o módulo não mantém estado global; todas as variáveis são locais ou imutáveis.  
5. **Eventos** – logging (`get_logger`) registra início/fim de cada etapa, avisos de arquivos já existentes, e contagem de linhas processadas por lote.

---

## 7. Conexões com outros arquivos do projeto  

| módulo | relação |
|--------|---------|
| `utils.py` | Fornece funções de configuração, logging, resolução de caminhos e normalização de chaves. |
| `data/dictionarys/bdq_municipio_bioma.csv` | Arquivo de dicionário usado para filtragem por bioma. |
| **Nenhum módulo** importa `inmet_consolidated.py` diretamente (conforme metadados). Ele é consumido via CLI ou por scripts externos que chamam `consolidate_inmet`. |

*(Links para a documentação de `utils` e para o dicionário podem ser inseridos quando disponíveis.)*

---

## 8. Pontos de atenção, riscos e melhorias recomendadas  

| ponto | descrição | recomendação |
|-------|-----------|--------------|
| **Validação de cabeçalho** | O código assume que todos os arquivos têm o mesmo conjunto de colunas. Caso haja divergência, o índice de município pode ficar incorreto. | Implementar verificação de consistência de cabeçalhos entre arquivos e abortar com mensagem clara se houver incompatibilidade. |
| **Performance de I/O** | A leitura e escrita são feitas linha a linha; para arquivos muito grandes pode ser um gargalo. | Avaliar uso de `csv.DictReader/DictWriter` com buffers maiores ou processamento em paralelo (ex.: `multiprocessing.Pool`). |
| **Gerenciamento de memória** | O modo *combine* carrega apenas um lote de arquivos, mas ainda mantém o arquivo de saída aberto por todo o processo. | Considerar escrita incremental em múltiplos arquivos temporários e concatenação final, reduzindo risco de corrupção em falhas. |
| **Dependência de `normalize_key`** | A normalização de municípios depende de `utils.normalize_key`. Se a lógica mudar, filtros por bioma podem ser afetados. | Documentar contrato da função `normalize_key` (ex.: remoção de acentos, caixa baixa) e criar testes de integração. |
| **Tratamento de erros de CSV** | Linhas com número de colunas diferente do cabeçalho são simplesmente ignoradas. | Registrar número de linhas descartadas e, opcionalmente, gerar relatório de qualidade dos dados. |
| **Configuração de caminhos** | `get_path` lê valores de um arquivo de configuração externo; ausência ou erro de configuração gera exceção em tempo de execução. | Validar a presença de todas as chaves necessárias na inicialização e prover mensagens de erro amigáveis. |
| **Teste unitário** | Não há cobertura de testes no repositório. | Criar testes unitários para cada helper (ex.: `_resolve_*_output_filename`, `_load_allowed_municipios_for_biome`) e para fluxos de `consolidate_inmet` com mocks de arquivos. |
| **CLI – parâmetros opcionais** | O parâmetro `--no-normalize-dates` inverte a flag `normalize_dates`. A documentação da CLI poderia explicitar o efeito. | Atualizar help da CLI para deixar explícito que a data permanecerá no formato original. |

--- 

*Esta documentação segue as diretrizes de estilo solicitadas: linguagem pt‑BR, tom técnico, uso de Markdown com seções claras e sem floreios.*
