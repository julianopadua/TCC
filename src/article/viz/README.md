# Visualização Streamlit (bases do artigo)

Lê os Parquets em `data/_article/0_datasets_with_coords/` conforme `article_pipeline.scenarios` no `config.yaml`.

## Executar

Na raiz do repositório (com o ambiente virtual ativo):

```bash
streamlit run src/article/viz/app.py
```

Windows (PowerShell):

```powershell
.\.venv\Scripts\streamlit.exe run src/article/viz/app.py
```

## Secções

- **Um ano**: um Parquet por execução; filtros de cidades e variáveis; gráfico Plotly (painéis por cidade se várias).
- **Vários anos**: até 5 anos concatenados; aviso se o volume de linhas for grande; opção de tabela de correlações biomassa × focos (mesma lógica do EDA em batch).

## Dependências

`streamlit` e `plotly` (listados em `requirements.txt`).

## Validação manual sugerida

1. Base E, ano 2003, uma cidade, precip + radiação.
2. HAS_FOCO como marcadores e como eixo secundário (1 cidade).
3. Duas cidades: legenda e painéis.
4. Vários anos: 3 anos, mesma cidade, correlações com biomassa selecionada.
