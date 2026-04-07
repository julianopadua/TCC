# BDQueimadas - Dicionário de Dados, Guia de Uso e Boas Práticas

> Este documento resume a estrutura dos arquivos CSV exportados pelo BDQueimadas, define tipos, domínios válidos, sentinelas de ausentes, dicas de parsing e checks de qualidade para integração com outras fontes como INMET.

---

## Sumário

1. [Visão geral](#visão-geral)  
2. [Formato e codificação](#formato-e-codificação)  
3. [Dicionário de dados](#dicionário-de-dados)  
4. [Tipos recomendados e domínios](#tipos-recomendados-e-domínios)  
5. [Sentinelas de ausentes e limpeza](#sentinelas-de-ausentes-e-limpeza)  
6. [Parsing com pandas - exemplo](#parsing-com-pandas---exemplo)  
7. [Checks de qualidade](#checks-de-qualidade)  
8. [Integração com INMET - ideias](#integração-com-inmet---ideias)  
9. [Layout sugerido de pastas e nomes](#layout-sugerido-de-pastas-e-nomes)  
10. [Consultas analíticas de exemplo](#consultas-analíticas-de-exemplo)  
11. [Glossário rápido](#glossário-rápido)

---

## Visão geral

O BDQueimadas disponibiliza detecções de focos ativos de queimadas e incêndios a partir de sensores em satélites. Cada linha no CSV representa um foco detectado em um instante e posição específicos, além de atributos ambientais derivados por cruzamentos espaciais com camadas auxiliares, como biomas e limites administrativos. Campos chave para modelagem incluem RiscoFogo e FRP.

---

## Formato e codificação

- Arquivo: CSV separado por vírgulas, com cabeçalho.
- Codificação: geralmente UTF-8.
- Decimal: ponto `.` nos campos numéricos do exemplo.
- Coordenadas: Latitude e Longitude em graus decimais, datum WGS84.
- Tempo: `DataHora` em GMT, formato `YYYY/MM/DD HH:MM:SS`.

---

## Dicionário de dados

| Campo         | Tipo    | Tamanho | Precisão | Descrição                                                                                                                                                | Domínio                                                                                     |
|---------------|---------|---------|----------|----------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| DataHora      | string  | 19      |          | Horário de referência da passagem do satélite em GMT. Formato `YYYY/MM/DD HH:MM:SS`.                                                                    | Qualquer timestamp válido em GMT.                                                           |
| Satelite      | string  | 15      |          | Nome do algoritmo e referência ao satélite provedor da imagem. Exemplos: `AQUA_M-T`, `TERRA_M-T`.                                                        | Lista controlada pelo provedor.                                                             |
| Pais          | string  | 25      |          | País segundo GADM nível 0.                                                                                                                               | Ex. `Brasil`.                                                                               |
| Estado        | string  | 30      |          | Unidade da federação segundo GADM nível 1.                                                                                                               | Ex. `BAHIA`, `PARÁ`.                                                                        |
| Municipio     | string  | 60      |          | Município. Para Brasil, referência IBGE 2000.                                                                                                            | Texto. Fora do Brasil pode variar.                                                          |
| Bioma         | string  | 25      |          | Bioma segundo IBGE 2004. Para outros países pode vir vazio.                                                                                              | Ex. `Amazônia`, `Cerrado`, `Mata Atlântica`, vazio fora do Brasil.                          |
| DiaSemChuva   | integer | 4       |          | Número de dias sem chuva até a detecção.                                                                                                                 | Valor válido maior ou igual a 0. Valor inválido: `-999`.                                    |
| Precipitacao  | double  | 7       | 4        | Precipitação acumulada no dia até o foco. Unidade: milímetros.                                                                                           | Valor válido maior ou igual a 0. Valor inválido: `-999`.                                    |
| RiscoFogo     | double  | 7       | 4        | Risco de fogo previsto para o dia.                                                                                                                       | Intervalo 0.0 a 1.0. Valor inválido: `-999`.                                                |
| FRP           | double  | 7       | 4        | Fire Radiative Power. Unidade: megawatts.                                                                                                                | Valor maior ou igual a 0 em MW.                                                             |
| Latitude      | double  | 7       | 4        | Latitude do centro do pixel do foco em graus decimais.                                                                                                   | Entre -90.0000 e 90.0000.                                                                   |
| Longitude     | double  | 7       | 4        | Longitude do centro do pixel do foco em graus decimais.                                                                                                  | Entre -180.0000 e 180.0000.                                                                 |

**Amostra**

```

DataHora,Satelite,Pais,Estado,Municipio,Bioma,DiaSemChuva,Precipitacao,RiscoFogo,FRP,Latitude,Longitude
2014/01/01 15:55:00,AQUA_M-T,Brasil,BAHIA,BELMONTE,Mata Atlântica,-999,0.0,0.0,14.8,-15.993,-38.951
2014/01/01 15:55:00,AQUA_M-T,Brasil,BAHIA,BELMONTE,Mata Atlântica,-999,0.0,0.0,18.2,-15.995,-38.97
2014/01/01 15:56:00,AQUA_M-T,Brasil,BAHIA,ILHÉUS,Mata Atlântica,4,0.0,0.0,38.6,-14.931,-39.095
2014/01/01 15:56:00,AQUA_M-T,Brasil,BAHIA,ILHÉUS,Mata Atlântica,4,0.0,0.0,47.5,-14.929,-39.076

````

---

## Tipos recomendados e domínios

- `DataHora`: `datetime64[ns, UTC]`. Parse com `utc=True`.
- `Satelite`, `Pais`, `Estado`, `Municipio`, `Bioma`: `string` ou `category` se memória for um limitante.
- `DiaSemChuva`: `Int64` (inteiro com suporte a nulos).
- `Precipitacao`, `RiscoFogo`, `FRP`, `Latitude`, `Longitude`: `float64`.
- Regras de domínio:
  - `RiscoFogo` entre 0.0 e 1.0, fora disso sinaliza dado inválido.
  - `DiaSemChuva` maior ou igual a 0.
  - `Latitude` em [-90, 90], `Longitude` em [-180, 180].
  - `Precipitacao` e `FRP` maior ou igual a 0.

---

## Sentinelas de ausentes e limpeza

- Valor sentinela para dados inválidos: `-999` em `DiaSemChuva`, `Precipitacao`, `RiscoFogo`.
- Tratamento recomendado:
  - Converter `-999` para `NaN` logo no `read_csv`.
  - Validar domínios e transformar valores fora de faixa em `NaN`.
  - Campos textuais podem vir vazios fora do Brasil, especialmente `Bioma`.

---

## Parsing com pandas - exemplo

```python
import pandas as pd
from pathlib import Path

NA_SENTINELS = [-999, "-999"]

DTYPE_MAP = {
    "DataHora": "string",
    "Satelite": "string",
    "Pais": "string",
    "Estado": "string",
    "Municipio": "string",
    "Bioma": "string",
    "DiaSemChuva": "Int64",            # inteiro com nulos
    "Precipitacao": "float64",
    "RiscoFogo": "float64",
    "FRP": "float64",
    "Latitude": "float64",
    "Longitude": "float64",
}

def load_bdqueimadas_csv(fp: Path, chunksize: int = 250_000):
    reader = pd.read_csv(
        fp,
        chunksize=chunksize,
        dtype=DTYPE_MAP,
        na_values=NA_SENTINELS,
        encoding="utf-8",
        sep=",",
        low_memory=False,
    )
    for i, chunk in enumerate(reader, 1):
        # DataHora -> datetime UTC
        chunk["DataHora"] = pd.to_datetime(
            chunk["DataHora"], format="%Y/%m/%d %H:%M:%S", errors="coerce", utc=True
        )

        # Correções de domínio
        chunk.loc[~chunk["RiscoFogo"].between(0.0, 1.0, inclusive="both"), "RiscoFogo"] = pd.NA
        chunk.loc[chunk["DiaSemChuva"].lt(0), "DiaSemChuva"] = pd.NA
        chunk.loc[chunk["Precipitacao"].lt(0), "Precipitacao"] = pd.NA
        chunk.loc[chunk["FRP"].lt(0), "FRP"] = pd.NA
        chunk.loc[~chunk["Latitude"].between(-90, 90), "Latitude"] = pd.NA
        chunk.loc[~chunk["Longitude"].between(-180, 180), "Longitude"] = pd.NA

        yield chunk

# Uso
# from pathlib import Path
# for df in load_bdqueimadas_csv(Path("data/raw/BDQUEIMADAS/export_2014.csv")):
#     process(df)
```

---

## Checks de qualidade

* Duplicatas exatas: mesma `DataHora`, `Latitude`, `Longitude`, `Satelite`. Se necessário, manter a de maior `FRP` quando houver conflito.
* Consistência de tempo: `DataHora` non-null, monotonicidade não é garantida, então ordenar antes de janelas temporais.
* Coordenadas: filtrar pontos fora do retângulo do Brasil quando a análise for focada no território nacional.
* `RiscoFogo` fora de [0, 1]: marcar como inválido e investigar.
* `DiaSemChuva` negativo: tratar como ausente.
* `Precipitacao` negativa: tratar como ausente.

---

## Integração com INMET - ideias

* Emparelhamento espaço-tempo:

  * Chave de tempo: arredondar `DataHora` para hora cheia ou criar janelas como `±3h`.
  * Chave espacial: vizinho mais próximo de estações INMET dentro de um raio, ex. 50 km, usando Haversine.
* Features meteorológicas locais na hora do foco:

  * Precipitação, umidade relativa, temperatura e vento do INMET como explicativas para FRP e ocorrência do foco.
* Agregações:

  * Por município, bioma, estado, semana e mês.
  * Estatísticas de FRP e distribuição de RiscoFogo.

---

## Layout sugerido de pastas e nomes

```
data/
  raw/
    BDQUEIMADAS/
      csv/
        2014/
          export_2014_01.csv
          export_2014_02.csv
        2015/
          export_2015_*.csv
  processed/
    BDQUEIMADAS/
      bdq_YYYY.parquet
      bdq_all.parquet
images/
  eda/
    bdqueimadas/
      Histograma_FRP.png
      Mapa_quicklook_YYYYMM.png
doc/
  bdqueimadas_dataset.md
```

Convenções de arquivo:

* Sem espaços e sem acentos quando possível.
* Datas no padrão `YYYYMMDD` no nome ajudam em ordenação.
* Sempre evitar caracteres especiais em nomes.

---

## Consultas analíticas de exemplo

Top 10 municípios por mediana de FRP em um ano:

```python
import pandas as pd

df = pd.read_parquet("data/processed/BDQUEIMADAS/bdq_2014.parquet")
top = (
    df.dropna(subset=["FRP"])
      .groupby("Municipio", as_index=False)["FRP"].median()
      .sort_values("FRP", ascending=False)
      .head(10)
)
print(top)
```

Série mensal de contagem de focos por bioma:

```python
q = df.copy()
q["mes"] = q["DataHora"].dt.to_period("M").dt.to_timestamp()
out = (
    q.groupby(["Bioma", "mes"], as_index=False)
     .size()
     .rename(columns={"size": "focos"})
)
```

---

## Glossário rápido

* FRP: Fire Radiative Power, potência radiativa do fogo estimada a partir do sensor. Unidade: MW.
* RiscoFogo: índice de risco de fogo previsto para o dia no local do foco, adimensional entre 0 e 1.
* GADM: base global de limites administrativos.
* IBGE 2000 e 2004: referências usadas para municípios e biomas no Brasil.
* WGS84: datum geodésico padrão para Latitude e Longitude.

---
