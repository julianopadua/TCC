# Follow-up de Decisões - Projeto TCC (Previsão de Queimadas)

> Documento vivo para registrar decisões de engenharia de dados, convenções e trade-offs ao longo do TCC. Alinhado aos objetivos/metodologia definidos no manuscrito do TCC. 

## Sumário
- [1. Estrutura de pastas e do projeto](#1-estrutura-de-pastas-e-do-projeto)
- [2. Extração inicial - BD Queimadas (INPE)](#2-extração-inicial--bd-queimadas-inpe)
  - [2.1 Escopo e fonte](#21-escopo-e-fonte)
  - [2.2 Parâmetros de exportação adotados](#22-parâmetros-de-exportação-adotados)
  - [2.3 Fluxo manual atual (passo a passo)](#23-fluxo-manual-atual-passo-a-passo)
  - [2.4 Convenção de nomenclatura de arquivos](#24-convenção-de-nomenclatura-de-arquivos)
  - [2.5 Justificativa técnica da convenção](#25-justificativa-técnica-da-convenção)
  - [2.6 Riscos/limitações conhecidos](#26-riscoslimitações-conhecidos)
  - [2.7 Próximos passos (automatização)](#27-próximos-passos-automatização)
- [3. Scraper - INMET](#3-scraper--inmet)

---

## 1. Estrutura de pastas e do projeto
_em construção…_

---

## 2. Extração inicial - BD Queimadas (INPE)

### 2.1 Escopo e fonte
Os dados de focos de calor são obtidos via BDQueimadas (módulo TerraBrasilis/INPE), um WebGIS que permite filtrar e exportar pontos (focos) por recortes espaciais/temporais e camadas (país, estado, bioma, etc.), com exportação em CSV/GeoJSON/KML/Shapefile.   
Na própria interface “BDQueimadas”, os filtros incluem Continentes, Países, Estados, Municípios, UCs/TIs, período (Data Início/Fim), satélites (incluindo a opção “Satélite de referência (Aqua Tarde)”) e Biomas (Brasil); a aba “Exportar Dados” envia o arquivo para o e-mail informado no formato escolhido. 

**Definição operacional de “foco”:** foco indica a existência de fogo em um elemento de resolução (pixel) da imagem de satélite, cuja dimensão varia conforme o sensor (≈375 m a 5×4 km). 

**Por que usar “Satélite de referência (Aqua Tarde)”:** o INPE utiliza um satélite de referência para garantir comparabilidade temporal das séries; a própria “Situação Atual” do portal e notas técnicas estaduais baseadas no INPE destacam que as comparações interanuais usam **apenas** o satélite de referência (AQUA Tarde). 

### 2.2 Parâmetros de exportação adotados
- **Continentes:** América do Sul  
- **Países:** Brasil  
- **Estados:** Todos os estados  
- **Municípios / UCs/TIs:** em branco  
- **Satélites:** Satélite de referência (Aqua Tarde)  
- **Biomas (Brasil):** Todos  
- **Janela temporal:** 1º de janeiro a 31 de dezembro de cada ano  
- **Formato de exportação:** CSV (enviado por e-mail; portal informa uso do e-mail apenas para envio e estatísticas de acesso) 

### 2.3 Fluxo manual atual (passo a passo)
1. Acessar o BDQueimadas no TerraBrasilis e configurar os filtros da seção 2.2.   
2. Definir o intervalo anual completo (01/01–31/12) e **Aplicar**.   
3. Informar o e-mail na seção **Exportar Dados** e selecionar **CSV**.   
4. Receber um **ZIP** por e-mail contendo o CSV e **extrair** localmente.   
5. O arquivo chega com padrão **`exportador_YYYY-MM-DD HH:MM:SS.ssssss.csv`** (timestamp da exportação).  
6. Renomear conforme a convenção definida na seção 2.4.

### 2.4 Convenção de nomenclatura de arquivos
- **Original (do portal):**  
  `exportador_YYYY-MM-DD HH:MM:SS.ssssss.csv`  
- **Padrão adotado no projeto:**  
  `exportador_YYYY-MM-DD_ref_YYYY.csv`  
  **Ex.:** `exportador_2025-09-16_ref_2024.csv`

### 2.5 Justificativa técnica da convenção
- **Rastreabilidade dupla (evento x conteúdo):** preserva a **data da exportação** (audit trail) e torna explícito o **ano de referência** do conteúdo do CSV (01/01–31/12 daquele ano), reduzindo ambiguidade quando múltiplas exportações ocorrem no mesmo dia.  
- **Consistência com a prática do INPE (séries anuais):** a análise interanual oficial utiliza **apenas** o **satélite de referência**; ao explicitar `_ref_YYYY`, a série fica alinhada à comparabilidade temporal recomendada/inferida pelo próprio portal e relatórios técnicos estaduais baseados no INPE. 0}  
- **Prevenção de colisões e legibilidade em pipelines:** o sufixo `_ref_YYYY` favorece _parsing_ determinístico (regex simples) e organização por partição (e.g., `year=YYYY`) em data lakes, sem depender de metadados externos.  
- **Diagnóstico de regressões:** caso o INPE altere camadas/atributos, é possível correlacionar a mudança com a **data da extração** embutida no nome, mantendo a série legível para _debug_.  
- **Compatibilidade com múltiplas fontes:** a mesma convenção pode ser espelhada para outros provedores anuais (e.g., INMET), facilitando _joins_ por chave `ref_ano`.

### 2.6 Riscos/limitações conhecidos
- **Lacunas do satélite de referência:** houve interrupções conhecidas no MODIS/AQUA (ex.: 31/03/2022–13/04/2022), afetando séries que dependem exclusivamente do satélite de referência; é prudente registrar _flags_ de disponibilidade e, quando necessário, considerar satélites alternativos (VIIRS) com devida harmonização. 1}  
- **Diferenças de sensor/resolução entre satélites:** VIIRS (≈375 m) detecta mais focos que MODIS (≈1 km), o que inviabiliza comparações diretas “Todos os satélites” sem normalização; manter o **AQUA Tarde** como base de série reduz esse viés. 2}  
- **Semântica de “foco”:** um foco é uma detecção por pixel; não é sinônimo de “número de incêndios” nem “área queimada”. Interpretar métricas com essa ressalva. 3}

### 2.7 Próximos passos (automatização)
- Implementar _scraper/exporter_ reproduzindo fielmente os filtros da UI do BDQueimadas (incluindo _headers_ e o _payload_ necessário para geração e envio por e-mail), ou, se disponível, migrar para endpoints/documentação estável do TerraBrasilis. 4}  
- Padronizar _ingest_ para salvar diretamente como `year=YYYY/…/exportador_YYYY-MM-DD_ref_YYYY.csv` com _hash_ do conteúdo para controle de versão.  
- Criar rotina de **validação** pós-download (contagem de linhas, campos esperados, faixa de datas, distribuição por UF/bioma) e _data quality checks_ (percentual de nulos, domínios).  
- Registrar **metadados**: filtros, _query hash_, data/hora UTC da extração, versão do dicionário de atributos.  

---

## 3. Scraper - INMET
_em construção…_

---
