# Hidden Technical Debt in Machine Learning Systems (Sculley et al., 2015)

## Ficha técnica

Autores: D. Sculley; G. Holt; D. Golovin; E. Davydov; T. Phillips; D. Ebner; V. Chaudhary; M. Young; J. F. Crespo; D. Dennison.
Tipo: Ensaio técnico sobre dívidas técnicas específicas de sistemas de ML.
Escopo: dívida técnica em ML e fatores de risco em produção
tópicos centrais incluem boundary erosion, entanglement, feedback loops, data dependencies, pipeline jungles, glue code, configuração, monitoramento e mudanças no mundo externo. 

---

## Trechos centrais curtos

> “Desenvolver ML é rápido e barato. Manter em produção é difícil e caro.” 

> “CACE: Changing Anything Changes Everything.” 

> “Apenas uma pequena fração do sistema é ML. O resto é infraestrutura.” 

> “Pipeline jungles e glue code elevam a dívida e bloqueiam inovação.” 

> “Laços de realimentação e consumidores não declarados criam acoplamentos ocultos.” 

---

## Por que importa para o seu TCC (usar em Data Pipeline Foundation)

1. Pipeline claro evita “pipeline jungle”
   O artigo mostra que cadeias de scrapes, joins e amostragens sem desenho holístico viram selva de pipeline. Para o seu ETL INMET + BDQueimadas, padronizar estágios, artefatos versionados e checagens automáticas reduz custo de manutenção e risco de erro. 

2. Gestão de dependências de dados
   Dependências instáveis e subutilizadas geram fragilidade. Versionar sinais, rodar rotinas regulares de leave-one-feature-out e mapear a árvore de dependências de dados do dataset consolidado dá segurança para atualizar fontes e features. 

3. Contenção de acoplamento e CACE
   Mudanças em uma feature ou amostragem podem afetar todo o sistema. Separar contratos entre estágios do pipeline, isolar modelos e evitar “correction cascades” reduz o efeito dominó em seus experimentos de bases com e sem imputação. 

4. Configuração como código auditável
   Grande parte da complexidade vive na configuração. Centralizar parâmetros do pipeline e dos treinos em arquivos versionados com validações automáticas previne erros de execução e inconsistências entre datasets anuais. 

5. Monitoramento e mudanças no mundo
   Distribuições de rótulos e de previsões precisam ser monitoradas no tempo. Para séries anuais por bioma e estação, métricas por fatias e alertas sobre deriva de dados ajudam a detectar quebras após reprocessamentos ou atualizações de fonte. 

---

## Tabela síntese conceito → ação no seu pipeline

| Conceito                    | Risco                              | Ação concreta                                                       |
| --------------------------- | ---------------------------------- | ------------------------------------------------------------------- |
| Pipeline jungle             | Erros caros e difíceis de rastrear | Desenho único de ETL com DAG explícito e artefatos bem definidos    |
| Glue code                   | Aprisiona a stack                  | Interfaces comuns para leitura escrita e featurização substituíveis |
| Dependência instável        | Quebra silenciosa                  | Versionar sinais e schemas congelados por release                   |
| Dependência subutilizada    | Fragilidade sem ganho              | Auditoria periódica leave-one-out e poda de features                |
| CACE                        | Efeito dominó                      | Contratos de dados entre estágios e testes de regressão de features |
| Consumidores não declarados | Acoplamento oculto                 | Catálogo de outputs com controle de acesso e SLAs                   |
| Laços de feedback           | Viés acumulado                     | Partições “blindadas” de validação e aleatorização controlada       |
| Configuração                | Erros manuais                      | Config como código com revisão e validações automáticas             |
| Mundo externo muda          | Deriva                             | Monitoramento de distribuição por fatias e alertas                  |



---

## Onde citar no TCC

Seção Fundamentos de Data Pipeline e Engenharia de ML em Produção
use para justificar decisões de desenho do seu ETL consolidado INMET + BDQueimadas, versionamento de dados e configurações, testes e monitoramento de deriva, além do racional para manter pipelines enxutos e auditáveis. 
