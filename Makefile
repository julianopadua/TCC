# =============================================================================
# Makefile — SSOT (Single Source of Truth) da CLI do projeto TCC
# Compatible: Windows cmd.exe, PowerShell, Git Bash, Linux/macOS
# =============================================================================
# Uso rapido:
#   make help
#   make audit SCENARIO=base_E_with_rad_knn_calculated
#   make audit-deep
#   make train
#   make train TRAIN_SCENARIO=tf_E_minirocket MODEL=random_forest VARIATIONS=1
#
# Convencoes:
#   - Cada alvo real e anotado com `## descricao` para alimentar `make help`.
#   - Variaveis com `?=` podem ser sobrescritas via linha de comando.
#   - EXTRA="..." e anexado ao final do comando Python para flags ad-hoc.
# =============================================================================

# Usar python como shell para tudo que precisar de logica cross-platform.
# Receitas simples de uma linha funcionam em qualquer shell.
MAKEFLAGS += --no-print-directory
METHODS=ewma_lags minirocket

# -----------------------------------------------------------------------------
# Configuracao
# -----------------------------------------------------------------------------
PY            ?= python
SRC           := src

# Cenarios
SCENARIO       ?= base_E_with_rad_knn_calculated
TRAIN_SCENARIO ?= tf_E_champion

# Modelagem
MODEL          ?= xgboost
VARIATIONS     ?= 1
ON_EXIST       ?= skip
ARTICLE        ?= 1

# Memoria / streaming
BATCH_ROWS     ?= 500000
MAX_TRAIN_ROWS ?=
MAX_TEST_ROWS  ?=

# Auditoria
AUDIT_ROOT     := data/_article/1_datasets_with_fusion

# Passagens extras (ex.: EXTRA="--dry-run")
EXTRA          ?=

# Flag --article condicional
ARTICLE_FLAG   := $(if $(filter 1 true yes on,$(ARTICLE)),--article,)

# Overrides opcionais de linhas
MAX_TRAIN_FLAG := $(if $(strip $(MAX_TRAIN_ROWS)),--max-train-rows $(MAX_TRAIN_ROWS),)
MAX_TEST_FLAG  := $(if $(strip $(MAX_TEST_ROWS)),--max-test-rows $(MAX_TEST_ROWS),)

# -----------------------------------------------------------------------------
# Help auto-documentado (Python — compativel com Windows cmd.exe)
# -----------------------------------------------------------------------------
.DEFAULT_GOAL := help

.PHONY: help
help: ## Lista comandos disponiveis
	$(PY) scripts/make_help.py Makefile
	@$(PY) -c "print(''); print('Variaveis (override via linha de comando):'); print('  SCENARIO=$(SCENARIO)'); print('  TRAIN_SCENARIO=$(TRAIN_SCENARIO)'); print('  MODEL=$(MODEL)   VARIATIONS=$(VARIATIONS)   ON_EXIST=$(ON_EXIST)   ARTICLE=$(ARTICLE)'); print('  BATCH_ROWS=$(BATCH_ROWS)   MAX_TRAIN_ROWS=$(MAX_TRAIN_ROWS)   MAX_TEST_ROWS=$(MAX_TEST_ROWS)'); print('  EXTRA=$(EXTRA)')"

##@ Correcao de duplicatas (dedup upstream)

.PHONY: dedupe-dry
dedupe-dry: ## Dry-run: duplicatas (cidade_norm+ts_hour) em data/modeling/base_*
	$(PY) -m src.dedupe_base_datasets --stage modeling $(EXTRA)

.PHONY: dedupe
dedupe: ## Remove duplicatas por chave em data/modeling/base_* — use EXTRA=--full-row p/ modo legado
	$(PY) -m src.dedupe_base_datasets --stage modeling --apply $(EXTRA)

.PHONY: dedupe-coords-dry
dedupe-coords-dry: ## Dry-run: reporta duplicatas de chave em 0_datasets_with_coords/
	$(PY) -m src.dedupe_base_datasets --stage coords $(EXTRA)

.PHONY: dedupe-coords
dedupe-coords: ## Remove duplicatas de (cidade_norm, ts_hour) em 0_datasets_with_coords/ (sobrescreve!)
	$(PY) -m src.dedupe_base_datasets --stage coords --apply $(EXTRA)

##@ Engenharia de features fisicas (regenera base_*_calculated)

.PHONY: physics-features
physics-features: ## Regenera base_*_calculated via feature_engineering_physics.py
	$(PY) $(SRC)/feature_engineering_physics.py $(EXTRA)

##@ Pipeline do artigo (coords -> GEE -> EDA)

.PHONY: pipeline
pipeline: ## Roda pipeline do artigo (coords + GEE + EDA)
	$(PY) $(SRC)/article/run_pipeline.py $(EXTRA)

.PHONY: pipeline-coords
pipeline-coords: ## Apenas enriquecimento de coordenadas
	$(PY) $(SRC)/article/run_pipeline.py --only-coords $(EXTRA)

.PHONY: pipeline-eda
pipeline-eda: ## Apenas EDA (plots + correlacoes)
	$(PY) $(SRC)/article/run_pipeline.py --only-eda $(EXTRA)

##@ Fusao temporal e champion base

.PHONY: fusion
fusion:
	$(PY) -m src.article.article_orchestrator \
		--scenario $(SCENARIO) \
		--methods $(METHODS) \
		$(EXTRA)

.PHONY: champion
champion:
	$(PY) -m src.article.article_orchestrator \
		--scenario $(SCENARIO) \
		--methods $(METHODS) \
		$(EXTRA)

.PHONY: champion-overwrite
champion-overwrite:
	$(PY) -m src.article.article_orchestrator \
		--scenario $(SCENARIO) \
		--methods $(METHODS) \
		--overwrite \
		$(EXTRA)
##@ Auditoria do pipeline (detecta duplicacao em cada estagio)

.PHONY: audit-pipeline
audit-pipeline: ## Audita todos os estagios (modeling -> calculated -> coords -> fusion). Grava em data/_article/_audits/
	$(PY) -m src.audit_pipeline --stage all $(EXTRA)

.PHONY: audit-stage
audit-stage: ## Audita um estagio especifico. Use: make audit-stage STAGE=modeling|calculated|coords|fusion
	$(PY) -m src.audit_pipeline --stage $(STAGE) $(EXTRA)

.PHONY: audit-pipeline-latest
audit-pipeline-latest: ## Abre/mostra o LATEST.md dos audits
	@$(PY) -c "from pathlib import Path; p=Path('data/_article/_audits/LATEST.md'); print(p.read_text(encoding='utf-8') if p.exists() else 'Nenhum audit encontrado. Rode make audit-pipeline.')"

.PHONY: audit-row-parity
audit-row-parity: ## Conta linhas: consolidated/INMET vs data/modeling/base_* (parquet). Gera _audits/*_row_parity/
	$(PY) -m src.audit_row_parity $(EXTRA)

##@ Auditoria de dados

.PHONY: audit
audit: ## Audita um cenario (schema/colunas/anos) -> audit.md
	$(PY) -m src.article.audit_fusion_dataset --scenario $(SCENARIO) $(EXTRA)

.PHONY: audit-deep
audit-deep: ## Auditoria profunda (pos_rate + NaN ratios + validacao 2003-2006)
	$(PY) -m src.article.audit_fusion_dataset --scenario $(SCENARIO) --deep $(EXTRA)

.PHONY: audit-all
audit-all: ## Audita todos os cenarios em 1_datasets_with_fusion/
	$(PY) -c "import subprocess,sys,glob,os; root='$(AUDIT_ROOT)'; [subprocess.run([sys.executable,'-m','src.article.audit_fusion_dataset','--scenario',os.path.basename(d.rstrip('/\\\\'))]) for d in sorted(glob.glob(root+'/*/')) if os.path.isdir(d)]"

.PHONY: audit-all-deep
audit-all-deep: ## Auditoria profunda em todos os cenarios
	$(PY) -c "import subprocess,sys,glob,os; root='$(AUDIT_ROOT)'; [subprocess.run([sys.executable,'-m','src.article.audit_fusion_dataset','--scenario',os.path.basename(d.rstrip('/\\\\')),'--deep']) for d in sorted(glob.glob(root+'/*/')) if os.path.isdir(d)]"

##@ Treino / teste de modelos (streaming seguro para RAM)

.PHONY: train
train: ## Treina modelo com parquets streamados (vars: TRAIN_SCENARIO, MODEL, VARIATIONS, BATCH_ROWS)
	$(PY) $(SRC)/train_runner.py run $(ARTICLE_FLAG) -s $(TRAIN_SCENARIO) -m $(MODEL) -v $(VARIATIONS) --on-exist $(ON_EXIST) --batch-rows $(BATCH_ROWS) $(MAX_TRAIN_FLAG) $(MAX_TEST_FLAG) $(EXTRA)

.PHONY: train-dry
train-dry: ## Valida o plano sem carregar dados
	$(PY) $(SRC)/train_runner.py run $(ARTICLE_FLAG) -s $(TRAIN_SCENARIO) -m $(MODEL) -v $(VARIATIONS) --dry-run $(EXTRA)

.PHONY: train-low-mem
train-low-mem: ## Treino conservador (batch_rows=200k, max_train=4M) para maquinas com pouca RAM
	$(PY) $(SRC)/train_runner.py run $(ARTICLE_FLAG) -s $(TRAIN_SCENARIO) -m $(MODEL) -v $(VARIATIONS) --on-exist $(ON_EXIST) --batch-rows 200000 --max-train-rows 4000000 --max-test-rows 1000000 $(EXTRA)

.PHONY: train-all-variations
train-all-variations: ## Treina variacoes 1-4 para MODEL em TRAIN_SCENARIO
	$(PY) $(SRC)/train_runner.py run $(ARTICLE_FLAG) -s $(TRAIN_SCENARIO) -m $(MODEL) -v 1,2,3,4 --on-exist $(ON_EXIST) --batch-rows $(BATCH_ROWS) $(EXTRA)

.PHONY: train-interactive
train-interactive: ## Menu interativo legado (input)
	$(PY) $(SRC)/train_runner.py interactive

.PHONY: list-scenarios
list-scenarios: ## Lista chaves de modeling_scenarios
	$(PY) $(SRC)/train_runner.py list-scenarios

.PHONY: list-models
list-models: ## Lista modelos disponiveis no ambiente
	$(PY) $(SRC)/train_runner.py list-models

.PHONY: describe-variations
describe-variations: ## Descreve variacoes 1-4 para MODEL
	$(PY) $(SRC)/train_runner.py describe-variations -m $(MODEL)

##@ Consolidacao / visualizacao de resultados

.PHONY: consolidate
consolidate: ## Consolida metrics_*.json em tabelas
	$(PY) $(SRC)/run_results_consolidator.py $(EXTRA)

.PHONY: viz
viz: ## Gera visualizacoes dos resultados
	$(PY) $(SRC)/run_results_visualization.py $(EXTRA)

##@ Utilitarios

.PHONY: clean-logs
clean-logs: ## Remove logs com mais de 7 dias
	$(PY) -c "import pathlib,time; [p.unlink() for p in pathlib.Path('logs').rglob('*') if p.is_file() and (time.time()-p.stat().st_mtime)>604800]"

.PHONY: doctor
doctor: ## Sanity-check do ambiente (python + pyarrow + pandas + sklearn)
	$(PY) -c "import sys,pyarrow,pandas,sklearn; print('python',sys.version.split()[0]); print('pyarrow',pyarrow.__version__); print('pandas',pandas.__version__); print('sklearn',sklearn.__version__)"

.PHONY: print-config
print-config: ## Mostra os principais paths resolvidos do config.yaml
	$(PY) -c "from src.utils import loadConfig; import json; c=loadConfig(); print(json.dumps({'root':c['paths']['root'],'data':c['paths']['data']},indent=2))"
