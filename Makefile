# pbp-unify: NBA + ESPN play-by-play unified with DuckDB, dbt and Prefect.
#   make all        clean clone -> venv, flow (extract, bronze, dbt build, report), evaluation, for every
#                   2025-26 NBA game (NBA feed + ESPN) and every 2026 WNBA game (ESPN only); ~12 GB of memory
#   make team       the same for one team per league, by default the Wizards and Mystics (~2 minutes)
#   make flow       re-run the Prefect flow (landed files are reused; only new games are fetched)
#   make flow-nba   NBA target only; make flow-wnba  WNBA target only (ESPN)
#   Other targets:  make flow NBA_TARGET=nba:2025-26:BOS WNBA_TARGET=wnba:2026:LVA
#   make dbt        dbt build only (bronze must already be loaded); LEAGUES=wnba rebuilds one league
#   make eval       matcher evaluation -> reports/matcher_evaluation.md
#   make clean      drop dbt output, landing files and caches; keeps bronze (the payload history)
#   make clean-all  delete everything under data/, bronze included
PY      := .venv/bin/python
DBT     := ../.venv/bin/dbt
NBA_TARGET  ?= nba:2025-26:ALL
WNBA_TARGET ?= wnba:2026:ALL
TEAM_NBA    ?= nba:2025-26:WAS
TEAM_WNBA   ?= wnba:2026:WAS

.PHONY: all team setup flow flow-nba flow-wnba dbt docs eval clean clean-all

all: setup flow eval

team: setup
	$(MAKE) flow eval NBA_TARGET=$(TEAM_NBA) WNBA_TARGET=$(TEAM_WNBA)

setup: .venv/.installed

.venv/.installed: requirements.lock pyproject.toml
	python3 -m venv .venv
	.venv/bin/pip install -q --upgrade pip
	.venv/bin/pip install -q -r requirements.lock
	.venv/bin/pip install -q -e .
	touch $@

flow: setup
	PREFECT_LOGGING_LEVEL=INFO $(PY) flows/pbp_flow.py --target $(NBA_TARGET) --target $(WNBA_TARGET)

flow-nba: setup
	PREFECT_LOGGING_LEVEL=INFO $(PY) flows/pbp_flow.py --target $(NBA_TARGET)

flow-wnba: setup
	PREFECT_LOGGING_LEVEL=INFO $(PY) flows/pbp_flow.py --target $(WNBA_TARGET)

dbt: setup
	cd dbt && $(DBT) build --profiles-dir . $(if $(LEAGUES),--vars '{leagues: [$(LEAGUES)]}')

docs: setup
	cd dbt && $(DBT) docs generate --profiles-dir .

eval: setup
	$(PY) -m pbp.evaluate

clean:
	rm -rf data/landing data/cache dbt/target dbt/logs
	@if [ -x $(PY) ]; then $(PY) -m pbp.clean; fi

clean-all:
	rm -rf data dbt/target dbt/logs
