.PHONY: install sample ingest test lint dbt-debug dbt-build docs

install:
	python -m pip install -e ".[dev]"

sample:
	python -m saas_elt.cli generate-sample --rows 250

ingest:
	python -m saas_elt.cli ingest --run-date 2026-09-19

test:
	pytest -q

lint:
	ruff check src tests airflow

dbt-debug:
	dbt debug --profiles-dir .

dbt-build:
	dbt deps && dbt build --profiles-dir . --target dev

docs:
	dbt docs generate --profiles-dir . --target dev

