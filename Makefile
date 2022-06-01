test:
	poetry run coverage run --source=dlt_pipeline --omit='*tests/*' --branch -m pytest tests
	poetry run coverage report -m --skip-empty

black:
	poetry run black . --check

lint:
	poetry run pylint dlt_pipeline

isort:
	poetry run isort dlt_pipeline --check

mypy:
	poetry run mypy dlt_pipeline

all_checks: isort black test lint mypy

package_dlt_pipeline:
	poetry build

deploy_dlt_pipeline: package_dlt_pipeline
	poetry run databricks fs cp --overwrite dist/dlt_pipeline-0.1.0-py3-none-any.whl dbfs:$(dbfs_destination)
