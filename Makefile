.DEFAULT_GOAL:=help

.PHONY: dev
dev: ## Installs adapter in develop mode along with development dependencies
	@\
	pip install -e . -r requirements.txt -r dev-requirements.txt -r dagger/requirements.txt && pre-commit install

.PHONY: dev-uninstall
dev-uninstall: ## Uninstalls all packages while maintaining the virtual environment
               ## Useful when updating versions, or if you accidentally installed into the system interpreter
	pip freeze | grep -v "^-e" | cut -d "@" -f1 | xargs pip uninstall -y
	pip uninstall -y dbt-spark

.PHONY: lint
lint: ## Runs flake8 and mypy code checks against staged changes.
	@\
	pre-commit run --all-files

.PHONY: unit
unit: ## Runs unit tests with py38.
	@\
	python -m pytest tests/unit

.PHONY: test
test: ## Runs unit tests with py38 and code checks against staged changes.
	@\
	python -m pytest tests/unit; \
	pre-commit run --all-files; \
	python dagger/run_dbt_spark_tests.py --profile spark_session

.PHONY: clean
	@echo "cleaning repo"
	@git clean -f -X

.PHONY: help
help: ## Show this help message.
	@echo 'usage: make [target]'
	@echo
	@echo 'targets:'
	@grep -E '^[7+a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

.PHONY: docker-prod
docker-prod:
	docker build -f docker/Dockerfile -t dbt-spark .
