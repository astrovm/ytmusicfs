.PHONY: clean clean-test clean-pyc clean-build test test-all lint format install dev-install build publish help

help:
	@echo "clean - remove all build, test, coverage, and Python artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-test - remove test and coverage artifacts"
	@echo "lint - check style with flake8"
	@echo "format - format code with black and isort"
	@echo "test - run tests quickly with the default Python"
	@echo "test-all - run tests on every Python version"
	@echo "coverage - check code coverage quickly with the default Python"
	@echo "install - install the package to the active Python site-packages"
	@echo "dev-install - install the package in development mode"
	@echo "build - build package for distribution"
	@echo "publish - package and upload a release"

clean: clean-build clean-pyc clean-test

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test:
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache/

lint:
	flake8 ytmusicfs tests
	black --check ytmusicfs tests
	isort --check-only --profile black ytmusicfs tests

format:
	black ytmusicfs tests
	isort --profile black ytmusicfs tests

test:
	pytest

test-all:
	pytest

coverage:
	pytest --cov=ytmusicfs tests/
	coverage report -m
	coverage html

install: clean
	pip install .

dev-install: clean
	pip install -e ".[dev]"

build: clean
	python -m build

publish: build
	twine check dist/*
	twine upload dist/*
