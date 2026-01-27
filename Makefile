.PHONY: install install-ui test lint run-demo ui verify format clean

install:
	pip install -e ".[dev]"

install-ui:
	pip install -e ".[dev,ui]"

test:
	pytest

lint:
	ruff check src tests

run-demo:
	python -m psx_ohlcv.demo

ui:
	streamlit run src/psx_ohlcv/ui/app.py

verify:
	python scripts/verify_features.py

format:
	ruff format .
	ruff check --fix .

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name ".ruff_cache" -exec rm -rf {} +
