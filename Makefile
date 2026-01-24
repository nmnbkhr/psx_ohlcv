.PHONY: install install-ui test lint run-demo ui

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
