lint:
	ruff check src tests

test:
	pytest -q
