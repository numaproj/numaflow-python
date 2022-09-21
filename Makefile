clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format: clean
	poetry run black pynumaflow/


lint: format
	poetry run flake8 .


test:
	poetry run pytest pynumaflow/tests/


requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes

# TODO: proto file generate