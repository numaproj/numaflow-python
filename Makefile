clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format: clean
	poetry run black pynumaflow/ tests/ examples/


lint: format
	poetry run ruff check --fix .


test:
	poetry run pytest tests/


requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes


setup:
	poetry install --with dev --no-root


proto:
	python3 -m grpc_tools.protoc -I=pynumaflow/function/proto --python_out=pynumaflow/function/proto --grpc_python_out=pynumaflow/function/proto  pynumaflow/function/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/sink/proto --python_out=pynumaflow/sink/proto --grpc_python_out=pynumaflow/sink/proto  pynumaflow/sink/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/map/proto --python_out=pynumaflow/map/proto --grpc_python_out=pynumaflow/map/proto  pynumaflow/map/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/mapstream/proto --python_out=pynumaflow/mapstream/proto --grpc_python_out=pynumaflow/mapstream/proto  pynumaflow/mapstream/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/reduce/proto --python_out=pynumaflow/reduce/proto --grpc_python_out=pynumaflow/reduce/proto  pynumaflow/reduce/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/sourcetransform/proto --python_out=pynumaflow/sourcetransform/proto --grpc_python_out=pynumaflow/sourcetransform/proto  pynumaflow/sourcetransform/proto/*.proto

	sed -i '' 's/^\(import.*_pb2\)/from . \1/' pynumaflow/*/proto/*.py
