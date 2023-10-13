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
	python3 -m grpc_tools.protoc -I=pynumaflow/sinker/proto --python_out=pynumaflow/sinker/proto --grpc_python_out=pynumaflow/sinker/proto  pynumaflow/sinker/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/mapper/proto --python_out=pynumaflow/mapper/proto --grpc_python_out=pynumaflow/mapper/proto  pynumaflow/mapper/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/mapstreamer/proto --python_out=pynumaflow/mapstreamer/proto --grpc_python_out=pynumaflow/mapstreamer/proto  pynumaflow/mapstreamer/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/reducer/proto --python_out=pynumaflow/reducer/proto --grpc_python_out=pynumaflow/reducer/proto  pynumaflow/reducer/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/sourcetransformer/proto --python_out=pynumaflow/sourcetransformer/proto --grpc_python_out=pynumaflow/sourcetransformer/proto  pynumaflow/sourcetransformer/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/sideinput/proto --python_out=pynumaflow/sideinput/proto --grpc_python_out=pynumaflow/sideinput/proto  pynumaflow/sideinput/proto/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/sourcer/proto --python_out=pynumaflow/sourcer/proto --grpc_python_out=pynumaflow/sourcer/proto  pynumaflow/sourcer/proto/*.proto


	sed -i '' 's/^\(import.*_pb2\)/from . \1/' pynumaflow/*/proto/*.py
