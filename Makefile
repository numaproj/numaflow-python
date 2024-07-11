clean:
	@rm -rf build .eggs *.egg-info
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
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/sinker --python_out=pynumaflow/proto/sinker --grpc_python_out=pynumaflow/proto/sinker  pynumaflow/proto/sinker/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/mapper --python_out=pynumaflow/proto/mapper --grpc_python_out=pynumaflow/proto/mapper  pynumaflow/proto/mapper/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/mapstreamer --python_out=pynumaflow/proto/mapstreamer --grpc_python_out=pynumaflow/proto/mapstreamer  pynumaflow/proto/mapstreamer/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/reducer --python_out=pynumaflow/proto/reducer --grpc_python_out=pynumaflow/proto/reducer  pynumaflow/proto/reducer/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/sourcetransformer --python_out=pynumaflow/proto/sourcetransformer --grpc_python_out=pynumaflow/proto/sourcetransformer  pynumaflow/proto/sourcetransformer/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/sideinput --python_out=pynumaflow/proto/sideinput --grpc_python_out=pynumaflow/proto/sideinput  pynumaflow/proto/sideinput/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/sourcer --python_out=pynumaflow/proto/sourcer --grpc_python_out=pynumaflow/proto/sourcer  pynumaflow/proto/sourcer/*.proto
	python3 -m grpc_tools.protoc -I=pynumaflow/proto/batchmapper --python_out=pynumaflow/proto/batchmapper --grpc_python_out=pynumaflow/proto/batchmapper  pynumaflow/proto/batchmapper/*.proto


	sed -i '' 's/^\(import.*_pb2\)/from . \1/' pynumaflow/proto/*/*.py
