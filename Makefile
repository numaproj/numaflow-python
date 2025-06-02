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
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sinker -I=pynumaflow/proto/sinker --python_out=pynumaflow/proto/sinker --grpc_python_out=pynumaflow/proto/sinker  pynumaflow/proto/sinker/*.proto
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/mapper -I=pynumaflow/proto/mapper --python_out=pynumaflow/proto/mapper --grpc_python_out=pynumaflow/proto/mapper  pynumaflow/proto/mapper/*.proto
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/reducer -I=pynumaflow/proto/reducer --python_out=pynumaflow/proto/reducer --grpc_python_out=pynumaflow/proto/reducer  pynumaflow/proto/reducer/*.proto
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sourcetransformer -I=pynumaflow/proto/sourcetransformer --python_out=pynumaflow/proto/sourcetransformer --grpc_python_out=pynumaflow/proto/sourcetransformer  pynumaflow/proto/sourcetransformer/*.proto
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sideinput -I=pynumaflow/proto/sideinput --python_out=pynumaflow/proto/sideinput --grpc_python_out=pynumaflow/proto/sideinput  pynumaflow/proto/sideinput/*.proto
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sourcer -I=pynumaflow/proto/sourcer --python_out=pynumaflow/proto/sourcer --grpc_python_out=pynumaflow/proto/sourcer  pynumaflow/proto/sourcer/*.proto
	python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/serving -I=pynumaflow/proto/serving --python_out=pynumaflow/proto/serving --grpc_python_out=pynumaflow/proto/serving  pynumaflow/proto/serving/*.proto


	sed -i '' 's/^\(import.*_pb2\)/from . \1/' pynumaflow/proto/*/*.py
