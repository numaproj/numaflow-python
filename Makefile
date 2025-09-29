clean:
	@rm -rf build .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format: clean
	poetry run black --exclude=pynumaflow/proto/ pynumaflow/ tests/ examples/


lint: format
	poetry run ruff check --fix .


test:
	poetry run pytest tests/


requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes


setup:
	poetry install --with dev --no-root

proto:
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sinker -I=pynumaflow/proto/sinker --python_out=pynumaflow/proto/sinker --grpc_python_out=pynumaflow/proto/sinker  pynumaflow/proto/sinker/*.proto
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/mapper -I=pynumaflow/proto/mapper --python_out=pynumaflow/proto/mapper --grpc_python_out=pynumaflow/proto/mapper  pynumaflow/proto/mapper/*.proto
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/reducer -I=pynumaflow/proto/reducer --python_out=pynumaflow/proto/reducer --grpc_python_out=pynumaflow/proto/reducer  pynumaflow/proto/reducer/*.proto
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sourcetransformer -I=pynumaflow/proto/sourcetransformer --python_out=pynumaflow/proto/sourcetransformer --grpc_python_out=pynumaflow/proto/sourcetransformer  pynumaflow/proto/sourcetransformer/*.proto
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sideinput -I=pynumaflow/proto/sideinput --python_out=pynumaflow/proto/sideinput --grpc_python_out=pynumaflow/proto/sideinput  pynumaflow/proto/sideinput/*.proto
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/sourcer -I=pynumaflow/proto/sourcer --python_out=pynumaflow/proto/sourcer --grpc_python_out=pynumaflow/proto/sourcer  pynumaflow/proto/sourcer/*.proto
	poetry run python3 -m grpc_tools.protoc --pyi_out=pynumaflow/proto/accumulator -I=pynumaflow/proto/accumulator --python_out=pynumaflow/proto/accumulator --grpc_python_out=pynumaflow/proto/accumulator  pynumaflow/proto/accumulator/*.proto


	sed -i.bak -e 's/^\(import.*_pb2\)/from . \1/' pynumaflow/proto/*/*.py
	rm pynumaflow/proto/*/*.py.bak
