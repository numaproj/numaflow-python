clean:
	@rm -rf build dist .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format: clean
	poetry run black .


lint: format
	poetry run flake8 .


test:
	poetry run pytest pynumaflow/tests/


requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes

# TODO: proto file generate
proto:
	 python3 -m grpc_tools.protoc -I=pynumaflow/function/proto --python_out=pynumaflow/function/proto --grpc_python_out=pynumaflow/function/proto  pynumaflow/function/proto/udfunction.proto
	 python3 -m grpc_tools.protoc -I=pynumaflow/sink/proto --python_out=pynumaflow/sink/proto --grpc_python_out=pynumaflow/sink/proto  pynumaflow/sink/proto/udsink.proto
	 sed -i '' 's/^\(import.*_pb2\)/from . \1/' pynumaflow/*/proto/*.py
