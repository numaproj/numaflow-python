format:
	poetry run black pynumaflow/

test:
	poetry run pytest pynumaflow/tests/

generate:
    poetry run python -m grpc_tools.protoc -I./protos/function/v1/ --python_out=pynumaflow/function/ --grpc_python_out=pynumaflow/function/  protos/function/v1/udfunction.proto
