format:
	poetry run black pynumaflow/

test:
	poetry run pytest pynumaflow/tests/

# TODO: proto file generate