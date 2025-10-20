.PHONY: help build develop stubgen clean test test-rust

# Default Python package/module name
MODULE ?= pynumaflow_lite

# Optional args to pass through to cargo test, e.g., ARGS="--lib pyiterables::tests::py_async_iter_stream_yields_incrementally"
ARGS ?=


help:
	@echo "Targets:"
	@echo "  build        - cargo build the Rust library"
	@echo "  develop      - maturin develop (install in current Python env)"
	@echo "  test         - run end-to-end pytest (depends on develop)"
		@echo "  test-rust    - cargo test with PYTHONHOME set; pass args via ARGS=\"...\""

	@echo "  clean        - cargo clean"

build:
	cargo build

# Installs the extension into the active Python environment.
# You can then discover the installed .so path to run stubgen against it if preferred.
develop:
	maturin develop

# Run pytest end-to-end tests. Assumes a working Python env with pytest installed.
# Example: (cd pynumaflow-lite && make test)
# Note: we do not install pytest here to avoid mutating global envs.
test: develop
	pytest -v


# Run cargo tests with PYTHONHOME pointed at base_prefix so embedded CPython finds stdlib
# Usage examples:
#   make test-rust ARGS="--lib"
#   make test-rust ARGS="--lib pyiterables::tests::py_async_iter_stream_yields_incrementally"
test-rust:
	@export PYTHONHOME="$(shell python -c 'import sys; print(sys.base_prefix)')" && \
		cargo test $(ARGS)


clean:
	cargo clean
