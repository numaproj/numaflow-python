clean:
	@rm -rf build .eggs *.egg-info
	@rm -rf .benchmarks .coverage coverage.xml htmlcov report.xml .tox
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +
	@find . -type d -name '*pytest_cache*' -exec rm -rf {} +
	@find . -type f -name "*.py[co]" -exec rm -rf {} +

format: clean
	uv run black --exclude=pynumaflow/proto/ pynumaflow/ tests/ examples/


lint: format
	uv run ruff check --fix .


test:
	uv run pytest tests/ -rA


setup:
	uv sync --all-groups

proto:
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/common/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sinker=pynumaflow/proto/sinker -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sinker/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/mapper=pynumaflow/proto/mapper -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/mapper/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/reducer=pynumaflow/proto/reducer -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/reducer/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sourcetransformer=pynumaflow/proto/sourcetransformer -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sourcetransformer/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sideinput=pynumaflow/proto/sideinput -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sideinput/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sourcer=pynumaflow/proto/sourcer -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sourcer/*.proto
	uv run python3 -m grpc_tools.protoc -Ipynumaflow/proto/accumulator=pynumaflow/proto/accumulator -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/accumulator/*.proto


# ============================================================================
# Documentation targets
# ============================================================================

.PHONY: docs docs-serve docs-build docs-deploy-dev docs-deploy-version docs-list docs-set-default docs-delete

docs: docs-serve  ## Alias for docs-serve

docs-serve:  ## Serve documentation locally with hot-reload (http://localhost:8000)
	uv run mkdocs serve

docs-build:  ## Build documentation locally
	uv run mkdocs build

docs-deploy-dev:  ## Deploy dev docs to docs-site branch
	uv run mike deploy -b docs-site dev --push

docs-deploy-version:  ## Deploy versioned docs (usage: make docs-deploy-version VERSION=0.11)
ifndef VERSION
	$(error VERSION is required. Usage: make docs-deploy-version VERSION=0.11)
endif
	uv run mike deploy -b docs-site $(VERSION) latest --update-aliases --push

docs-list:  ## List all deployed documentation versions
	uv run mike list -b docs-site

docs-set-default:  ## Set the default documentation version to 'latest'
	uv run mike set-default -b docs-site latest --push

docs-delete:  ## Delete a documentation version (usage: make docs-delete VERSION=0.10)
ifndef VERSION
	$(error VERSION is required. Usage: make docs-delete VERSION=0.10)
endif
	uv run mike delete -b docs-site $(VERSION) --push
