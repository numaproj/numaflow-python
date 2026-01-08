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
	poetry run pytest tests/ -rA


requirements:
	poetry export -f requirements.txt --output requirements.txt --without-hashes


setup:
	poetry install --with dev --no-root

proto:
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/common/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sinker=pynumaflow/proto/sinker -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sinker/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/mapper=pynumaflow/proto/mapper -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/mapper/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/reducer=pynumaflow/proto/reducer -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/reducer/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sourcetransformer=pynumaflow/proto/sourcetransformer -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sourcetransformer/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sideinput=pynumaflow/proto/sideinput -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sideinput/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/sourcer=pynumaflow/proto/sourcer -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/sourcer/*.proto
	poetry run python3 -m grpc_tools.protoc -Ipynumaflow/proto/accumulator=pynumaflow/proto/accumulator -Ipynumaflow/proto/common=pynumaflow/proto/common --pyi_out=. --python_out=. --grpc_python_out=.  pynumaflow/proto/accumulator/*.proto


# ============================================================================
# Documentation targets
# ============================================================================

.PHONY: docs docs-serve docs-build docs-deploy-dev docs-deploy-version docs-list docs-set-default docs-delete

docs: docs-serve  ## Alias for docs-serve

docs-serve:  ## Serve documentation locally with hot-reload (http://localhost:8000)
	poetry run mkdocs serve

docs-build:  ## Build documentation locally
	poetry run mkdocs build

docs-deploy-dev:  ## Deploy dev docs to docs-site branch
	#poetry run mike deploy -b docs-site dev --push
	poetry run mike deploy -b docs-site dev

docs-deploy-version:  ## Deploy versioned docs (usage: make docs-deploy-version VERSION=0.11)
ifndef VERSION
	$(error VERSION is required. Usage: make docs-deploy-version VERSION=0.11)
endif
	poetry run mike deploy -b docs-site $(VERSION) latest --update-aliases --push

docs-list:  ## List all deployed documentation versions
	poetry run mike list -b docs-site

docs-set-default:  ## Set the default documentation version to 'latest'
	#poetry run mike set-default -b docs-site latest --push
	poetry run mike set-default -b docs-site latest

docs-delete:  ## Delete a documentation version (usage: make docs-delete VERSION=0.10)
ifndef VERSION
	$(error VERSION is required. Usage: make docs-delete VERSION=0.10)
endif
	poetry run mike delete -b docs-site $(VERSION) --push

docs-setup:  ## Install documentation dependencies
	poetry install --with docs --no-root