####################################################################################################
# Stage 1: Builder - installs all dependencies using uv
####################################################################################################
FROM ghcr.io/astral-sh/uv:python3.13-trixie AS builder

ENV PYSETUP_PATH="/opt/pysetup"
WORKDIR $PYSETUP_PATH

COPY pyproject.toml uv.lock README.md ./
COPY pynumaflow/ ./pynumaflow/

ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/reduce/counter"
COPY examples/reduce/counter/ $EXAMPLE_PATH/

WORKDIR $EXAMPLE_PATH
RUN uv sync --no-dev --no-install-project --frozen

####################################################################################################
# Stage 2: Runtime - clean image with only installed packages
####################################################################################################
FROM ghcr.io/astral-sh/uv:python3.13-trixie AS udf

ENV PYSETUP_PATH="/opt/pysetup"
ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/reduce/counter"

WORKDIR $EXAMPLE_PATH
COPY --from=builder $EXAMPLE_PATH/.venv $EXAMPLE_PATH/.venv
COPY --from=builder $EXAMPLE_PATH/ $EXAMPLE_PATH/

# NOTE: We cannot use "uv run python example.py" here because uv run reads the
# example's pyproject.toml, finds the pynumaflow path source (path = "../../../"),
# and tries to resolve it. In the runtime stage, the parent pynumaflow source tree
# is not present (by design, to keep the image small), so uv run fails.
# Instead, we activate the pre-built .venv via PATH and run python directly.
ENV PATH="$EXAMPLE_PATH/.venv/bin:$PATH"
CMD ["python", "example.py"]

EXPOSE 5000
