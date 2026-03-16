####################################################################################################
# Stage 1: Builder - installs all dependencies using uv
####################################################################################################
FROM ghcr.io/astral-sh/uv:python3.13-trixie AS builder

ENV PYSETUP_PATH="/opt/pysetup"
WORKDIR $PYSETUP_PATH

COPY pyproject.toml uv.lock README.md ./
COPY pynumaflow/ ./pynumaflow/

ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/sink/all_sinks"
COPY examples/sink/all_sinks/ $EXAMPLE_PATH/

WORKDIR $EXAMPLE_PATH
RUN uv sync --no-dev --no-install-project --frozen

####################################################################################################
# Stage 2: Runtime - clean image with only installed packages
####################################################################################################
FROM ghcr.io/astral-sh/uv:python3.13-trixie AS udf

ENV PYSETUP_PATH="/opt/pysetup"
ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/sink/all_sinks"

WORKDIR $EXAMPLE_PATH
COPY --from=builder $EXAMPLE_PATH/.venv $EXAMPLE_PATH/.venv
COPY --from=builder $EXAMPLE_PATH/ $EXAMPLE_PATH/

ENTRYPOINT []
CMD ["uv", "run", "python", "example.py"]

EXPOSE 5000
