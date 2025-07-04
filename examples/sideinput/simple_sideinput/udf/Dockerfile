####################################################################################################
# Stage 1: Base Builder - installs core dependencies using poetry
####################################################################################################
FROM python:3.10-slim-bullseye AS base-builder

ENV PYSETUP_PATH="/opt/pysetup"
WORKDIR $PYSETUP_PATH

# Copy only core dependency files first for better caching
COPY pyproject.toml poetry.lock README.md ./
COPY pynumaflow/ ./pynumaflow/
RUN apt-get update && apt-get install --no-install-recommends -y \
        curl wget build-essential git \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && pip install poetry \
    && poetry install --no-root --no-interaction

####################################################################################################
# Stage 2: UDF Builder - adds UDF code and installs UDF-specific deps
####################################################################################################
FROM base-builder AS udf-builder

ENV EXAMPLE_PATH="/opt/pysetup/examples/sideinput/simple_sideinput/udf"
ENV POETRY_VIRTUALENVS_IN_PROJECT=true

WORKDIR $EXAMPLE_PATH
COPY examples/sideinput/simple_sideinput/udf/ ./
RUN poetry install --no-root --no-interaction

####################################################################################################
# Stage 3: UDF Runtime - clean container with only needed stuff
####################################################################################################
FROM python:3.10-slim-bullseye AS udf

ENV PYSETUP_PATH="/opt/pysetup"
ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/sideinput/simple_sideinput/udf"
ENV VENV_PATH="$EXAMPLE_PATH/.venv"
ENV PATH="$VENV_PATH/bin:$PATH"

RUN apt-get update && apt-get install --no-install-recommends -y wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && wget -O /dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64 \
    && chmod +x /dumb-init

WORKDIR $PYSETUP_PATH
COPY --from=udf-builder $VENV_PATH $VENV_PATH
COPY --from=udf-builder $EXAMPLE_PATH $EXAMPLE_PATH

WORKDIR $EXAMPLE_PATH
RUN chmod +x entry.sh

ENTRYPOINT ["/dumb-init", "--"]
CMD ["sh", "-c", "$EXAMPLE_PATH/entry.sh"]

EXPOSE 5000
