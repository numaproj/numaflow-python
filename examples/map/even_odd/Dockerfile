####################################################################################################
# Stage 1: Builder - installs dependencies using poetry
####################################################################################################
FROM python:3.10-slim-bullseye AS builder

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=on \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.2.2 \
    POETRY_HOME="/opt/poetry" \
    POETRY_VIRTUALENVS_IN_PROJECT=true \
    POETRY_NO_INTERACTION=1 \
    PYSETUP_PATH="/opt/pysetup"

ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/map/even_odd"
ENV VENV_PATH="$EXAMPLE_PATH/.venv"
ENV PATH="$POETRY_HOME/bin:$VENV_PATH/bin:$PATH"

# Install build dependencies and poetry
RUN apt-get update && apt-get install --no-install-recommends -y \
        curl \
        wget \
        build-essential \
        git \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && wget -O /dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64 \
    && chmod +x /dumb-init \
    && curl -sSL https://install.python-poetry.org | python3 -

# Copy project into builder
WORKDIR $PYSETUP_PATH
COPY ./ ./ 

# Install deps
WORKDIR $EXAMPLE_PATH
RUN poetry lock && \
    poetry install --no-cache --no-root && \
    rm -rf ~/.cache/pypoetry/

####################################################################################################
# Stage 2: UDF Runtime - clean container with only needed stuff
####################################################################################################
FROM python:3.10-slim-bullseye AS udf

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYSETUP_PATH="/opt/pysetup"

ENV EXAMPLE_PATH="$PYSETUP_PATH/examples/map/even_odd"
ENV VENV_PATH="$EXAMPLE_PATH/.venv"
ENV PATH="$VENV_PATH/bin:$PATH"

# Install only runtime system dependencies
RUN apt-get update && apt-get install --no-install-recommends -y \
        wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && wget -O /dumb-init https://github.com/Yelp/dumb-init/releases/download/v1.2.5/dumb-init_1.2.5_x86_64 \
    && chmod +x /dumb-init

# Copy virtual environment and entry script
WORKDIR $PYSETUP_PATH
COPY --from=builder $VENV_PATH $VENV_PATH
COPY --from=builder $EXAMPLE_PATH $EXAMPLE_PATH

WORKDIR $EXAMPLE_PATH
RUN chmod +x entry.sh

ENTRYPOINT ["/dumb-init", "--"]
CMD ["sh", "-c", "$EXAMPLE_PATH/entry.sh"]

EXPOSE 5000
