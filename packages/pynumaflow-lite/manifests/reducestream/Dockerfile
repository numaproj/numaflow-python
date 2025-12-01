FROM python:3.11-slim-bullseye AS builder

ENV PYTHONFAULTHANDLER=1 \
  PYTHONUNBUFFERED=1 \
  PYTHONHASHSEED=random \
  PIP_NO_CACHE_DIR=on \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  PIP_DEFAULT_TIMEOUT=100 \
  POETRY_HOME="/opt/poetry" \
  POETRY_VIRTUALENVS_IN_PROJECT=true \
  POETRY_NO_INTERACTION=1 \
  PYSETUP_PATH="/opt/pysetup"

ENV PATH="$POETRY_HOME/bin:$PATH"

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        curl \
        wget \
        # deps for building python deps
        build-essential \
    && apt-get install -y git \
    && apt-get clean && rm -rf /var/lib/apt/lists/* \
    && curl -sSL https://install.python-poetry.org | python3 -

FROM builder AS udf

WORKDIR $PYSETUP_PATH
COPY ./ ./

# NOTE: place the built wheel in this directory before building the image
RUN pip install $PYSETUP_PATH/pynumaflow_lite-0.1.0-cp311-cp311-manylinux_2_17_aarch64.manylinux2014_aarch64.whl

RUN poetry lock
RUN poetry install --no-cache --no-root && \
    rm -rf ~/.cache/pypoetry/

CMD ["python", "reducestream_counter.py"]

