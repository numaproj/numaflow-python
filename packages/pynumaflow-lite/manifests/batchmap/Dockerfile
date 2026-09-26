FROM python:3.11-slim-trixie AS builder

COPY --from=ghcr.io/astral-sh/uv:0.12.13 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY . .

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Install the local pynumaflow-lite wheel that matches the build platform.
RUN uv pip install --no-index --find-links . pynumaflow-lite

FROM python:3.11-slim-trixie

# Setup a non-root user
RUN groupadd --system --gid 999 nonroot \
 && useradd --system --gid 999 --uid 999 --create-home nonroot

COPY --from=builder --chown=nonroot:nonroot /app /app

ENV PATH="/app/.venv/bin:$PATH"

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# Use the non-root user to run our application
USER nonroot

WORKDIR /app

CMD ["python", "batchmap_cat.py"]
