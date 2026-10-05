# Ingest gateway image. Build context is the repository root (see infra/compose).
# Base images are pinned by digest so a rebuild produces the same runtime.

FROM python:3.12-slim@sha256:02108f5d322dd89f1c9e552442c25acb0543dfdbc455693a5599624f20d9155d AS build
COPY --from=ghcr.io/astral-sh/uv:0.12.23 /uv /bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY packages ./packages
COPY apps/gateway ./apps/gateway
# Non-editable install: the runtime stage needs the virtualenv only, not the source.
RUN uv sync --frozen --no-dev --no-editable --package watchtower-gateway

FROM python:3.12-slim@sha256:02108f5d322dd89f1c9e552442c25acb0543dfdbc455693a5599624f20d9155d AS runtime
RUN groupadd --gid 10001 gateway \
    && useradd --create-home --uid 10001 --gid 10001 --shell /usr/sbin/nologin gateway
WORKDIR /app
# Same path as the build stage, because the venv's scripts carry absolute shebangs.
COPY --from=build --chown=10001:10001 /app/.venv /app/.venv
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=12 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=2)"]
CMD ["wt-gateway"]
