FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim AS build
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project
COPY src ./src
RUN uv sync --locked --no-dev

FROM build AS test
COPY tests ./tests
RUN uv sync --locked --all-groups
ENTRYPOINT ["uv", "run"]

FROM python:3.14-slim-bookworm
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH" PYTHONUNBUFFERED=1
COPY --from=build /app /app
USER 65532:65532
ENTRYPOINT ["python", "-m"]
