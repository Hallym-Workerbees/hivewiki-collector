# syntax=docker/dockerfile:1.7

FROM ghcr.io/astral-sh/uv:python3.12-trixie-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
  PYTHONUNBUFFERED=1 \
  UV_COMPILE_BYTECODE=1 \
  UV_LINK_MODE=copy \
  PATH="/app/.venv/bin:$PATH" \
  PYTHONPATH="/app"

WORKDIR /app

RUN groupadd --system --gid 10001 app \
  && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-dev --no-install-project

COPY app ./app

RUN chown -R app:app /app

USER 10001:10001

CMD ["python", "-m", "app.main"]
