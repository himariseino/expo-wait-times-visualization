# ---- 1. Installing uv -------------------------------------
FROM python:3.12-slim-trixie
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# ---- 2. Installing a project ------------------------------
# Copy the project into the image
ADD . /app

# Sync the project into a new environment, asserting the lockfile is up to date
WORKDIR /app
ADD pyproject.toml uv.lock ./
RUN uv sync --locked

# ---- 3. Runtime env ---------------------------------------
ENV PATH="/app/.venv/bin:$PATH" \
    TZ=Asia/Tokyo \
    DB_PATH=/app/data/db/wait_times.db \
    MAP_JSON=/app/data/out/map_latest.json \
    PORT=8050 \
    PYTHONUNBUFFERED=1

EXPOSE 8050

# ✅ ここが本質修正（モジュール実行にする）
CMD ["uv", "run", "python", "-m", "dash_app.app"]