# Dockerfile
# ベースに uv (+ Python3.11, bookworm) を使用
FROM ghcr.io/astral-sh/uv:python3.11-bookworm AS base
WORKDIR /app

# タイムゾーン等（必要に応じて）
ENV TZ=Asia/Tokyo \
    DB_PATH=/app/data/db/wait_times.db \
    MAP_JSON=/app/data/out/map_latest.json \
    PORT=8050

# 依存のみ先にコピーしてロック・インストール（レイヤーキャッシュ最適化）
COPY pyproject.toml uv.lock* ./
RUN uv sync --frozen --no-dev

# アプリ本体
COPY . .

# uv が作った .venv を PATH に追加
ENV PATH="/app/.venv/bin:${PATH}"

EXPOSE 8050
WORKDIR /app
CMD ["python", "-m", "dash_app.app"]
