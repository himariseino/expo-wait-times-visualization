docker build -t dash-app .
docker run --rm -p 8050:8050 dash-app

docker run --rm -it dash-app bash
# 例：手で起動
uv run python -m dash_app.app


# 依存関係や Python の場所は .venv が使われます
docker run --rm -it dash-app python -V
docker run --rm -it dash-app uv run python -c "import sys; print(sys.executable)"
docker run --rm -it dash-app ls -la dash_app


docker run --rm -it --entrypoint /bin/sh dash-app
docker run --rm -p 8050:8050 dash-app:latest