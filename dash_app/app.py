# dash_app/app.py
import os
import dash
import dash_bootstrap_components as dbc
from flask_caching import Cache
from pathlib import Path

from dash_app.layout import create_layout
from dash_app.callbacks import register_callbacks

# 環境変数（docker run -e で上書き可）
DB_PATH = os.getenv("DB_PATH", str(Path(__file__).resolve().parents[1] / "data/db/wait_times.db"))
MAP_JSON = os.getenv("MAP_JSON", str(Path(__file__).resolve().parents[1] / "data/out/map_latest.json"))
PORT = int(os.getenv("PORT", "8050"))

# Dash アプリ初期化
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="万博待ち時間ダッシュボード",
)
server = app.server

# シンプルキャッシュ（60秒）
cache = Cache(app.server, config={"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 60})

# レイアウト（DFは渡さない前提）
app.layout = create_layout()

# コールバック登録（新シグネチャ）
register_callbacks(app, DB_PATH, MAP_JSON, cache)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=PORT, use_reloader=True)
