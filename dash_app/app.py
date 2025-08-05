"""
基本的な構成、layout.py、callbacks.pyとの連携、SQLiteデータベースからの読み込み、サーバー実行
"""

import dash
import dash_bootstrap_components as dbc
import sqlite3
import pandas as pd
from pathlib import Path

# レイアウトとコールバック関数をインポート
from dash_app.layout import create_layout
from dash_app.callbacks import register_callbacks

# データベースのパス設定（プロジェクトルートからの相対パス）
DB_PATH = Path(__file__).resolve().parents[1] / "data/db/wait_times.db"
TABLE_NAME = "wait_times"

def load_data() -> pd.DataFrame:
    """SQLiteからデータを読み込む"""
    with sqlite3.connect(DB_PATH) as conn:
        df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
    return df

# Dashアプリケーションの初期化
app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="万博待ち時間ダッシュボード"
)

# データ読み込みとレイアウト適用
df = load_data()
app.layout = create_layout(df)

# コールバック登録
register_callbacks(app, df)

# サーバー起動
if __name__ == "__main__":
    app.run_server(debug=True, host="0.0.0.0", port=8050)
