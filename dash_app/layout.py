# dash_app/layout.py
from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc
import dash_leaflet as dl

def create_layout():
    return html.Div(
        [
            html.H1("関西大阪万博 パビリオン待ち時間ダッシュボード"),

            # KPI（3枚）
            dbc.Row(
                [
                    dbc.Col(dbc.Card(dbc.CardBody([html.Div("全体平均待ち時間（分）"), html.H2(id="card-avg")])), md=4),
                    dbc.Col(dbc.Card(dbc.CardBody([html.Div("曜日別偏差（分）"),   html.H2(id="card-weekday5p")])), md=4),
                    dbc.Col(dbc.Card(dbc.CardBody([html.Div("時間帯別偏差（分）"), html.H2(id="card-hour5p")])), md=4),
                ],
                className="mb-3",
            ),

            # 操作（集計期間・パビリオン選択）
            dbc.Row(
                [
                    dbc.Col(
                        dcc.Dropdown(
                            id="dd-range",
                            options=[
                                {"label": "直近7日", "value": "7d"},
                                {"label": "直近30日", "value": "30d"},
                                {"label": "全期間", "value": "all"},
                            ],
                            value="7d",
                            clearable=False,
                        ),
                        md=3,
                    ),
                    dbc.Col(
                        dcc.Dropdown(
                            id="dd-pavilion",
                            options=[],   # 初期は空。callbacksで埋める
                            multi=True,
                            placeholder="パビリオンを選択（複数可）",
                        ),
                        md=9,
                    ),
                ],
                className="mb-4",
            ),

            # グラフ群（ランキング / 曜日×時間帯ヒートマップ）
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="fig-ranking"), md=6),
                    dbc.Col(dcc.Graph(id="fig-weekday-heat"), md=6),
                ],
                className="mb-4",
            ),

            # グラフ群（時系列 / 曜日別棒）
            dbc.Row(
                [
                    dbc.Col(dcc.Graph(id="fig-timeseries"), md=6),
                    dbc.Col(dcc.Graph(id="fig-weekday-bar"), md=6),
                ],
                className="mb-4",
            ),

            # 最新一覧テーブル
            html.H3("最新の待ち時間（投稿順）"),
            dash_table.DataTable(
                id="latest-wait-table",
                columns=[
                    {"name": "パビリオン", "id": "pavilion_name"},
                    {"name": "待ち時間（分）", "id": "wait_time_minutes"},
                    {"name": "投稿時刻", "id": "timestamp"},
                ],
                page_size=10,
                style_table={"overflowX": "auto"},
            ),
            html.Hr(),

            # 混雑マップ（最下部）
            html.H3("混雑マップ"),
            dl.Map(
                id="congestion-map",
                center=[34.70, 135.42],  # 仮中心
                zoom=14,
                style={"height": "420px", "width": "100%"},
                children=[
                    dl.TileLayer(url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"),
                    dl.GeoJSON(id="map-geojson"),
                ],
            ),
            dcc.Interval(id="ivl-latest", interval=60_000, n_intervals=0),

            html.Div(id="last-updated-text", style={"textAlign": "right", "marginTop": "10px"}),
        ],
        style={"padding": "20px"},
    )
