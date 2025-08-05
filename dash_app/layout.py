from dash import html, dcc, dash_table
import pandas as pd


def create_layout(df: pd.DataFrame):
    # パビリオン名一覧（ユニーク・ソート）
    pavilion_options = sorted(df['pavilion_name'].dropna().unique())
    pavilion_dropdown_options = [{"label": name, "value": name} for name in pavilion_options]

    return html.Div([
        html.H1("パビリオン待ち時間 詳細分析", style={"textAlign": "center"}),

        # --- 操作パネル ---
        html.Div([
            html.Label("パビリオンを選択", style={"fontWeight": "bold"}),
            dcc.Dropdown(
                id="pavilion-selector",
                options=pavilion_dropdown_options,
                multi=True,
                placeholder="パビリオンを選択（複数可）"
            ),
            html.Br(),

            html.Label("集計期間を選択", style={"fontWeight": "bold"}),
            dcc.RadioItems(
                id="aggregation-range",
                options=[
                    {"label": "全期間", "value": "all"},
                    {"label": "直近1ヶ月", "value": "1month"},
                    {"label": "直近7日間", "value": "7days"},
                    {"label": "曜日別平均", "value": "weekday"},
                ],
                value="all",
                labelStyle={"display": "inline-block", "margin-right": "15px"}
            ),
        ], style={"marginBottom": "30px"}),

        # --- 全体傾向グラフ ---
        html.Div([
            html.H2("平均待ち時間ランキング"),
            dcc.Graph(id="avg-wait-bar"),

            html.H2("最新の待ち時間（投稿順）"),
            dash_table.DataTable(
                id="latest-wait-table",
                columns=[
                    {"name": "パビリオン", "id": "pavilion_name"},
                    {"name": "待ち時間（分）", "id": "wait_time_minutes"},
                    {"name": "投稿時刻", "id": "timestamp"},
                ],
                style_table={"overflowX": "auto"},
                page_size=10,
            ),
        ], style={"marginBottom": "50px"}),

        # --- 詳細分析グラフ ---
        html.Div([
            html.H2("選択パビリオンの時間推移"),
            dcc.Graph(id="wait-time-line"),

            html.H2("曜日 × 時間帯ヒートマップ"),
            dcc.Graph(id="wait-heatmap"),
        ]),

        # --- 最終更新情報 ---
        html.Div(id="last-updated-text", style={"textAlign": "right", "marginTop": "20px"}),
    ], style={"padding": "40px"})