# dash_app/callbacks.py

from dash import Input, Output
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta


def register_callbacks(app, df_original: pd.DataFrame):
    # 日付列をdatetime型に変換
    df_original["timestamp"] = pd.to_datetime(df_original["timestamp"])

    @app.callback(
        Output("avg-wait-bar", "figure"),
        Output("latest-wait-table", "data"),
        Output("wait-time-line", "figure"),
        Output("wait-heatmap", "figure"),
        Output("last-updated-text", "children"),
        Input("pavilion-selector", "value"),
        Input("aggregation-range", "value"),
    )
    def update_all_graphs(selected_pavilions, aggregation_range):
        now = df_original["timestamp"].max()
        df_filtered = df_original.copy()

        # 集計期間でフィルタ
        if aggregation_range == "1month":
            df_filtered = df_filtered[df_filtered["timestamp"] >= now - timedelta(days=30)]
        elif aggregation_range == "7days":
            df_filtered = df_filtered[df_filtered["timestamp"] >= now - timedelta(days=7)]

        # パビリオン選択フィルタ
        if selected_pavilions:
            df_filtered = df_filtered[df_filtered["pavilion_name"].isin(selected_pavilions)]

        # --- グラフ①: 平均待ち時間バー ---
        if aggregation_range == "weekday":
            bar_df = (
                df_filtered.groupby(["pavilion_name", "weekday"])
                .agg({"wait_time_minutes": "mean"})
                .reset_index()
            )
            bar_df = bar_df.groupby("pavilion_name")["wait_time_minutes"].mean().reset_index()
        else:
            bar_df = df_filtered.groupby("pavilion_name")["wait_time_minutes"].mean().reset_index()

        bar_df = bar_df.sort_values("wait_time_minutes", ascending=False).head(20)
        bar_fig = px.bar(
            bar_df,
            x="pavilion_name",
            y="wait_time_minutes",
            title="平均待ち時間（上位20件）",
            labels={"wait_time_minutes": "待ち時間（分）", "pavilion_name": "パビリオン"}
        )

        # --- 表②: 最新の待ち時間ランキング ---
        latest_df = df_filtered.sort_values("timestamp", ascending=False).head(20)
        table_data = latest_df[["pavilion_name", "wait_time_minutes", "timestamp"]].to_dict("records")

        # --- グラフ③: 時系列推移 ---
        line_fig = px.line(
            df_filtered.sort_values("timestamp"),
            x="timestamp",
            y="wait_time_minutes",
            color="pavilion_name",
            title="待ち時間の時間推移",
            labels={"wait_time_minutes": "待ち時間（分）", "timestamp": "投稿時刻"}
        )

        # --- グラフ④: ヒートマップ（曜日 × 時間帯）---
        heat_df = (
            df_filtered.groupby(["weekday", "hour_range"])["wait_time_minutes"]
            .mean().reset_index()
        )

        weekday_order = ["月曜日", "火曜日", "水曜日", "木曜日", "金曜日", "土曜日", "日曜日"]
        heat_df["weekday"] = pd.Categorical(heat_df["weekday"], categories=weekday_order, ordered=True)

        heatmap_fig = px.density_heatmap(
            heat_df,
            x="hour_range",
            y="weekday",
            z="wait_time_minutes",
            title="曜日 × 時間帯の待ち時間",
            color_continuous_scale="Blues",
            labels={"wait_time_minutes": "待ち時間（分）", "hour_range": "時間帯", "weekday": "曜日"},
        )

        # --- 更新情報表示 ---
        updated_text = f"最終更新: {now.strftime('%Y-%m-%d %H:%M')}"

        return bar_fig, table_data, line_fig, heatmap_fig, updated_text