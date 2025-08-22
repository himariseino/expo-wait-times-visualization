from dash import Input, Output
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import json, re

from dash_app.data_access import (
    open_conn, get_pavilion_options, get_overall_avg,
    get_weekday_dispersion, get_hour_dispersion, get_ranking,
    get_latest_rows, get_time_series, get_weekday_hour, get_weekday_bar,
    get_last_updated
)

WEEKDAY_ORDER = ["月曜日","火曜日","水曜日","木曜日","金曜日","土曜日","日曜日"]

def _range_to_days(key: str):
    return {"7d":7, "30d":30, "all":None}.get(key or "7d", 7)

def _sorted_hours(hours):
    import re
    def h2i(h):
        m = re.match(r"(\d{1,2}):00", str(h))
        return int(m.group(1)) if m else 0
    return sorted(set(hours), key=h2i)

def register_callbacks(app, db_path: str, map_json_path: str, cache):

    # ▼▼ ここを削除：con = open_conn(db_path) 使い回しはしない ▼▼
    # con = open_conn(db_path)

    # パビリオン選択肢
    @app.callback(Output("dd-pavilion", "options"), Input("dd-range", "value"))
    def _init_pavilion_options(_):
        with open_conn(db_path) as con:
            return [{"label": n, "value": n} for n in get_pavilion_options(con)]

    # KPI
    @app.callback(
        Output("card-avg","children"),
        Output("card-weekday5p","children"),
        Output("card-hour5p","children"),
        Input("dd-range","value"),
    )
    def _kpis(range_key):
        days = _range_to_days(range_key)
        with open_conn(db_path) as con:
            avg = get_overall_avg(con, days)
            wd  = get_weekday_dispersion(con, days)
            hr  = get_hour_dispersion(con, days)
        fmt = lambda v: f"{round(v):,} 分" if v is not None else "—"
        return fmt(avg), fmt(wd), fmt(hr)

    # ランキング
    @app.callback(Output("fig-ranking","figure"), Input("dd-range","value"))
    def _ranking(range_key):
        with open_conn(db_path) as con:
            rows = get_ranking(con, _range_to_days(range_key), top_n=20)
            if not rows and (range_key != "all"):  # フォールバック（親切）
                rows = get_ranking(con, None, top_n=20)
        if not rows: return px.bar(title="データなし")
        fig = px.bar(rows, x="pavilion_name", y="avg_wait", hover_data=["n"],
                     labels={"avg_wait":"平均待ち分","pavilion_name":"パビリオン"},
                     title="平均待ち時間ランキング")
        fig.update_layout(margin=dict(l=10,r=10,t=40,b=10), xaxis_tickangle=-30)
        return fig

    # 最新テーブル
    @app.callback(Output("latest-wait-table","data"), Input("dd-range","value"))
    def _latest_table(range_key):
        with open_conn(db_path) as con:
            return get_latest_rows(con, _range_to_days(range_key), limit=20)

    # 時系列
    @app.callback(Output("fig-timeseries","figure"),
                  Input("dd-range","value"), Input("dd-pavilion","value"))
    def _timeseries(range_key, names):
        with open_conn(db_path) as con:
            rows = get_time_series(con, names or [], _range_to_days(range_key))
        if not rows:
            return go.Figure(layout_title_text="データなし")
        df = pd.DataFrame(rows)
        df["ts_hour"] = pd.to_datetime(df["ts_hour"])
        fig = px.line(df, x="ts_hour", y="avg_wait", color="pavilion_name",
                      labels={"ts_hour":"時刻","avg_wait":"待ち分","pavilion_name":"パビリオン"},
                      title="待ち時間の時間推移（1時間平均）")
        fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
        return fig

    # 曜日×時間帯ヒートマップ
    @app.callback(Output("fig-weekday-heat","figure"), Input("dd-range","value"))
    def _heat(range_key):
        with open_conn(db_path) as con:
            rows = get_weekday_hour(con, _range_to_days(range_key))
        if not rows:
            return go.Figure(layout_title_text="データなし")
        df = pd.DataFrame(rows)
        df["weekday"] = pd.Categorical(df["weekday"], categories=WEEKDAY_ORDER, ordered=True)
        hours_sorted = _sorted_hours(df["hour"].tolist())
        df["hour"] = pd.Categorical(df["hour"], categories=hours_sorted, ordered=True)
        pvt = df.pivot(index="weekday", columns="hour", values="avg_wait").reindex(WEEKDAY_ORDER)
        fig = px.imshow(pvt, aspect="auto", origin="lower",
                        labels=dict(color="平均待ち分", x="時間帯", y="曜日"),
                        title="曜日 × 時間帯の平均待ち時間")
        fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
        return fig

    # 曜日別棒
    @app.callback(Output("fig-weekday-bar","figure"), Input("dd-range","value"))
    def _weekday_bar(range_key):
        with open_conn(db_path) as con:
            rows = get_weekday_bar(con, _range_to_days(range_key))
        if not rows: return px.bar(title="データなし")
        df = pd.DataFrame(rows)
        df["weekday"] = pd.Categorical(df["weekday"], categories=WEEKDAY_ORDER, ordered=True)
        df = df.sort_values("weekday")
        fig = px.bar(df, x="weekday", y="avg_wait",
                     labels={"weekday":"曜日","avg_wait":"平均待ち分"}, title="曜日別 平均待ち時間")
        fig.update_layout(margin=dict(l=10,r=10,t=40,b=10))
        return fig

    # 最終更新
    @app.callback(Output("last-updated-text","children"), Input("dd-range","value"))
    def _last_updated(_):
        with open_conn(db_path) as con:
            ts = get_last_updated(con)
        if not ts: return "最終更新: —"
        try:
            dt = pd.to_datetime(ts)
            return f"最終更新: {dt.strftime('%Y-%m-%d %H:%M')}"
        except Exception:
            return f"最終更新: {ts}"

    # 地図（60秒キャッシュ）
    @cache.memoize(timeout=60)
    def _read_geojson(path: str):
        with open(path, encoding="utf-8") as f:
            return json.load(f)

    @app.callback(Output("map-geojson","data"), Input("ivl-latest","n_intervals"))
    def _update_map(_n):
        return _read_geojson(map_json_path)
