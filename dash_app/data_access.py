# dash_app/data_access.py
from __future__ import annotations
import sqlite3, json
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict

# ----- 基本設定 -----
JST = timezone(timedelta(hours=9))

def open_conn(db_path: str) -> sqlite3.Connection:
    """
    Dash のコールバックは別スレッドで動くため、check_same_thread=False を必ず付与。
    row_factory=Row で dict 風アクセスを可能に。
    """
    con = sqlite3.connect(db_path, check_same_thread=False)
    con.row_factory = sqlite3.Row
    return con

def get_latest_feature_collection(json_path: str) -> dict:
    with open(json_path, encoding="utf-8") as f:
        return json.load(f)

def _cutoff_day(days: int) -> str:
    d = datetime.now(JST) - timedelta(days=days)
    return d.strftime("%Y-%m-%d")

# ===== ドロップダウン =====
def get_pavilion_options(con: sqlite3.Connection) -> List[str]:
    sql = """
      SELECT DISTINCT COALESCE(d.name_canonical, w.pavilion_name_raw) AS pavilion_name
      FROM waits_fact w
      LEFT JOIN pavilion_dim d ON d.pavilion_id = w.pavilion_id
      WHERE w.wait_min IS NOT NULL
      ORDER BY pavilion_name
    """
    return [r["pavilion_name"] for r in con.execute(sql).fetchall()]

# ===== KPI =====
def get_overall_avg(con: sqlite3.Connection, days: Optional[int]) -> Optional[float]:
    where, params = "WHERE wait_min IS NOT NULL", []
    if days is not None:
        where += " AND day >= ?"
        params.append(_cutoff_day(days))
    row = con.execute(f"SELECT AVG(wait_min) AS v FROM waits_fact {where}", params).fetchone()
    return float(row["v"]) if row and row["v"] is not None else None

def _dispersion(con: sqlite3.Connection, days: Optional[int], group_col: str) -> Optional[float]:
    where, params = "WHERE wait_min IS NOT NULL", []
    if days is not None:
        where += " AND day >= ?"
        params.append(_cutoff_day(days))
    sql = f"""
      WITH agg AS (
        SELECT {group_col} AS g, AVG(wait_min) AS v
        FROM waits_fact {where}
        GROUP BY {group_col}
      )
      SELECT MAX(v) - MIN(v) AS disp FROM agg
    """
    row = con.execute(sql, params).fetchone()
    return float(row["disp"]) if row and row["disp"] is not None else None

def get_weekday_dispersion(con: sqlite3.Connection, days: Optional[int]) -> Optional[float]:
    return _dispersion(con, days, "weekday")

def get_hour_dispersion(con: sqlite3.Connection, days: Optional[int]) -> Optional[float]:
    return _dispersion(con, days, "hour")

# ===== ランキング =====
def get_ranking(con: sqlite3.Connection, days: Optional[int], top_n: int = 20) -> List[Dict]:
    params, where = [], "WHERE w.wait_min IS NOT NULL"
    if days is not None:
        where += " AND w.day >= ?"
        params.append(_cutoff_day(days))
    sql = f"""
      SELECT
        COALESCE(d.name_canonical, w.pavilion_name_raw) AS pavilion_name,
        AVG(w.wait_min) AS avg_wait,
        COUNT(*) AS n
      FROM waits_fact w
      LEFT JOIN pavilion_dim d ON d.pavilion_id = w.pavilion_id
      {where}
      GROUP BY pavilion_name
      ORDER BY avg_wait DESC
      LIMIT ?
    """
    params.append(top_n)
    return [dict(r) for r in con.execute(sql, params).fetchall()]

# ===== 最新テーブル =====
def get_latest_rows(con: sqlite3.Connection, days: Optional[int], limit: int = 20) -> List[Dict]:
    params, where = [], "WHERE w.wait_min IS NOT NULL"
    if days is not None:
        where += " AND w.day >= ?"
        params.append(_cutoff_day(days))
    sql = f"""
      SELECT
        COALESCE(d.name_canonical, w.pavilion_name_raw) AS pavilion_name,
        w.wait_min AS wait_time_minutes,
        w.timestamp
      FROM waits_fact w
      LEFT JOIN pavilion_dim d ON d.pavilion_id = w.pavilion_id
      {where}
      ORDER BY w.timestamp DESC
      LIMIT ?
    """
    params.append(limit)
    return [dict(r) for r in con.execute(sql, params).fetchall()]

# ===== 時系列（1時間平均） =====
def get_time_series(con: sqlite3.Connection, names: List[str] | None, days: Optional[int]) -> List[Dict]:
    params, where = [], "WHERE w.wait_min IS NOT NULL"
    if days is not None:
        where += " AND w.day >= ?"
        params.append(_cutoff_day(days))
    if names:
        ph = ",".join("?" * len(names))
        where += f" AND COALESCE(d.name_canonical, w.pavilion_name_raw) IN ({ph})"
        params.extend(names)
    sql = f"""
      SELECT
        substr(COALESCE(w.data_time_jst, w.timestamp), 1, 13) || ':00' AS ts_hour,
        COALESCE(d.name_canonical, w.pavilion_name_raw) AS pavilion_name,
        AVG(w.wait_min) AS avg_wait
      FROM waits_fact w
      LEFT JOIN pavilion_dim d ON d.pavilion_id = w.pavilion_id
      {where}
      GROUP BY ts_hour, pavilion_name
      ORDER BY ts_hour, pavilion_name
    """
    return [dict(r) for r in con.execute(sql, params).fetchall()]

# ===== 曜日×時間帯ヒートマップ =====
def get_weekday_hour(con: sqlite3.Connection, days: Optional[int]) -> List[Dict]:
    params, where = [], "WHERE wait_min IS NOT NULL"
    if days is not None:
        where += " AND day >= ?"
        params.append(_cutoff_day(days))
    sql = f"""
      SELECT weekday, hour, AVG(wait_min) AS avg_wait
      FROM waits_fact
      {where}
      GROUP BY weekday, hour
    """
    return [dict(r) for r in con.execute(sql, params).fetchall()]

# ===== 曜日別棒 =====
def get_weekday_bar(con: sqlite3.Connection, days: Optional[int]) -> List[Dict]:
    params, where = [], "WHERE wait_min IS NOT NULL"
    if days is not None:
        where += " AND day >= ?"
        params.append(_cutoff_day(days))
    sql = f"""
      SELECT weekday, AVG(wait_min) AS avg_wait
      FROM waits_fact
      {where}
      GROUP BY weekday
    """
    return [dict(r) for r in con.execute(sql, params).fetchall()]

# ===== 最終更新 =====
def get_last_updated(con: sqlite3.Connection) -> Optional[str]:
    row = con.execute("SELECT MAX(timestamp) AS ts FROM waits_fact").fetchone()
    return row["ts"] if row and row["ts"] else None

# ===== 地図レイヤー用 集計 =====
def get_geo_rows(
    con: sqlite3.Connection,
    mode: str,
    day: str | None = None,
    hour: str | None = None,
    weekday: str | None = None,
) -> List[Dict]:
    """
    レイヤー切替用の集計データを返す。
    戻り値: [{pavilion_id, pavilion_name, lon, lat, avg_wait, last_ts}]
    mode:
      - "date"       : 指定日 day=YYYY-MM-DD
      - "date_hour"  : 指定日+時間帯 day=YYYY-MM-DD, hour="HH:00"
      - "weekday"    : 曜日別 weekday="月曜日"など
      - "hour"       : 時間帯別 hour="HH:00"
      - "latest"     : 使わない（MAP_JSONで別処理）
    """
    where = ["w.wait_min IS NOT NULL"]
    params: List[str] = []

    if mode == "date":
        if not day:  # パラメータ不足なら該当なし
            return []
        where.append("w.day = ?"); params.append(day)
    elif mode == "date_hour":
        if not (day and hour):
            return []
        where.append("w.day = ?");  params.append(day)
        where.append("w.hour = ?"); params.append(hour)
    elif mode == "weekday":
        if not weekday:
            return []
        where.append("w.weekday = ?"); params.append(weekday)
    elif mode == "hour":
        if not hour:
            return []
        where.append("w.hour = ?"); params.append(hour)
    else:
        # 未知のモードは空返し
        return []

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
      SELECT
        w.pavilion_id,
        COALESCE(d.name_canonical, w.pavilion_name_raw) AS pavilion_name,
        g.geojson AS gj,
        AVG(w.wait_min) AS avg_wait,
        MAX(w.timestamp) AS last_ts
      FROM waits_fact w
      LEFT JOIN pavilion_dim d      ON d.pavilion_id = w.pavilion_id
      LEFT JOIN pavilion_geometry g ON g.pavilion_id = w.pavilion_id
      {where_sql}
      GROUP BY w.pavilion_id
      HAVING gj IS NOT NULL
    """

    out: List[Dict] = []
    for r in con.execute(sql, params).fetchall():
        try:
            gj = json.loads(r["gj"])
            lon, lat = gj["geometry"]["coordinates"]  # [lon, lat]
            out.append(dict(
                pavilion_id   = r["pavilion_id"],
                pavilion_name = r["pavilion_name"],
                lon           = float(lon),
                lat           = float(lat),
                avg_wait      = float(r["avg_wait"]) if r["avg_wait"] is not None else None,
                last_ts       = r["last_ts"],
            ))
        except Exception:
            # 位置が壊れている等はスキップ
            continue
    return out
