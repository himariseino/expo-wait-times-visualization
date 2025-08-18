#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DBから地図用GeoJSONを出力するエクスポータ（完全版）
- mode=latest: 各パビリオンの最新レコードを1件ずつ返す
- mode=aggregate: 日付/曜日/時間帯の任意組み合わせで集計し、各館1値を返す

【前提スキーマ】
- waits_fact(timestamp, pavilion_id, wait_min, staleness_min, weekday, hour_range, day, hour, data_time_jst, ...)
- pavilion_dim(pavilion_id, name_canonical)
- pavilion_geometry(pavilion_id, geojson)  # Feature または Geometry を文字列保存

使い方例はファイル末尾に記載。
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Iterable, List, Optional, Tuple, Dict, Any


# ---------------------------
# ユーティリティ
# ---------------------------
JST = timezone(timedelta(hours=9))

WEEKDAYS_JA = {"月曜日","火曜日","水曜日","木曜日","金曜日","土曜日","日曜日"}

def now_iso() -> str:
    return datetime.now(JST).strftime("%Y-%m-%dT%H:%M:%S%z")

def coerce_list(v: Optional[str | Iterable[str]]) -> Optional[List[str]]:
    """クエリ引数をリスト化。'a,b' も ['a','b'] にする。"""
    if v is None:
        return None
    if isinstance(v, str):
        parts = [p.strip() for p in v.split(",") if p.strip()]
        return parts or None
    return list(v)

def p_percentile(values: List[float], p: float) -> float:
    """単純なパーセンタイル（0-100）。values は非空を想定。"""
    if not values:
        return float("nan")
    s = sorted(values)
    k = (len(s) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(s) - 1)
    if f == c:
        return float(s[f])
    d0 = s[f] * (c - k)
    d1 = s[c] * (k - f)
    return float(d0 + d1)

def ensure_feature(geojson_str: str) -> Dict[str, Any]:
    """DBに保存された geojson（Feature or Geometry）を Feature に正規化。"""
    gj = json.loads(geojson_str)
    if isinstance(gj, dict) and gj.get("type") == "Feature":
        return gj
    # Geometry の場合
    return {"type": "Feature", "properties": {}, "geometry": gj}


# ---------------------------
# DBアクセス
# ---------------------------
@dataclass
class Filters:
    day: Optional[List[str]] = None            # ['YYYY-MM-DD', ...]
    date_from: Optional[str] = None            # 'YYYY-MM-DD'
    date_to: Optional[str] = None
    weekday: Optional[List[str]] = None        # ['月曜日', ...]
    hour: Optional[List[str]] = None           # ['11:00', ...]
    min_samples: int = 3
    fresh_within: Optional[int] = None         # minutes（latestモード用）

def build_where_clause(f: Filters) -> Tuple[str, List[Any]]:
    """aggregate用のWHERE句生成（必要なフィルタだけANDで追加）。"""
    where = []
    params: List[Any] = []

    if f.day:
        where.append(f"w.day IN ({','.join('?'*len(f.day))})")
        params.extend(f.day)
    elif f.date_from and f.date_to:
        where.append("w.day BETWEEN ? AND ?")
        params.extend([f.date_from, f.date_to])

    if f.weekday:
        # バリデーション（任意：不正値は落とす）
        wds = [w for w in f.weekday if w in WEEKDAYS_JA]
        if wds:
            where.append(f"w.weekday IN ({','.join('?'*len(wds))})")
            params.extend(wds)

    if f.hour:
        where.append(f"w.hour IN ({','.join('?'*len(f.hour))})")
        params.extend(f.hour)

    # 常に wait_min は必要
    where.append("w.wait_min IS NOT NULL")

    clause = "WHERE " + " AND ".join(where) if where else "WHERE w.wait_min IS NOT NULL"
    return clause, params

def fetch_latest(con: sqlite3.Connection, fresh_within: Optional[int]) -> List[sqlite3.Row]:
    clause = "WHERE w.wait_min IS NOT NULL"
    params: List[Any] = []
    if fresh_within is not None:
        clause += " AND (w.staleness_min IS NULL OR w.staleness_min <= ?)"
        params.append(int(fresh_within))

    sql = f"""
    WITH latest AS (
      SELECT pavilion_id, MAX(timestamp) AS ts
      FROM waits_fact
      GROUP BY pavilion_id
    )
    SELECT w.pavilion_id,
           d.name_canonical AS pavilion_name,
           w.wait_min,
           w.staleness_min,
           w.data_time_jst,
           g.geojson
    FROM waits_fact w
    JOIN latest l
      ON w.pavilion_id=l.pavilion_id AND w.timestamp=l.ts
    LEFT JOIN pavilion_dim d ON d.pavilion_id=w.pavilion_id
    LEFT JOIN pavilion_geometry g ON g.pavilion_id=w.pavilion_id
    {clause}
    """
    return con.execute(sql, params).fetchall()

def fetch_for_aggregate(con: sqlite3.Connection, f: Filters) -> List[sqlite3.Row]:
    clause, params = build_where_clause(f)
    sql = f"""
    SELECT w.pavilion_id,
           d.name_canonical AS pavilion_name,
           w.wait_min,
           g.geojson
    FROM waits_fact w
    LEFT JOIN pavilion_dim d ON d.pavilion_id=w.pavilion_id
    LEFT JOIN pavilion_geometry g ON g.pavilion_id=w.pavilion_id
    {clause}
    """
    return con.execute(sql, params).fetchall()


# ---------------------------
# エクスポート本体
# ---------------------------
def export_latest(con: sqlite3.Connection, out_path: str, fresh_within: Optional[int]) -> int:
    rows = fetch_latest(con, fresh_within)
    feats: List[Dict[str, Any]] = []
    for r in rows:
        gj = ensure_feature(r["geojson"])
        gj["properties"] = {
            "pavilion_id": r["pavilion_id"],
            "pavilion_name": r["pavilion_name"],
            "wait_min": r["wait_min"],
            "staleness_min": r["staleness_min"],
            "data_time_jst": r["data_time_jst"],
        }
        feats.append(gj)

    fc = {
        "type": "FeatureCollection",
        "features": feats,
        "meta": {
            "mode": "latest",
            "fresh_within": fresh_within,
            "generated_at": now_iso(),
        }
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
    return len(feats)

def export_aggregate(con: sqlite3.Connection, out_path: str, stat: str, filters: Filters) -> int:
    rows = fetch_for_aggregate(con, filters)

    # pavilion_idごとに値を貯める
    by_pid: Dict[str, Tuple[List[float], Tuple[str, str]]] = {}  # pid -> ([waits], (name, geojson))
    for r in rows:
        if r["wait_min"] is None:
            continue
        lst, meta = by_pid.setdefault(r["pavilion_id"], ([], (r["pavilion_name"], r["geojson"])))
        lst.append(float(r["wait_min"]))
        by_pid[r["pavilion_id"]] = (lst, meta)

    feats: List[Dict[str, Any]] = []
    for pid, (vals, (name, geojson_str)) in by_pid.items():
        if len(vals) < (filters.min_samples or 0):
            continue

        if stat == "avg":
            v = sum(vals) / len(vals)
        elif stat == "max":
            v = max(vals)
        elif stat == "min":
            v = min(vals)
        elif stat == "median":
            vals_sorted = sorted(vals)
            n = len(vals_sorted)
            mid = n // 2
            v = (vals_sorted[mid] if n % 2 == 1 else (vals_sorted[mid-1] + vals_sorted[mid]) / 2.0)
        elif stat == "p90":
            v = p_percentile(vals, 90)
        else:
            v = sum(vals) / len(vals)

        gj = ensure_feature(geojson_str)
        gj["properties"] = {
            "pavilion_id": pid,
            "pavilion_name": name,
            "value": round(v),         # 前段の色分けルールに合わせて四捨五入
            "stat": stat,
            "n": len(vals),
        }
        feats.append(gj)

    fc = {
        "type": "FeatureCollection",
        "features": feats,
        "meta": {
            "mode": "aggregate",
            "stat": stat,
            "filters": {
                "day": filters.day,
                "date_from": filters.date_from,
                "date_to": filters.date_to,
                "weekday": filters.weekday,
                "hour": filters.hour,
                "min_samples": filters.min_samples,
            },
            "generated_at": now_iso(),
        }
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(fc, f, ensure_ascii=False, indent=2)
    return len(feats)


# ---------------------------
# CLI
# ---------------------------
def main():
    ap = argparse.ArgumentParser(description="Export map FeatureCollection (latest / aggregate).")
    ap.add_argument("db_path", help="SQLite DB path (e.g., data/db/wait_times.db)")
    ap.add_argument("out_path", help="Output JSON path (e.g., data/out/map_latest.json)")
    ap.add_argument("--mode", choices=["latest", "aggregate"], default="latest")

    # latest 用
    ap.add_argument("--fresh-within", type=int, default=None, help="最新モード: 鮮度がN分以内のものだけに制限")

    # aggregate 用
    ap.add_argument("--stat", choices=["avg","max","min","median","p90"], default="avg")
    ap.add_argument("--day", help="特定日をカンマ区切り指定（YYYY-MM-DD[,..]）")
    ap.add_argument("--date-from", dest="date_from", help="開始日 YYYY-MM-DD")
    ap.add_argument("--date-to", dest="date_to", help="終了日 YYYY-MM-DD")
    ap.add_argument("--weekday", help="曜日（日本語）をカンマ区切り（例: 土曜日,日曜日）")
    ap.add_argument("--hour", help="時間帯（HH:00）をカンマ区切り（例: 11:00,12:00）")
    ap.add_argument("--min-samples", type=int, default=3, help="集計に必要な最小サンプル数")

    args = ap.parse_args()

    filters = Filters(
        day = coerce_list(args.day),
        date_from = args.date_from,
        date_to = args.date_to,
        weekday = coerce_list(args.weekday),
        hour = coerce_list(args.hour),
        min_samples = int(args.min_samples),
        fresh_within = args.fresh_within
    )

    con = sqlite3.connect(args.db_path)
    con.row_factory = sqlite3.Row
    try:
        if args.mode == "latest":
            n = export_latest(con, args.out_path, filters.fresh_within)
            print(f"[latest] features={n} → {args.out_path}")
        else:
            n = export_aggregate(con, args.out_path, args.stat, filters)
            print(f"[aggregate:{args.stat}] features={n} → {args.out_path}")
    finally:
        con.close()


if __name__ == "__main__":
    main()
