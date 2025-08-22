#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw CSV の pavilion_name から、最低限の pavilion_dim / pavilion_alias を初期化するスクリプト。
- pavilion_id は 正規化名のSHA1から安定生成（pav_XXXXXXXXXX）
- alias は「正規化済み表記」をキー（ETLの normalize_name と同じ規則）
- 既存行は INSERT OR IGNORE で温存
"""

import sqlite3, pandas as pd, re, unicodedata, hashlib, yaml
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # /app
CONFIG = ROOT / "etl/config/config.yaml"

def normalize_name(s: str) -> str:
    if not isinstance(s, str): return ""
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r"\s+", "", s.strip())

def pav_id_from_norm(norm: str) -> str:
    h = hashlib.sha1(norm.encode("utf-8")).hexdigest()[:10]
    return f"pav_{h}"

def main():
    with open(CONFIG, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    raw_csv = ROOT / cfg["RAW_CSV_PATH"]
    db_path = ROOT / cfg["DB_PATH"]

    df = pd.read_csv(raw_csv, usecols=["pavilion_name"]).dropna()
    # 全ユニーク表記
    names = df["pavilion_name"].astype(str).unique().tolist()

    # 正規化名 → 代表表記（最初に出たもの）
    norm_to_repr = {}
    for name in names:
        norm = normalize_name(name)
        if not norm: continue
        norm_to_repr.setdefault(norm, name)

    print(f"unique names={len(names)}, normalized unique={len(norm_to_repr)}")

    # DDL
    ddl_dim = """
    CREATE TABLE IF NOT EXISTS pavilion_dim (
      pavilion_id TEXT PRIMARY KEY,
      name_canonical TEXT NOT NULL,
      area TEXT
    );
    """
    ddl_alias = """
    CREATE TABLE IF NOT EXISTS pavilion_alias (
      alias TEXT PRIMARY KEY,           -- 正規化済み別名（ETLの normalize_name と一致させる）
      pavilion_id TEXT NOT NULL,
      FOREIGN KEY (pavilion_id) REFERENCES pavilion_dim(pavilion_id)
    );
    """
    ddl_geom = """
    CREATE TABLE IF NOT EXISTS pavilion_geometry (
      pavilion_id TEXT PRIMARY KEY,
      geojson TEXT,
      FOREIGN KEY (pavilion_id) REFERENCES pavilion_dim(pavilion_id)
    );
    """

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")
        conn.execute(ddl_dim)
        conn.execute(ddl_alias)
        conn.execute(ddl_geom)

        # INSERT OR IGNORE
        dim_rows = []
        alias_rows = []
        for norm, rep in norm_to_repr.items():
            pid = pav_id_from_norm(norm)
            dim_rows.append((pid, rep, None))
            alias_rows.append((norm, pid))

        conn.executemany(
            "INSERT OR IGNORE INTO pavilion_dim(pavilion_id, name_canonical, area) VALUES (?,?,?)",
            dim_rows
        )
        conn.executemany(
            "INSERT OR IGNORE INTO pavilion_alias(alias, pavilion_id) VALUES (?,?)",
            alias_rows
        )
        conn.commit()

    print(f"inserted (or kept) pavilion_dim={len(dim_rows)}, pavilion_alias={len(alias_rows)}")
    print(f"DB: {db_path}")

if __name__ == "__main__":
    main()
