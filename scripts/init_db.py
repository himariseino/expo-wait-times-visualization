# scripts/init_db.py
import sqlite3, os, sys
DB_PATH = sys.argv[1] if len(sys.argv)>1 else "data/db/app.db"

DDL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS pavilion_dim (
  pavilion_id TEXT PRIMARY KEY,
  name_canonical TEXT NOT NULL,
  area TEXT,
  format TEXT,
  duration_minutes INTEGER
);

CREATE TABLE IF NOT EXISTS pavilion_alias (
  alias TEXT PRIMARY KEY,
  pavilion_id TEXT NOT NULL,
  FOREIGN KEY (pavilion_id) REFERENCES pavilion_dim(pavilion_id)
);

-- geojsonはFeatureでもGeometryでもOK（文字列で保存）
CREATE TABLE IF NOT EXISTS pavilion_geometry (
  pavilion_id TEXT PRIMARY KEY,
  geojson TEXT NOT NULL,
  FOREIGN KEY (pavilion_id) REFERENCES pavilion_dim(pavilion_id)
);

CREATE TABLE IF NOT EXISTS waits_fact (
  timestamp TEXT NOT NULL,
  pavilion_id TEXT NOT NULL,
  pavilion_name_raw TEXT NOT NULL,
  wait_time_raw TEXT,
  post_time_raw TEXT,
  wait_min INTEGER,
  staleness_min INTEGER,
  weekday TEXT,
  hour_range TEXT,
  data_time_jst TEXT,
  PRIMARY KEY (timestamp, pavilion_id),
  FOREIGN KEY (pavilion_id) REFERENCES pavilion_dim(pavilion_id)
);

CREATE TABLE IF NOT EXISTS unresolved_names (
  timestamp TEXT NOT NULL,
  pavilion_name_raw TEXT NOT NULL,
  wait_time_raw TEXT,
  post_time_raw TEXT,
  PRIMARY KEY (timestamp, pavilion_name_raw)
);

CREATE VIEW IF NOT EXISTS v_waits_for_map AS
SELECT
  w.timestamp,
  w.pavilion_id,
  d.name_canonical AS pavilion_name,
  w.wait_min,
  w.staleness_min,
  w.weekday,
  w.hour_range,
  w.data_time_jst,
  g.geojson
FROM waits_fact w
LEFT JOIN pavilion_dim d ON w.pavilion_id = d.pavilion_id
LEFT JOIN pavilion_geometry g ON w.pavilion_id = g.pavilion_id;
"""
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
with sqlite3.connect(DB_PATH) as conn:
    conn.executescript(DDL)
print(f"Initialized: {os.path.abspath(DB_PATH)}")
