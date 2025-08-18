# /app/scripts/migrate_phase0.py
import sqlite3, sys

DB = sys.argv[1] if len(sys.argv)>1 else "data/db/wait_times.db"

DDL_VIEW = """
DROP VIEW IF EXISTS v_waits_for_map;
CREATE VIEW v_waits_for_map AS
SELECT
  w.timestamp,
  w.pavilion_id,
  d.name_canonical AS pavilion_name,
  w.wait_min,
  w.staleness_min,
  w.weekday,
  w.hour_range,
  w.day,
  w.hour,
  w.data_time_jst,
  g.geojson
FROM waits_fact w
LEFT JOIN pavilion_dim d ON w.pavilion_id = d.pavilion_id
LEFT JOIN pavilion_geometry g ON w.pavilion_id = g.pavilion_id
WHERE w.wait_min IS NOT NULL
  AND g.geojson IS NOT NULL;
"""

def column_exists(cur, table, col):
    cur.execute(f"PRAGMA table_info({table})")
    return any(r[1]==col for r in cur.fetchall())

with sqlite3.connect(DB) as conn:
    c = conn.cursor()
    # 1) waits_fact に day/hour が無ければ追加
    if not column_exists(c, "waits_fact", "day"):
        c.execute("ALTER TABLE waits_fact ADD COLUMN day TEXT")
    if not column_exists(c, "waits_fact", "hour"):
        c.execute("ALTER TABLE waits_fact ADD COLUMN hour TEXT")

    # 2) 既存データのバックフィル（data_time_jst から day、hour_range から hour）
    c.execute("UPDATE waits_fact SET day  = substr(coalesce(data_time_jst,''),1,10) WHERE day IS NULL OR day='';")
    c.execute("UPDATE waits_fact SET hour = hour_range WHERE hour IS NULL OR hour='';")

    # 3) インデックス
    c.execute("CREATE INDEX IF NOT EXISTS idx_waits_day_hour ON waits_fact(day, hour, pavilion_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_waits_weekday_hour ON waits_fact(weekday, hour, pavilion_id)")

    # 4) ビューを再作成（描画対象のみ）
    conn.executescript(DDL_VIEW)

    conn.commit()
print(f"migrated: {DB}")
