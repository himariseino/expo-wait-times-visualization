# scripts/load_master_from_geojson.py
import json, sqlite3, sys, hashlib, unicodedata, re, os
from collections import OrderedDict

DB_PATH = sys.argv[1] if len(sys.argv)>1 else "data/db/app.db"
GEOJSON_PATH = sys.argv[2] if len(sys.argv)>2 else "data/geo/pavilions_points.geojson"

def normalize_name(s: str) -> str:
    s = unicodedata.normalize("NFKC", s)
    s = s.strip()
    s = re.sub(r"\s+", "", s)        # 空白除去
    return s

def make_pavilion_id(name: str) -> str:
    # 日本語でも安定する短いID: pav_ + sha1先頭8桁
    base = normalize_name(name)
    return "pav_" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]

def main():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with open(GEOJSON_PATH, "r", encoding="utf-8") as f:
        fc = json.load(f)

    # 名前重複（例: アイルランドが2回）を除去（最初に出たものを採用）
    by_name = OrderedDict()
    for feat in fc["features"]:
        name = feat.get("properties", {}).get("pavilion_name")
        if not name:
            continue
        if name not in by_name:
            by_name[name] = feat

    rows_dim = []
    rows_alias = []
    rows_geom = []

    for name, feat in by_name.items():
        props = feat.get("properties", {}) or {}
        area = props.get("area")
        duration = props.get("duration_minutes")
        pavilion_id = make_pavilion_id(name)

        rows_dim.append((pavilion_id, name, area, None, duration if duration is not None else None))
        # とりあえず正規名＝別名1本で登録（後でエイリアスを追加）
        rows_alias.append((normalize_name(name), pavilion_id))

        # geometry or feature全文を保存（ここではFeature全文を推奨）
        geojson_str = json.dumps({
            "type": "Feature",
            "properties": {},   # プロパティはDB側のJOINで付与する前提
            "geometry": feat.get("geometry")
        }, ensure_ascii=False)
        rows_geom.append((pavilion_id, geojson_str))

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        # 重複を上書きしたくない場合は INSERT OR IGNORE に
        cur.executemany(
            """INSERT OR REPLACE INTO pavilion_dim
               (pavilion_id, name_canonical, area, format, duration_minutes)
               VALUES (?, ?, ?, ?, ?)""",
            rows_dim,
        )
        cur.executemany(
            """INSERT OR IGNORE INTO pavilion_alias
               (alias, pavilion_id) VALUES (?, ?)""",
            rows_alias,
        )
        cur.executemany(
            """INSERT OR REPLACE INTO pavilion_geometry
               (pavilion_id, geojson) VALUES (?, ?)""",
            rows_geom,
        )
        conn.commit()
    print(f"Inserted DIM:{len(rows_dim)}  ALIAS:{len(rows_alias)}  GEOM:{len(rows_geom)}")

if __name__ == "__main__":
    main()
