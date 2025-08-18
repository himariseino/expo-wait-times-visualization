import pandas as pd
import sqlite3
import re
import os
import logging
import unicodedata
from datetime import datetime
import yaml
from pathlib import Path
from typing import Optional

# ----------------------------------
# ログ設定
# ----------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# ----------------------------------
# 設定読み込み
# ----------------------------------
config_path = Path(__file__).parent / "config/config.yaml"
with open(config_path, 'r', encoding='utf-8') as yml:
    config = yaml.safe_load(yml)

# プロジェクトルート
ROOT_DIR = Path(__file__).resolve().parents[1]

# パス設定（RAW_CSV_PATH, DB_PATH は config.yaml に定義）
RAW_CSV_PATH = ROOT_DIR / config["RAW_CSV_PATH"]
DB_PATH = ROOT_DIR / config["DB_PATH"]

# 旧版互換（未使用でもエラーにしない）
TABLE_NAME = config.get("TABLE_NAME")
MASTER_CSV_PATH = ROOT_DIR / config.get("MASTER_CSV_PATH", "data/master/unused.csv")

# ----------------------------------
# 正規表現パターン
# ----------------------------------
WAIT_PATTERNS = [
    (re.compile(r"^\s*(\d+)時間(\d+)分以下\s*$"), lambda h, m: int(h)*60 + int(m)),
    (re.compile(r"^\s*(\d+)時間(\d+)分\s*$"),     lambda h, m: int(h)*60 + int(m)),
    (re.compile(r"^\s*(\d+)分以下\s*$"),          lambda m: int(m)),
    (re.compile(r"^\s*(\d+)分\s*$"),              lambda m: int(m)),
]

STALE_PATTERNS = [
    (re.compile(r"^\s*(\d+)分前\s*$"),   lambda m: int(m)),
    (re.compile(r"^\s*(\d+)時間前\s*$"), lambda h: int(h)*60),
    (re.compile(r"^\s*60分以上前\s*$"),  lambda: 60),  # 運用方針：最低60分として扱う
]

# ----------------------------------
# ユーティリティ
# ----------------------------------
def normalize_name(s: str) -> str:
    """表記揺れ吸収のための正規化（全半角、空白除去）"""
    if not isinstance(s, str):
        return s
    s = unicodedata.normalize("NFKC", s)
    return re.sub(r"\s+", "", s.strip())

def load_raw_data(csv_path: Path) -> pd.DataFrame:
    """RawのCSVデータを読み込む"""
    df = pd.read_csv(csv_path)
    expected = {"timestamp", "pavilion_name", "wait_time_raw", "post_time_raw"}
    missing = expected - set(df.columns)
    if missing:
        raise ValueError(f"Raw CSV に必須カラムが不足しています: {missing}")
    return df

def parse_wait_time(raw: Optional[str]) -> Optional[int]:
    """'1時間30分', '0時間10分以下', '15分', '15分以下' 等を分に変換"""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if s in ("情報なし", "—", "-", "不明", ""):
        return None
    for pat, calc in WAIT_PATTERNS:
        m = pat.match(s)
        if m:
            return calc(*m.groups())
    return None

def parse_staleness_min(raw: Optional[str]) -> Optional[int]:
    """'12分前', '1時間前', '60分以上前' → 分"""
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    for pat, calc in STALE_PATTERNS:
        m = pat.match(s)
        if m:
            return calc(*m.groups()) if m.groups() else calc()
    return None

def enrich_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """曜日・時間帯を追加（日本語曜日／時刻帯）"""
    ts = pd.to_datetime(df["timestamp"], errors="coerce")
    if ts.isna().any():
        bad = df.loc[ts.isna(), "timestamp"].head(3).tolist()
        logger.warning(f"timestamp 変換失敗が存在します。例: {bad}")

    df["timestamp_dt"] = ts

    weekday_map = {
        "Monday": "月曜日", "Tuesday": "火曜日", "Wednesday": "水曜日",
        "Thursday": "木曜日", "Friday": "金曜日", "Saturday": "土曜日", "Sunday": "日曜日"
    }
    weekday_en = df["timestamp_dt"].dt.day_name()
    df["weekday"] = weekday_en.map(weekday_map)
    df["hour_range"] = df["timestamp_dt"].dt.strftime("%H:00")

    # data_time_jst：タイムゾーンがあればJSTへ、なければそのまま
    try:
        if pd.api.types.is_datetime64tz_dtype(df["timestamp_dt"].dtype):
            df["data_time_jst"] = df["timestamp_dt"].dt.tz_convert("Asia/Tokyo").dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        else:
            df["data_time_jst"] = df["timestamp_dt"].dt.strftime("%Y-%m-%dT%H:%M:%S")
    except Exception as e:
        logger.warning(f"data_time_jst の生成で例外: {e}. 文字列化にフォールバックします。")
        df["data_time_jst"] = df["timestamp_dt"].astype(str)

    df.drop(columns=["timestamp_dt"], inplace=True)
    return df

def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """データの欠損・重複を除去"""
    before = len(df)
    df = df.drop_duplicates(subset=["timestamp", "pavilion_name"])
    after = len(df)
    if before - after > 0:
        logger.info(f"バリデーション: 重複 {before - after} 件を除外")
    return df

def load_alias_map_from_db(db_path: Path) -> dict[str, str]:
    """pavilion_alias テーブルから名寄せ辞書を作成（alias 正規化済みで保存されている想定）"""
    with sqlite3.connect(str(db_path)) as conn:
        try:
            rows = conn.execute("SELECT alias, pavilion_id FROM pavilion_alias;").fetchall()
        except sqlite3.OperationalError as e:
            raise RuntimeError(
                f"pavilion_alias テーブルが見つかりません。DBマイグレーションとマスター投入を先に実行してください。詳細: {e}"
            )
    return {alias: pid for (alias, pid) in rows}

def resolve_pavilion_id(name: Optional[str], alias_map: dict[str, str]) -> Optional[str]:
    if not isinstance(name, str):
        return None
    return alias_map.get(normalize_name(name))

# ----------------------------------
# DB I/O（waits_fact UPSERT / unresolved_names）
# ----------------------------------
WAITS_FACT_DDL = """
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
  day TEXT,     -- 追加
  hour TEXT,    -- 追加
  PRIMARY KEY (timestamp, pavilion_id),
  FOREIGN KEY (pavilion_id) REFERENCES pavilion_dim(pavilion_id)
);
"""


UNRESOLVED_DDL = """
CREATE TABLE IF NOT EXISTS unresolved_names (
  timestamp TEXT NOT NULL,
  pavilion_name_raw TEXT NOT NULL,
  wait_time_raw TEXT,
  post_time_raw TEXT,
  PRIMARY KEY (timestamp, pavilion_name_raw)
);
"""

def ensure_etl_tables(conn: sqlite3.Connection):
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute(WAITS_FACT_DDL)
    conn.execute(UNRESOLVED_DDL)

def upsert_waits_fact(conn: sqlite3.Connection, df: pd.DataFrame):
    """waits_fact へ主キー(timestamp, pavilion_id)で重複なくINSERT"""
    # 必要カラム整形
    cols_map = {
        "timestamp": "timestamp",
        "pavilion_id": "pavilion_id",
        "pavilion_name": "pavilion_name_raw",
        "wait_time_raw": "wait_time_raw",
        "post_time_raw": "post_time_raw",
        "wait_time_minutes": "wait_min",
        "staleness_min": "staleness_min",
        "weekday": "weekday",
        "hour_range": "hour_range",
        "data_time_jst": "data_time_jst",
        "day": "day",     # 追加
        "hour": "hour",   # 追加
    }

    need_cols = list(cols_map.keys())
    for c in need_cols:
        if c not in df.columns:
            df[c] = None

    insert_df = df[need_cols].rename(columns=cols_map)
    insert_df.to_sql("_tmp_waits_fact", conn, if_exists="replace", index=False)

    conn.execute("""
        INSERT OR IGNORE INTO waits_fact
        (timestamp, pavilion_id, pavilion_name_raw, wait_time_raw, post_time_raw,
        wait_min, staleness_min, weekday, hour_range, data_time_jst, day, hour)   -- 追加
        SELECT timestamp, pavilion_id, pavilion_name_raw, wait_time_raw, post_time_raw,
            wait_min, staleness_min, weekday, hour_range, data_time_jst, day, hour -- 追加
        FROM _tmp_waits_fact;
    """)
    conn.execute("DROP TABLE _tmp_waits_fact;")

def insert_unresolved(conn: sqlite3.Connection, df_unresolved: pd.DataFrame):
    if df_unresolved.empty:
        return

    # 必要列・重複・欠損を整理
    df_tmp = (df_unresolved.rename(columns={"pavilion_name": "pavilion_name_raw"})
              [["timestamp", "pavilion_name_raw", "wait_time_raw", "post_time_raw"]]
              .dropna(subset=["timestamp", "pavilion_name_raw"])
              .drop_duplicates(subset=["timestamp", "pavilion_name_raw"])
             )

    # pandas→一時表→INSERT OR IGNORE で本表へ
    df_tmp.to_sql("_tmp_unresolved", conn, if_exists="replace", index=False)
    conn.execute("""
        INSERT OR IGNORE INTO unresolved_names
        (timestamp, pavilion_name_raw, wait_time_raw, post_time_raw)
        SELECT timestamp, pavilion_name_raw, wait_time_raw, post_time_raw
          FROM _tmp_unresolved;
    """)
    conn.execute("DROP TABLE _tmp_unresolved;")


# ----------------------------------
# メイン処理
# ----------------------------------
def main():
    logger.info("ETL処理を開始します")
    logger.info(f"RAW_CSV_PATH: {RAW_CSV_PATH}")
    logger.info(f"DB_PATH: {DB_PATH}")

    # 1) Raw 読み込み
    raw_df = load_raw_data(RAW_CSV_PATH)
    logger.info(f"Rawデータ件数: {len(raw_df)}")

    # 2) 待ち時間・鮮度の正規化
    raw_df["wait_time_minutes"] = raw_df["wait_time_raw"].apply(parse_wait_time)
    raw_df["staleness_min"] = raw_df["post_time_raw"].apply(parse_staleness_min)

    # 3) 時間特徴量付与
    raw_df = enrich_time_features(raw_df)

    # 4) 名寄せ（pavilion_name → pavilion_id）
    alias_map = load_alias_map_from_db(DB_PATH)
    raw_df["pavilion_id"] = raw_df["pavilion_name"].apply(lambda n: resolve_pavilion_id(n, alias_map))
    raw_df["day"]  = raw_df["data_time_jst"].str.slice(0, 10)
    raw_df["hour"] = raw_df["hour_range"]

    unresolved = raw_df[raw_df["pavilion_id"].isna()][["timestamp", "pavilion_name", "wait_time_raw", "post_time_raw"]]
    resolved = raw_df[raw_df["pavilion_id"].notna()].copy()

    # 5) バリデーション（重複除去）
    cleaned_df = validate_and_clean(resolved)

    # 6) DB 書き込み
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    with sqlite3.connect(str(DB_PATH)) as conn:
        ensure_etl_tables(conn)
        upsert_waits_fact(conn, cleaned_df)
        if not unresolved.empty:
            insert_unresolved(conn, unresolved)
            logger.warning(f"未解決パビリオン名: {len(unresolved)} 件（unresolved_names へ退避）")

    logger.info("ETL処理が完了しました")

if __name__ == "__main__":
    main()
