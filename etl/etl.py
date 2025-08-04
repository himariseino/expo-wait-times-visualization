import pandas as pd
import sqlite3
import re
import os
import logging
from datetime import datetime
import yaml
from pathlib import Path

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# 設定読み込み
config_path = Path(__file__).parent / "config/config.yaml"
with open(config_path, 'r') as yml:
    config = yaml.safe_load(yml)

# プロジェクトルートを取得
ROOT_DIR = Path(__file__).resolve().parents[1]

# ファイルパス設定
RAW_CSV_PATH = ROOT_DIR / config["RAW_CSV_PATH"]
MASTER_CSV_PATH = ROOT_DIR / config["MASTER_CSV_PATH"]
DB_PATH = ROOT_DIR / config["DB_PATH"]
TABLE_NAME = config["TABLE_NAME"]

def load_raw_data(csv_path: str) -> pd.DataFrame:
    """RawのCSVデータを読み込む"""
    return pd.read_csv(csv_path)

def parse_wait_time(raw: str) -> int | None:
    """'1時間30分'などの文字列を分に変換"""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    if "情報なし" in raw:
        return None
    pattern = re.match(r"(\d+)時間(\d+)分", raw)
    if pattern:
        return int(pattern.group(1)) * 60 + int(pattern.group(2))
    pattern2 = re.match(r"(\d+)時間(\d+)分以下", raw)
    if pattern2:
        return int(pattern2.group(1)) * 60 + int(pattern2.group(2))
    return None

def enrich_time_features(df: pd.DataFrame) -> pd.DataFrame:
    """曜日・時間帯を追加"""
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"])
    df["weekday"] = df["timestamp_dt"].dt.day_name(locale="ja_JP")
    df["hour_range"] = df["timestamp_dt"].dt.strftime("%H:00")
    df.drop(columns=["timestamp_dt"], inplace=True)
    return df

def join_pavilion_info(df: pd.DataFrame, master_df: pd.DataFrame) -> pd.DataFrame:
    """パビリオンのエリア・形式・所要時間を付与"""
    return df.merge(master_df, on="pavilion_name", how="left")

def validate_and_clean(df: pd.DataFrame) -> pd.DataFrame:
    """データの欠損・重複を除去"""
    before = len(df)
    df = df.drop_duplicates(subset=["timestamp", "pavilion_name"])
    after = len(df)
    logger.info(f"バリデーションにより {before - after} 件を除外")
    return df

def save_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str):
    """整形後データをSQLiteに保存（DB既存レコードと重複除外）"""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    with sqlite3.connect(db_path) as conn:
        # テーブル作成（なければ）
        conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TEXT NOT NULL,
            pavilion_name TEXT NOT NULL,
            wait_time_raw TEXT,
            post_time_raw TEXT,
            wait_time_minutes INTEGER,
            weekday TEXT,
            hour_range TEXT,
            area TEXT,
            format TEXT,
            duration_minutes INTEGER,
            PRIMARY KEY (timestamp, pavilion_name)
        );
        """)

        # 既存の (timestamp, pavilion_name) を取得
        existing_keys = pd.read_sql_query(
            f"SELECT timestamp, pavilion_name FROM {table_name};", conn
        )

        # 重複除外
        if not existing_keys.empty:
            merged = df.merge(
                existing_keys,
                on=["timestamp", "pavilion_name"],
                how="left",
                indicator=True
            )
            df = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])

        # 保存
        if len(df) > 0:
            df.to_sql(table_name, conn, if_exists="append", index=False)
            logger.info(f"DB保存先: {db_path.resolve()}")
            logger.info(f"{len(df)}件をSQLiteに保存しました（既存と重複除外済み）")
        else:
            logger.info("新規データがないため、DBへの保存はスキップしました。")

def main():
    logger.info("ETL処理を開始します")

    raw_df = load_raw_data(RAW_CSV_PATH)
    logger.info(f"Rawデータ件数: {len(raw_df)}")

    raw_df["wait_time_minutes"] = raw_df["wait_time_raw"].apply(parse_wait_time)
    raw_df = enrich_time_features(raw_df)

    master_df = pd.read_csv(MASTER_CSV_PATH)
    enriched_df = join_pavilion_info(raw_df, master_df)
    logger.debug(f"結合後のカラム一覧: {enriched_df.columns.tolist()}")

    cleaned_df = validate_and_clean(enriched_df)
    save_to_sqlite(cleaned_df, DB_PATH, TABLE_NAME)

    logger.info("ETL処理が完了しました")

if __name__ == "__main__":
    main()
