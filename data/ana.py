import pandas as pd
from pathlib import Path
path = Path(__file__).parent / "wait_times.csv"
output_path = Path(__file__).parent / "pavilion_master.csv"

# with open(path, encoding='utf-8') as f:
#     for i, line in enumerate(f, start=1):
#         if line.count(',') != 3:
#             print(f"Line {i}: {line.strip()} (commas: {line.count(',')})")


df = pd.read_csv(path, header=None)

df.columns = ["取得時刻", "パビリオン名", "待ち時間", "投稿からの経過時間"]

unique_pavilions = df["パビリオン名"].dropna().unique()

# データフレームにして保存
pd.DataFrame(unique_pavilions, columns=["パビリオン名"]).to_csv(output_path, index=False, encoding="utf-8")

print(f"{len(unique_pavilions)} 件のユニークなパビリオン名を {output_path} に出力しました。")