import os
import pandas as pd

DATA_DIR = "data"
OUT_DIR = "docs"
OUT_FILE = os.path.join(OUT_DIR, "Data_Dictionary.xlsx")

os.makedirs(OUT_DIR, exist_ok=True)

TABLE_FILES = [
    "lead_logs.csv",
    "paid_transactions.csv",
    "referral_rewards.csv",
    "user_logs.csv",
    "user_referral_logs.csv",
    "user_referral_statuses.csv",
    "user_referrals.csv",
]

def semantic(dtype: str) -> str:
    d = dtype.lower()
    if "int" in d:
        return "integer"
    if "float" in d:
        return "decimal"
    if "bool" in d:
        return "boolean"
    if "datetime" in d:
        return "timestamp"
    return "string"

rows = []

for f in TABLE_FILES:
    path = os.path.join(DATA_DIR, f)
    df = pd.read_csv(path)
    table = f.replace(".csv", "")
    for col in df.columns:
        s = df[col]
        rows.append({
            "table_name": table,
            "column_name": col,
            "data_type": str(s.dtype),
            "semantic_type": semantic(str(s.dtype)),
            "null_count": int(s.isna().sum()),
            "distinct_count": int(s.nunique(dropna=True)),
            "example_value": "" if s.dropna().empty else str(s.dropna().iloc[0])[:200],
            "business_description": ""
        })

dd = pd.DataFrame(rows).sort_values(["table_name", "column_name"])

with pd.ExcelWriter(OUT_FILE, engine="openpyxl") as writer:
    dd.to_excel(writer, sheet_name="data_dictionary", index=False)

print(f" Data Dictionary written to {OUT_FILE}")
