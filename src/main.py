import os
import re
import pandas as pd
import numpy as np

DATA_DIR = "data"
OUT_PROFILING_DIR = "profiling"
OUT_REPORT_DIR = "output"
OUT_DOCS_DIR = "docs"

os.makedirs(OUT_PROFILING_DIR, exist_ok=True)
os.makedirs(OUT_REPORT_DIR, exist_ok=True)
os.makedirs(OUT_DOCS_DIR, exist_ok=True)

PROFILE_FILE = os.path.join(OUT_PROFILING_DIR, "data_profiling_summary.csv")
REPORT_FILE = os.path.join(OUT_REPORT_DIR, "referral_validation_report.csv")

TABLE_FILES = [
    "lead_logs.csv",
    "paid_transactions.csv",
    "referral_rewards.csv",
    "user_logs.csv",
    "user_referral_logs.csv",
    "user_referral_statuses.csv",
    "user_referrals.csv",
]

def parse_utc(ts):
    if pd.isna(ts):
        return pd.NaT
    return pd.to_datetime(str(ts), utc=True, errors="coerce")

def convert_to_local(utc_series, tz_series, default_tz="UTC"):
    out = []
    for ts, tz in zip(utc_series, tz_series):
        if pd.isna(ts):
            out.append(pd.NaT)
            continue
        ts = parse_utc(ts)
        tzname = tz if isinstance(tz, str) and tz.strip() else default_tz
        try:
            out.append(ts.tz_convert(tzname).tz_localize(None))
        except Exception:
            out.append(ts.tz_convert("UTC").tz_localize(None))
    return pd.to_datetime(out, errors="coerce")

def initcap_safe(x):
    if pd.isna(x):
        return x
    return str(x).title()

def profile_df(df, table_name):
    rows = []
    n = len(df)
    for col in df.columns:
        s = df[col]
        rows.append({
            "table_name": table_name,
            "column_name": col,
            "data_type": str(s.dtype),
            "row_count": n,
            "null_count": int(s.isna().sum()),
            "distinct_count": int(s.nunique(dropna=True)),
        })
    return pd.DataFrame(rows)

def main():
    # Load all CSVs
    dfs = {}
    for fname in TABLE_FILES:
        path = os.path.join(DATA_DIR, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file: {path}")
        dfs[fname.replace(".csv", "")] = pd.read_csv(path)

    # Step 1: Profiling
    profiles = []
    for tname, df in dfs.items():
        profiles.append(profile_df(df, tname))
    pd.concat(profiles, ignore_index=True).to_csv(PROFILE_FILE, index=False)

    # Aliases
    lead_logs = dfs["lead_logs"].copy()
    paid_transactions = dfs["paid_transactions"].copy()
    referral_rewards = dfs["referral_rewards"].copy()
    user_logs = dfs["user_logs"].copy()
    user_referral_logs = dfs["user_referral_logs"].copy()
    user_referral_statuses = dfs["user_referral_statuses"].copy()
    user_referrals = dfs["user_referrals"].copy()

    # Parse timestamps
    for col in ["referral_at", "updated_at"]:
        user_referrals[col] = user_referrals[col].apply(parse_utc)

    user_referral_logs["created_at"] = user_referral_logs["created_at"].apply(parse_utc)
    user_referral_statuses["created_at"] = user_referral_statuses["created_at"].apply(parse_utc)
    referral_rewards["created_at"] = referral_rewards["created_at"].apply(parse_utc)
    paid_transactions["transaction_at"] = paid_transactions["transaction_at"].apply(parse_utc)
    lead_logs["created_at"] = lead_logs["created_at"].apply(parse_utc)

    user_logs["membership_expired_date"] = pd.to_datetime(
        user_logs["membership_expired_date"], errors="coerce"
    ).dt.date

    # Deduplicate user_logs to latest snapshot per user_id
    user_logs_latest = (
        user_logs.sort_values("id")
        .drop_duplicates(subset=["user_id"], keep="last")
        .copy()
    )

    # Latest referral log per user_referral_id
    latest_log = (
        user_referral_logs.sort_values(["user_referral_id", "created_at", "id"])
        .drop_duplicates(subset=["user_referral_id"], keep="last")
        .rename(columns={"id": "referral_details_id", "created_at": "latest_referral_log_at"})
    )

    granted = user_referral_logs[user_referral_logs["is_reward_granted"] == True].copy()
    reward_granted_at = (
        granted.sort_values(["user_referral_id", "created_at"])
        .groupby("user_referral_id", as_index=False)["created_at"]
        .first()
        .rename(columns={"created_at": "reward_granted_at"})
    )

    reward_granted_flag = (
        user_referral_logs.groupby("user_referral_id", as_index=False)["is_reward_granted"]
        .max()
        .rename(columns={"is_reward_granted": "is_reward_granted_any"})
    )

    # Base join
    df = (
        user_referrals.merge(
            latest_log[
                ["user_referral_id", "referral_details_id", "source_transaction_id", "latest_referral_log_at", "is_reward_granted"]
            ],
            left_on="referral_id",
            right_on="user_referral_id",
            how="left",
        )
        .drop(columns=["user_referral_id"], errors="ignore")
    )

    df = (
        df.merge(reward_granted_at, left_on="referral_id", right_on="user_referral_id", how="left")
        .drop(columns=["user_referral_id"], errors="ignore")
    )
    df = (
        df.merge(reward_granted_flag, left_on="referral_id", right_on="user_referral_id", how="left")
        .drop(columns=["user_referral_id"], errors="ignore")
    )

    # Status
    df = df.merge(
        user_referral_statuses[["id", "description"]].rename(
            columns={"id": "user_referral_status_id", "description": "referral_status"}
        ),
        on="user_referral_status_id",
        how="left",
    )

    # Rewards
    rewards = referral_rewards.rename(columns={"id": "referral_reward_id"}).copy()
    rewards["num_reward_days"] = (
        rewards["reward_value"].astype(str).str.extract(r"(\d+)").astype(float).astype("Int64")
    )
    df = df.merge(rewards[["referral_reward_id", "reward_value", "num_reward_days"]], on="referral_reward_id", how="left")

    # Transactions
    df["transaction_id_final"] = df["transaction_id"].fillna(df["source_transaction_id"])
    tx = paid_transactions.rename(columns={"transaction_id": "transaction_id_final"}).copy()
    df = df.merge(tx, on="transaction_id_final", how="left")

    # Lead logs dedupe + join
    lead_latest = (
        lead_logs.sort_values(["lead_id", "created_at", "id"])
        .drop_duplicates(subset=["lead_id"], keep="last")
        .rename(columns={"lead_id": "referee_id"})
    )
    df = df.merge(lead_latest[["referee_id", "source_category", "timezone_location"]], on="referee_id", how="left")

    # Referrer info
    referrer = user_logs_latest.rename(columns={
        "user_id": "referrer_id",
        "name": "referrer_name",
        "phone_number": "referrer_phone_number",
        "homeclub": "referrer_homeclub",
        "timezone_homeclub": "referrer_timezone",
        "membership_expired_date": "referrer_membership_expired_date",
        "is_deleted": "referrer_is_deleted",
    })
    df = df.merge(
        referrer[[
            "referrer_id", "referrer_name", "referrer_phone_number", "referrer_homeclub",
            "referrer_timezone", "referrer_membership_expired_date", "referrer_is_deleted"
        ]],
        on="referrer_id", how="left"
    )

    # Referee info
    referee = user_logs_latest.rename(columns={
        "user_id": "referee_id",
        "name": "referee_name_userlog",
        "phone_number": "referee_phone_userlog",
        "timezone_homeclub": "referee_timezone",
    })
    df = df.merge(referee[["referee_id", "referee_name_userlog", "referee_phone_userlog", "referee_timezone"]], on="referee_id", how="left")
    df["referee_name"] = df["referee_name"].fillna(df["referee_name_userlog"])
    df["referee_phone"] = df["referee_phone"].fillna(df["referee_phone_userlog"])

    # referral_source_category logic
    def derive_source_category(row):
        rs = row.get("referral_source")
        if rs == "User Sign Up":
            return "Online"
        if rs == "Draft Transaction":
            return "Offline"
        if rs == "Lead":
            return row.get("source_category")
        return np.nan

    df["referral_source_category"] = df.apply(derive_source_category, axis=1)

    # Timezone conversions
    df["referral_at_local"] = convert_to_local(df["referral_at"], df["referrer_timezone"])
    df["updated_at_local"] = convert_to_local(df["updated_at"], df["referrer_timezone"])
    df["transaction_at_local"] = convert_to_local(df["transaction_at"], df["timezone_transaction"])
    df["reward_granted_at_local"] = convert_to_local(df["reward_granted_at"], df["referrer_timezone"])

    # Initcap strings (except club name)
    for col in ["referral_source", "referral_source_category", "referral_status", "transaction_status", "transaction_type", "transaction_location"]:
        if col in df.columns:
            df[col] = df[col].apply(initcap_safe)

    df["referrer_name"] = df["referrer_name"].apply(initcap_safe)
    df["referee_name"] = df["referee_name"].apply(initcap_safe)

    # Business logic
    reward_days = df["num_reward_days"].fillna(0).astype(int)
    has_reward_value = df["num_reward_days"].notna() & (reward_days > 0)

    status_berhasil = df["referral_status"].eq("Berhasil")
    status_pending_failed = df["referral_status"].isin(["Menunggu", "Tidak Berhasil"])

    has_txid = df["transaction_id_final"].notna()
    tx_paid = df["transaction_status"].eq("Paid")
    tx_new = df["transaction_type"].eq("New")

    tx_after_ref = (
        df["transaction_at_local"].notna()
        & df["referral_at_local"].notna()
        & (df["transaction_at_local"] > df["referral_at_local"])
    )

    same_month = (
        df["transaction_at_local"].notna()
        & df["referral_at_local"].notna()
        & (df["transaction_at_local"].dt.to_period("M") == df["referral_at_local"].dt.to_period("M"))
    )

    ref_date = df["referral_at_local"].dt.date
    mem_ok = df["referrer_membership_expired_date"].notna() & (df["referrer_membership_expired_date"] >= ref_date)

    not_deleted = df["referrer_is_deleted"].fillna(False).astype(bool).eq(False)
    reward_granted = df["is_reward_granted_any"].fillna(False).astype(bool).eq(True)

    valid_cond1 = (
        has_reward_value & status_berhasil & has_txid & tx_paid & tx_new & tx_after_ref &
        same_month & mem_ok & not_deleted & reward_granted
    )
    valid_cond2 = status_pending_failed & (~has_reward_value)
    is_valid = valid_cond1 | valid_cond2

    invalid = pd.Series(False, index=df.index)
    invalid |= has_reward_value & (~status_berhasil)
    invalid |= has_reward_value & (~has_txid)
    invalid |= (~has_reward_value) & has_txid & tx_paid & tx_after_ref
    invalid |= status_berhasil & (~has_reward_value)
    invalid |= (
        df["transaction_at_local"].notna() & df["referral_at_local"].notna() &
        (df["transaction_at_local"] < df["referral_at_local"])
    )

    df["is_business_logic_valid"] = (is_valid & ~invalid)

    # Output report
    out = pd.DataFrame({
        "referral_details_id": df["referral_details_id"].astype("Int64"),
        "referral_id": df["referral_id"].astype(str),
        "referral_source": df["referral_source"],
        "referral_source_category": df["referral_source_category"],
        "referral_at": df["referral_at_local"],
        "referrer_id": df["referrer_id"].astype(str),
        "referrer_name": df["referrer_name"],
        "referrer_phone_number": df["referrer_phone_number"],
        "referrer_homeclub": df["referrer_homeclub"],
        "referee_id": df["referee_id"].astype(str),
        "referee_name": df["referee_name"],
        "referee_phone": df["referee_phone"],
        "referral_status": df["referral_status"],
        "num_reward_days": df["num_reward_days"].astype("Int64"),
        "transaction_id": df["transaction_id_final"],
        "transaction_status": df["transaction_status"],
        "transaction_at": df["transaction_at_local"],
        "transaction_location": df["transaction_location"],
        "transaction_type": df["transaction_type"],
        "updated_at": df["updated_at_local"],
        "reward_granted_at": df["reward_granted_at_local"],
        "is_business_logic_valid": df["is_business_logic_valid"].astype(bool),
    })

    out.to_csv(REPORT_FILE, index=False)

    print("✅ Done.")
    print(f"Profiling written to: {PROFILE_FILE}")
    print(f"Report written to:    {REPORT_FILE}")
    print(f"Rows in report:       {len(out)}")

if __name__ == "__main__":
    main()
