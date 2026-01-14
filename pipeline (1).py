import os
import re
from datetime import datetime
import pandas as pd

from config import PARTNER_CONFIG

STANDARD_COLUMNS = [
    "external_id",
    "first_name",
    "last_name",
    "dob",
    "email",
    "phone",
    "partner_code",
]

def parse_dob(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s:
        return None

    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None

def format_phone(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    digits = re.sub(r"\D", "", str(value))
    if len(digits) < 10:
        return None
    digits = digits[-10:]
    return f"{digits[0:3]}-{digits[3:6]}-{digits[6:10]}"

def standardize_dataframe(df, mapping, partner_code):
    df = df.rename(columns=mapping)

    keep_cols = [c for c in mapping.values() if c in df.columns]
    df = df[keep_cols].copy()

    for col in STANDARD_COLUMNS:
        if col not in df.columns:
            df[col] = None

    df["first_name"] = df["first_name"].astype(str).str.strip().str.title().replace({"Nan": None})
    df["last_name"] = df["last_name"].astype(str).str.strip().str.title().replace({"Nan": None})
    df["email"] = df["email"].astype(str).str.strip().str.lower().replace({"nan": None, "": None})

    df["dob"] = df["dob"].apply(parse_dob)
    df["phone"] = df["phone"].apply(format_phone)

    df["partner_code"] = partner_code

    df["external_id"] = df["external_id"].astype(str).str.strip()
    df = df[df["external_id"].notna() & (df["external_id"] != "")].copy()

    return df[STANDARD_COLUMNS]

def ingest_partner(cfg):
    df = pd.read_csv(
        cfg["file_path"],
        delimiter=cfg["delimiter"],
        dtype=str,
        engine="python",
        on_bad_lines="skip",
    )
    return standardize_dataframe(df, cfg["mapping"], cfg["partner_code"])

def run_pipeline(output_path="output/unified_eligibility.csv"):
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    frames = []
    for _, cfg in PARTNER_CONFIG.items():
        frames.append(ingest_partner(cfg))

    unified = pd.concat(frames, ignore_index=True)
    unified.to_csv(output_path, index=False)

    print(f"✅ Output written: {output_path}")
    print(unified)

if __name__ == "__main__":
    run_pipeline()
