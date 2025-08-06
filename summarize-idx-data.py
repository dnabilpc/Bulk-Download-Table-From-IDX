import glob
import os
import re
import numpy as np
import pandas as pd

DATA_FOLDER  = "./"
FILE_PATTERN = os.path.join(DATA_FOLDER, "idx_summary_*.xlsx")

def normalize_columns(cols: pd.Index) -> pd.Index:
    """
    Strip whitespace, lowercase, replace non-alphanumerics with underscores.
    Example: 'StockCode' → 'stockcode'
    """
    return (
        cols.str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
    )

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """
    Calculate the Relative Strength Index (RSI) over the specified period.
    """
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

# Map normalized column names to our target names
COLUMN_MAP = {
    "stockcode":  "code",
    "stock_code": "code",
    "date":        "date",
    "close":       "close",
    "volume":      "volume",
}

# We need exactly these four fields in each file
REQUIRED_COLS = {"code", "date", "close", "volume"}

def main():
    records = []

    for filepath in glob.glob(FILE_PATTERN):
        # 1) Read file
        df = pd.read_excel(filepath, engine="openpyxl")

        # 2) Normalize headers
        df.columns = normalize_columns(df.columns)

        # 3) Rename mapped columns
        rename_dict = {col: COLUMN_MAP[col] for col in df.columns if col in COLUMN_MAP}
        df.rename(columns=rename_dict, inplace=True)

        # 4) Check for required fields
        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            print(f"⚠ Skipping {os.path.basename(filepath)} — missing: {missing}")
            continue

        # 5) Parse dates and collect core fields
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        records.append(df[["code", "date", "close", "volume"]])

    if not records:
        raise RuntimeError(f"No valid files found under pattern: {FILE_PATTERN}")

    # 6) Combine and sort
    data = pd.concat(records, ignore_index=True)
    data.sort_values(["code", "date"], inplace=True)

    # 7) Compute indicators per stock code
    grp = data.groupby("code", group_keys=False)
    data["ma20"] = grp["close"] \
        .transform(lambda x: x.rolling(window=20, min_periods=1).mean())
    data["ma50"] = grp["close"] \
        .transform(lambda x: x.rolling(window=50, min_periods=1).mean())
    data["rsi14"] = grp["close"].transform(compute_rsi)

    # 8) Generate latest signals
    latest = data.groupby("code").tail(1).reset_index(drop=True)
    latest["signal"] = np.where(
        (latest["ma20"] > latest["ma50"]) & (latest["rsi14"] < 30),
        "Buy",
        np.where(
            (latest["ma20"] < latest["ma50"]) & (latest["rsi14"] > 70),
            "Sell",
            "Hold"
        )
    )

    # 9) Export summary to Excel
    summary_cols = ["code", "date", "close", "ma20", "ma50", "rsi14", "signal"]
    latest[summary_cols].sort_values(["signal", "code"]) \
                       .to_excel("summary_signals.xlsx", index=False)
    print("✅ summary_signals.xlsx created with", len(latest), "rows")

if __name__ == "__main__":
    main()