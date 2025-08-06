import glob
import os
from datetime import datetime
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages

DATA_FOLDER  = "./Stock Summary Folder"
FILE_PATTERN = os.path.join(DATA_FOLDER, "idx_summary_*.xlsx")

def normalize_columns(cols: pd.Index) -> pd.Index:
    """Strip whitespace, lowercase, replace non-alphanumerics with underscores."""
    return (
        cols.str.strip()
            .str.lower()
            .str.replace(r"[^\w]+", "_", regex=True)
    )

def compute_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    """Calculate the Relative Strength Index (RSI)."""
    delta    = series.diff()
    gain     = delta.clip(lower=0)
    loss     = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()
    rs       = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def compute_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Calculate Average True Range (ATR)."""
    high  = df["high"]
    low   = df["low"]
    close = df["close"]
    tr1 = high - low
    tr2 = (high - close.shift()).abs()
    tr3 = (low  - close.shift()).abs()
    tr  = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(window=period, min_periods=1).mean()

def compute_cagr(returns: pd.Series, days_per_trade=5, trading_days_per_year=252) -> float:
    """Compute CAGR from trade returns."""
    if returns.empty:
        return 0.0
    cumulative_return = (1 + returns/100).prod()
    total_days = len(returns) * days_per_trade
    years = total_days / trading_days_per_year
    return (cumulative_return ** (1/years) - 1) * 100 if years > 0 else 0.0

def compute_max_drawdown(returns: pd.Series) -> float:
    """Compute Max Drawdown from trade returns."""
    if returns.empty:
        return 0.0
    equity_curve = (1 + returns/100).cumprod()
    rolling_max = equity_curve.cummax()
    drawdown = equity_curve / rolling_max - 1
    return drawdown.min() * 100  # Negative percentage

def plot_equity_and_drawdown(code: str, returns: pd.Series, pdf: PdfPages):
    """Plot equity curve and drawdown for a single stock code and save to PDF."""
    if returns.empty:
        return

    equity_curve = (1 + returns/100).cumprod()
    rolling_max = equity_curve.cummax()
    drawdown = equity_curve / rolling_max - 1

    plt.figure(figsize=(10,5))
    plt.plot(equity_curve.index, equity_curve, label="Equity Curve")
    plt.fill_between(drawdown.index, 1+drawdown, 1, color='red', alpha=0.3, label="Drawdown")
    
    plt.title(f"Equity Curve & Drawdown: {code}")
    plt.xlabel("Trade Number")
    plt.ylabel("Growth (Starting at 1.0)")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    pdf.savefig()
    plt.close()
    print(f"📈 Added {code} to PDF report")

COLUMN_MAP = {
    "stockcode":  "code",
    "stock_code": "code",
    "date":        "date",
    "high":        "high",
    "low":         "low",
    "close":       "close",
    "volume":      "volume",
}

REQUIRED_COLS = {"code", "date", "high", "low", "close", "volume"}

def main():
    records = []

    # 1) Read all files and normalize
    for fp in glob.glob(FILE_PATTERN):
        df = pd.read_excel(fp, engine="openpyxl")
        df.columns = normalize_columns(df.columns)
        rename_dict = {c: COLUMN_MAP[c] for c in df.columns if c in COLUMN_MAP}
        df.rename(columns=rename_dict, inplace=True)

        missing = REQUIRED_COLS - set(df.columns)
        if missing:
            print(f"⚠ Skipping {os.path.basename(fp)} — missing: {missing}")
            continue

        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        records.append(df[["code","date","high","low","close","volume"]])

    if not records:
        raise RuntimeError(f"No valid files found under pattern: {FILE_PATTERN}")

    # 2) Combine & sort
    data = pd.concat(records, ignore_index=True)
    data.sort_values(["code", "date"], inplace=True)

    # 3) Compute indicators per stock code
    grp = data.groupby("code", group_keys=False)
    data["ma20"]   = grp["close"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    data["ma50"]   = grp["close"].transform(lambda x: x.rolling(50, min_periods=1).mean())
    data["rsi14"]  = grp["close"].transform(compute_rsi)
    data["vol20"]  = grp["volume"].transform(lambda x: x.rolling(20, min_periods=1).mean())
    data["atr14"]  = grp.apply(compute_atr, include_groups=False).reset_index(level=0, drop=True)

    # 4) Generate Buy/Sell/Hold signals
    data["signal"] = np.where(
        (data["ma20"] > data["ma50"]) & (data["rsi14"] < 30) & (data["volume"] > data["vol20"]),
        "Buy",
        np.where(
            (data["ma20"] < data["ma50"]) & (data["rsi14"] > 70) & (data["volume"] > data["vol20"]),
            "Sell",
            "Hold"
        )
    )

    # 5) Compute stop-loss & take-profit for the latest record
    latest = data.groupby("code").tail(1).reset_index(drop=True)
    latest["stop_loss"]   = latest["close"] - 2 * latest["atr14"]
    latest["take_profit"] = latest["close"] + 3 * latest["atr14"]

    # 6) 5-day backtest for Buy signals
    def get_returns(df: pd.DataFrame, hold_days: int = 5) -> pd.Series:
        df = df.reset_index(drop=True)
        buys = df[df["signal"] == "Buy"]

        if buys.empty:
            # Return an empty Series with float dtype to avoid FutureWarning
            return pd.Series(dtype=float, name="pct_return")

        rets = []
        for idx, row in buys.iterrows():
            exit_idx = idx + hold_days
            if exit_idx < len(df):
                ret = (df.loc[exit_idx, "close"] / row["close"] - 1) * 100
                rets.append(ret)

        return pd.Series(rets, name="pct_return")

    backtest = (
        data.groupby("code", group_keys=True)
            .apply(get_returns, include_groups=False)
            .reset_index(level=0, drop=False)
    )
    backtest = backtest.dropna(subset=["pct_return"])

    # 7) Backtest summary with Win Rate, Profit Factor, CAGR, Max Drawdown
    summary_list = []

    with PdfPages("backtest_report.pdf") as pdf:
        for code, group in backtest.groupby("code"):
            returns = group["pct_return"]
            if returns.empty:
                continue

            trades = len(returns)
            avg_return = returns.mean()
            win_rate = (returns > 0).mean() * 100
            profit_factor = returns[returns > 0].sum() / abs(returns[returns < 0].sum()) if (returns < 0).sum() != 0 else np.inf
            cagr = compute_cagr(returns)
            max_dd = compute_max_drawdown(returns)

            # Save plot to PDF
            plot_equity_and_drawdown(code, returns, pdf)

            summary_list.append({
                "code": code,
                "trades": trades,
                "avg_return": avg_return,
                "win_rate": win_rate,
                "profit_factor": profit_factor,
                "CAGR": cagr,
                "Max_Drawdown": max_dd
            })

    summary = pd.DataFrame(summary_list)

    # 8) Export results
    signal_cols = ["code", "date", "close", "ma20", "ma50", "rsi14", "atr14",
                   "stop_loss", "take_profit", "signal"]
    latest[signal_cols].sort_values(["signal", "code"]) \
                       .to_excel("summary_signals.xlsx", index=False)

    summary.to_excel("backtest_summary.xlsx", index=False)

    print(f"✅ summary_signals.xlsx ({len(latest)} rows), backtest_summary.xlsx ({len(summary)} rows), and backtest_report.pdf created")


if __name__ == "__main__":
    main()
