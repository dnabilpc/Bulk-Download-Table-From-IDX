import os
import time
import logging
from datetime import datetime, timedelta

import cloudscraper
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(message)s")

# 1. Ensure the output directory exists
OUTPUT_DIR = "Stock Summary Folder"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 2. Create your scraper
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False},
    delay=5
)

COMMON_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                        " AppleWebKit/537.36 (KHTML, like Gecko)"
                        " Chrome/115.0.0.0 Safari/537.36",
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Referer":          "https://www.idx.co.id/primary/TradingSummary",
    "X-Requested-With": "XMLHttpRequest",
    "Origin":           "https://www.idx.co.id"
}

def fetch_json_for_date(date_str):
    url    = "https://www.idx.co.id/primary/TradingSummary/GetStockSummary"
    params = {"length": 9999, "start": 0, "date": date_str}
    r      = scraper.get(url, headers=COMMON_HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def json_to_excel(data, filepath):
    df = pd.DataFrame(data)
    df.to_excel(filepath, index=False)

if __name__ == "__main__":
    days_back = int(input("How many days back? "))
    today     = datetime.today()

    for i in range(days_back):
        d        = today - timedelta(days=i)
        date_str = d.strftime("%Y%m%d")

        try:
            js   = fetch_json_for_date(date_str)
            data = js.get("data") or []

            # skip if no data
            if not data:
                print(f"✘ {date_str}: no data available, skipping export")
                continue

            # build the output path in the subfolder
            filename = f"idx_summary_{date_str}.xlsx"
            filepath = os.path.join(OUTPUT_DIR, filename)

            json_to_excel(data, filepath)
            print(f"✔ {date_str} saved → {filepath}")

        except Exception as e:
            print(f"✘ {date_str} error:", e)

        time.sleep(1)