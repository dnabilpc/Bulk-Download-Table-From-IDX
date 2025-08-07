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

def is_weekday(date_obj):
    """
    Check if date is Monday-Friday (potential trading day)
    weekday() returns 0=Monday, 6=Sunday
    """
    return date_obj.weekday() < 5  # 0-4 are Mon-Fri

if __name__ == "__main__":
    days_needed = int(input("How many trading days back? "))
    
    print(f"Collecting {days_needed} trading days of data...")
    
    current_date = datetime.today()
    trading_days_collected = 0
    days_checked = 0
    max_days_to_check = days_needed * 3  # Safety limit
    
    while trading_days_collected < days_needed and days_checked < max_days_to_check:
        date_str = current_date.strftime("%Y%m%d")
        days_checked += 1
        
        # Skip weekends (Saturday/Sunday)
        if not is_weekday(current_date):
            print(f"⏩ {date_str}: Weekend, skipping")
            current_date -= timedelta(days=1)
            continue
        
        try:
            js = fetch_json_for_date(date_str)
            data = js.get("data") or []
            
            # If no data available, it's likely a holiday
            if not data:
                print(f"⏩ {date_str}: No data (holiday), skipping")
                current_date -= timedelta(days=1)
                continue
            
            # Save the trading data
            filename = f"idx_summary_{date_str}.xlsx"
            filepath = os.path.join(OUTPUT_DIR, filename)
            json_to_excel(data, filepath)
            
            trading_days_collected += 1
            print(f"✔ {date_str}: Saved ({trading_days_collected}/{days_needed})")
            
        except Exception as e:
            print(f"✘ {date_str}: Error - {e}")
        
        # Move to previous day
        current_date -= timedelta(days=1)
        time.sleep(1)
    
    print(f"\nCompleted! Collected {trading_days_collected} trading days.")
    if trading_days_collected < days_needed:
        print(f"Note: Only found {trading_days_collected} out of {days_needed} requested days.")
    print(f"Files saved in: {OUTPUT_DIR}")