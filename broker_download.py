import os
import logging
from datetime import datetime
import cloudscraper
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(message)s")

# Output directory
OUTPUT_DIR = "Stock Summary Folder/broker summary"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Scraper
scraper = cloudscraper.create_scraper(
    browser={"browser": "chrome", "platform": "windows", "mobile": False},
    delay=5
)

COMMON_HEADERS = {
    "User-Agent":       "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
                        " AppleWebKit/537.36 (KHTML, like Gecko)"
                        " Chrome/115.0.0.0 Safari/537.36",
    "Accept":           "application/json, text/javascript, */*; q=0.01",
    "Referer":          "https://stockbit.com/",
    "X-Requested-With": "XMLHttpRequest",
    "Origin":           "https://stockbit.com",
    "authorization":    "Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6IjU3MDc0NjI3LTg4MWItNDQzZC04OTcyLTdmMmMzOTNlMzYyOSIsInR5cCI6IkpXVCJ9.eyJkYXRhIjp7InVzZSI6ImRuYWJpbHBjIiwiZW1hIjoiZG5hYmlscGNAZ21haWwuY29tIiwiZnVsIjoiRGlhbmRyYSBOYWJpbCIsInNlcyI6ImhkQ3luT2VIbDNzVDkzeHYiLCJkdmMiOiJlMzZlNmY2NjA1NTE2NmJkYTNkOTc0ZmQ2ZTY3NzgwMCIsInVpZCI6Mzk4ODU3MiwiY291IjoiU0cifSwiZXhwIjoxNzU1NDQ5ODU0LCJpYXQiOjE3NTUzNjM0NTQsImlzcyI6IlNUT0NLQklUIiwianRpIjoiY2Q3M2I2NmItZTA2MS00YzUyLWE5Y2YtMjlhZTBlMzlkMzM4IiwibmJmIjoxNzU1MzYzNDU0LCJ2ZXIiOiJ2MSJ9.mdhBi_gg0FR_2umT3Pa4zm5zv9NLxnvkWAsvi9zYYBJYkkVaCrysI7G_FNPzNvvlcY4_i2avl8mU0GultEVnFdC4ns5PoigCo7QasWWC63jugTD48Flapa1Sot7BDTwWrJuRJ6VBiboCpk9kU0qyINVUmbSZcdlzb7sedkuhKxtCrLofgCG-IjWns-aCTWgy-Ai6aG8XG3I0DgsmvcFyHsgFG8BRyXOmSlzNadldbtwn-4CP0mUjQYR1OD7XCNp40s96ZW5rinymHu0mSdE3pTX2VZWWqO951WPKJTCvxksW8gmWFZShODZAsaK6-ns0g-35RJfSZrcVeW9JdcWhIw"
}

def fetch_broker_summary(broker_code, from_date, to_date):
    url = f"https://exodus.stockbit.com/findata-view/marketdetectors/activity/{broker_code}/detail?"
    params = {
        "page": 1,
        "limit": 100,
        "from": from_date,
        "to": to_date
    }
    
    print(f"🔍 API Request: {url}")
    print(f"📅 Date range: {from_date} to {to_date}")
    print(f"🔗 Full URL: {url}from={from_date}&to={to_date}&page=1&limit=100")
    
    r = scraper.get(url, headers=COMMON_HEADERS, params=params, timeout=30)
    print(f"📊 API Response status: {r.status_code}")
    r.raise_for_status()
    js = r.json()
    
    print(f"📋 Response structure: {list(js.keys())}")
    
    if "data" in js:
        broker_data = js.get("data", {}).get("broker_summary", {})
        buy_count = len(broker_data.get("brokers_buy", []))
        sell_count = len(broker_data.get("brokers_sell", []))
        print(f"📈 Found {buy_count} buy records and {sell_count} sell records")
        return broker_data
    else:
        print("⚠️ No 'data' field in API response")
        print(f"Full response: {js}")
        return {}

def format_number(value):
    """Format large numbers into readable format"""
    try:
        num = float(value)
        if abs(num) >= 1e9:
            return f"{num/1e9:.2f}B"
        elif abs(num) >= 1e6:
            return f"{num/1e6:.2f}M"
        elif abs(num) >= 1e3:
            return f"{num/1e3:.2f}K"
        else:
            return f"{num:.2f}"
    except (ValueError, TypeError):
        return value

def format_date(date_str):
    """Format date from YYYYMMDD to readable format"""
    try:
        if len(date_str) == 8:
            return f"{date_str[6:8]}/{date_str[4:6]}/{date_str[:4]}"
    except:
        pass
    return date_str

def broker_summary_to_excel(broker_summary, filepath):
    """Enhanced function to create a comprehensive Excel file"""
    
    # Create a Pandas Excel writer object with formatting
    with pd.ExcelWriter(filepath, engine='xlsxwriter') as writer:
        workbook = writer.book
        
        # Define formats
        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,
            'valign': 'top',
            'fg_color': '#D7E4BC',
            'border': 1
        })
        
        number_format = workbook.add_format({
            'num_format': '#,##0.00',
            'border': 1
        })
        
        large_number_format = workbook.add_format({
            'num_format': '#,##0',
            'border': 1
        })
        
        text_format = workbook.add_format({
            'border': 1
        })
        
        # Process Buy Data
        buy_data = broker_summary.get("brokers_buy", [])
        if buy_data:
            buy_df = pd.DataFrame(buy_data)
            
            # Rename columns for better readability
            buy_column_mapping = {
                'netbs_broker_code': 'Broker Code',
                'netbs_stock_code': 'Stock Code',
                'netbs_date': 'Date',
                'netbs_buy_avg_price': 'Avg Buy Price',
                'type': 'Type',
                'blot': 'Buy Lot',
                'blotv': 'Buy Lot Value',
                'bval': 'Buy Value',
                'bvalv': 'Buy Value Volume'
            }
            
            # Select and rename columns
            available_cols = [col for col in buy_column_mapping.keys() if col in buy_df.columns]
            buy_df_clean = buy_df[available_cols].copy()
            buy_df_clean = buy_df_clean.rename(columns=buy_column_mapping)
            
            # Convert numeric columns to proper numeric types
            numeric_columns = ['Avg Buy Price', 'Buy Lot', 'Buy Lot Value', 'Buy Value', 'Buy Value Volume']
            for col in numeric_columns:
                if col in buy_df_clean.columns:
                    buy_df_clean[col] = pd.to_numeric(buy_df_clean[col], errors='coerce')
            
            # Format specific columns
            if 'Date' in buy_df_clean.columns:
                buy_df_clean['Date'] = buy_df_clean['Date'].apply(format_date)
            
            # Write to Excel
            buy_df_clean.to_excel(writer, sheet_name='Buy Transactions', index=False, startrow=1)
            
            # Format the Buy sheet
            buy_worksheet = writer.sheets['Buy Transactions']
            buy_worksheet.write('A1', 'BROKER BUY TRANSACTIONS', header_format)
            
            # Apply formatting to headers
            for col_num, value in enumerate(buy_df_clean.columns.values):
                buy_worksheet.write(1, col_num, value, header_format)
            
            # Apply number formatting to numeric columns
            for col_num, col_name in enumerate(buy_df_clean.columns):
                if col_name in ['Buy Lot Value', 'Buy Value', 'Buy Value Volume']:
                    buy_worksheet.set_column(col_num, col_num, 15, large_number_format)
                elif col_name == 'Avg Buy Price':
                    buy_worksheet.set_column(col_num, col_num, 12, number_format)
                else:
                    max_len = max(buy_df_clean[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    buy_worksheet.set_column(col_num, col_num, min(max_len, 20), text_format)
        
        # Process Sell Data
        sell_data = broker_summary.get("brokers_sell", [])
        if sell_data:
            sell_df = pd.DataFrame(sell_data)
            
            # Rename columns for better readability
            sell_column_mapping = {
                'netbs_broker_code': 'Broker Code',
                'netbs_stock_code': 'Stock Code',
                'netbs_date': 'Date',
                'netbs_sell_avg_price': 'Avg Sell Price',
                'type': 'Type',
                'slot': 'Sell Lot',
                'slotv': 'Sell Lot Value',
                'sval': 'Sell Value',
                'svalv': 'Sell Value Volume'
            }
            
            # Select and rename columns
            available_cols = [col for col in sell_column_mapping.keys() if col in sell_df.columns]
            sell_df_clean = sell_df[available_cols].copy()
            sell_df_clean = sell_df_clean.rename(columns=sell_column_mapping)
            
            # Convert numeric columns to proper numeric types
            numeric_columns = ['Avg Sell Price', 'Sell Lot', 'Sell Lot Value', 'Sell Value', 'Sell Value Volume']
            for col in numeric_columns:
                if col in sell_df_clean.columns:
                    sell_df_clean[col] = pd.to_numeric(sell_df_clean[col], errors='coerce')
            
            # Format specific columns
            if 'Date' in sell_df_clean.columns:
                sell_df_clean['Date'] = sell_df_clean['Date'].apply(format_date)
            
            # Write to Excel
            sell_df_clean.to_excel(writer, sheet_name='Sell Transactions', index=False, startrow=1)
            
            # Format the Sell sheet
            sell_worksheet = writer.sheets['Sell Transactions']
            sell_worksheet.write('A1', 'BROKER SELL TRANSACTIONS', header_format)
            
            # Apply formatting to headers
            for col_num, value in enumerate(sell_df_clean.columns.values):
                sell_worksheet.write(1, col_num, value, header_format)
            
            # Apply number formatting to numeric columns
            for col_num, col_name in enumerate(sell_df_clean.columns):
                if col_name in ['Sell Lot Value', 'Sell Value', 'Sell Value Volume']:
                    sell_worksheet.set_column(col_num, col_num, 15, large_number_format)
                elif col_name == 'Avg Sell Price':
                    sell_worksheet.set_column(col_num, col_num, 12, number_format)
                else:
                    max_len = max(sell_df_clean[col_name].astype(str).map(len).max(), len(col_name)) + 2
                    sell_worksheet.set_column(col_num, col_num, min(max_len, 20), text_format)
        
        # Create Summary Sheet
        summary_data = []
        
        # Add buy summary
        if buy_data:
            buy_count = len(buy_data)
            total_buy_lots = sum(pd.to_numeric(item.get('blot', 0), errors='coerce') for item in buy_data)
            total_buy_value = sum(pd.to_numeric(item.get('bval', 0), errors='coerce') for item in buy_data)
            
            summary_data.extend([
                ['Transaction Type', 'Count', 'Total Lots', 'Total Value'],
                ['Buy Transactions', buy_count, total_buy_lots, total_buy_value]
            ])
        
        # Add sell summary
        if sell_data:
            sell_count = len(sell_data)
            total_sell_lots = sum(pd.to_numeric(item.get('slot', 0), errors='coerce') for item in sell_data)
            total_sell_value = sum(pd.to_numeric(item.get('sval', 0), errors='coerce') for item in sell_data)
            
            if not summary_data:
                summary_data.append(['Transaction Type', 'Count', 'Total Lots', 'Total Value'])
            
            summary_data.append(['Sell Transactions', sell_count, total_sell_lots, total_sell_value])
        
        if summary_data:
            summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
            
            # Convert numeric columns
            summary_df['Total Lots'] = pd.to_numeric(summary_df['Total Lots'], errors='coerce')
            summary_df['Total Value'] = pd.to_numeric(summary_df['Total Value'], errors='coerce')
            
            summary_df.to_excel(writer, sheet_name='Summary', index=False, startrow=1)
            
            # Format the Summary sheet
            summary_worksheet = writer.sheets['Summary']
            summary_worksheet.write('A1', 'BROKER TRANSACTIONS SUMMARY', header_format)
            
            # Apply formatting to headers
            for col_num, value in enumerate(summary_data[0]):
                summary_worksheet.write(1, col_num, value, header_format)
            
            # Apply formatting to columns
            summary_worksheet.set_column(0, 0, 20, text_format)  # Transaction Type
            summary_worksheet.set_column(1, 1, 10, text_format)  # Count
            summary_worksheet.set_column(2, 2, 15, large_number_format)  # Total Lots
            summary_worksheet.set_column(3, 3, 20, large_number_format)  # Total Value
    
    print(f"✔ Enhanced Excel file created with multiple sheets: {filepath}")

if __name__ == "__main__":
    broker_code = input("Enter broker code (e.g., 'CC'): ").strip().upper()
    from_date = input("Enter FROM date (YYYY-MM-DD): ").strip()
    to_date = input("Enter TO date (YYYY-MM-DD): ").strip()
    
    try:
        datetime.strptime(from_date, "%Y-%m-%d")
        datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        print("❌ Invalid date format. Please use YYYY-MM-DD.")
        exit()
    
    print(f"Fetching broker_summary from {from_date} to {to_date}...")
    
    try:
        broker_summary = fetch_broker_summary(broker_code, from_date, to_date)
        
        if not broker_summary:
            print("⚠ No broker_summary data found for this date range.")
        else:
            filename = f"{broker_code}_{from_date}_to_{to_date}.xlsx"
            filepath = os.path.join(OUTPUT_DIR, filename)
            
            broker_summary_to_excel(broker_summary, filepath)
            
            # Print summary of what was processed
            buy_count = len(broker_summary.get("brokers_buy", []))
            sell_count = len(broker_summary.get("brokers_sell", []))
            print(f"📊 Processed {buy_count} buy transactions and {sell_count} sell transactions")
            print(f"✔ Data saved to {filepath}")
            
    except Exception as e:
        print(f"❌ Error fetching data: {e}")