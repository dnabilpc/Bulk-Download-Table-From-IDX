import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class IDXTradingSignalAnalyzer:
    def __init__(self, data_folder="Stock Summary Folder"):
        self.data_folder = data_folder
        self.signals_folder = "Trading Signals"
        os.makedirs(self.signals_folder, exist_ok=True)
    
    def load_daily_summary(self, filename):
        """Load daily trading summary from Excel file"""
        try:
            filepath = os.path.join(self.data_folder, filename)
            df = pd.read_excel(filepath)
            
            # Clean column names (remove spaces)
            df.columns = df.columns.str.strip()
            
            # Convert numeric columns
            numeric_columns = ['Previous', 'OpenPrice', 'High', 'Low', 'Close', 'Change', 
                             'Volume', 'Value', 'Frequency', 'OfferVolume', 'BidVolume']
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # Extract date from filename (assuming format: idx_summary_YYYYMMDD.xlsx)
            date_str = filename.split('_')[-1].replace('.xlsx', '')
            try:
                df['Date'] = pd.to_datetime(date_str, format='%Y%m%d')
            except:
                df['Date'] = pd.to_datetime('today').normalize()
            
            # Remove rows with invalid data
            df = df.dropna(subset=['Close', 'Volume'])
            df = df[df['Close'] > 0]
            df = df[df['Volume'] > 0]
            
            return df
        except Exception as e:
            print(f"Error loading {filename}: {e}")
            return None
    
    def load_multiple_days(self, days_to_load=5):
        """Load multiple days of data and combine"""
        excel_files = sorted([f for f in os.listdir(self.data_folder) if f.endswith('.xlsx')])
        
        if not excel_files:
            print(f"No Excel files found in {self.data_folder}")
            return None
        
        # Get the most recent files
        recent_files = excel_files[-days_to_load:]
        
        all_data = []
        for filename in recent_files:
            daily_data = self.load_daily_summary(filename)
            if daily_data is not None:
                all_data.append(daily_data)
                print(f"Loaded {filename}: {len(daily_data)} stocks")
        
        if not all_data:
            return None
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    
    def analyze_daily_signals(self, df_today, df_historical=None):
        """Analyze today's trading data for short-term signals"""
        signals = []
        
        for _, stock in df_today.iterrows():
            try:
                signal_data = {
                    'StockCode': stock['StockCode'],
                    'StockName': stock['StockName'],
                    'Close': stock['Close'],
                    'Change': stock['Change'],
                    'Change_Pct': (stock['Change'] / stock['Previous'] * 100) if stock['Previous'] > 0 else 0,
                    'Volume': stock['Volume'],
                    'Value': stock['Value'],
                    'High': stock['High'],
                    'Low': stock['Low'],
                    'Date': stock['Date']
                }
                
                # Calculate daily trading metrics
                score = 0
                reasons = []
                
                # 1. Price Movement Analysis (40% weight)
                change_pct = signal_data['Change_Pct']
                if change_pct >= 5:  # Strong positive move
                    score += 4
                    reasons.append(f"Strong gain +{change_pct:.1f}%")
                elif change_pct >= 2:
                    score += 2
                    reasons.append(f"Good gain +{change_pct:.1f}%")
                elif change_pct >= 0.5:
                    score += 1
                    reasons.append(f"Modest gain +{change_pct:.1f}%")
                elif change_pct <= -5:  # Strong negative move
                    score -= 4
                    reasons.append(f"Heavy loss {change_pct:.1f}%")
                elif change_pct <= -2:
                    score -= 2
                    reasons.append(f"Significant loss {change_pct:.1f}%")
                elif change_pct <= -0.5:
                    score -= 1
                    reasons.append(f"Small loss {change_pct:.1f}%")
                
                # 2. Volume Analysis (30% weight)
                # Calculate average volume if we have historical data
                avg_volume = None
                if df_historical is not None:
                    hist_stock = df_historical[df_historical['StockCode'] == stock['StockCode']]
                    if len(hist_stock) > 1:
                        avg_volume = hist_stock['Volume'].mean()
                        volume_ratio = stock['Volume'] / avg_volume
                        
                        if volume_ratio >= 2:  # Very high volume
                            score += 2
                            reasons.append(f"Very high volume ({volume_ratio:.1f}x avg)")
                        elif volume_ratio >= 1.5:  # High volume
                            score += 1
                            reasons.append(f"High volume ({volume_ratio:.1f}x avg)")
                        elif volume_ratio <= 0.3:  # Very low volume
                            score -= 1
                            reasons.append(f"Very low volume ({volume_ratio:.1f}x avg)")
                
                # 3. Daily Range Analysis (20% weight)
                daily_range = ((stock['High'] - stock['Low']) / stock['Previous'] * 100) if stock['Previous'] > 0 else 0
                close_position = ((stock['Close'] - stock['Low']) / (stock['High'] - stock['Low'])) if (stock['High'] - stock['Low']) > 0 else 0.5
                
                if daily_range >= 8:  # High volatility
                    if close_position >= 0.8:  # Closed near high
                        score += 2
                        reasons.append(f"High volatility, closed near high")
                    elif close_position <= 0.2:  # Closed near low
                        score -= 2
                        reasons.append(f"High volatility, closed near low")
                elif daily_range >= 5:  # Moderate volatility
                    if close_position >= 0.7:
                        score += 1
                        reasons.append(f"Moderate volatility, closed strong")
                    elif close_position <= 0.3:
                        score -= 1
                        reasons.append(f"Moderate volatility, closed weak")
                
                # 4. Value Analysis (10% weight)
                if stock['Value'] >= 10_000_000_000:  # 10B+ rupiah
                    score += 1
                    reasons.append("High trading value")
                elif stock['Value'] >= 1_000_000_000:  # 1B+ rupiah
                    score += 0.5
                    reasons.append("Good trading value")
                
                # Determine signal
                if score >= 4:
                    signal = 'STRONG BUY'
                elif score >= 2:
                    signal = 'BUY'
                elif score <= -4:
                    signal = 'STRONG SELL'
                elif score <= -2:
                    signal = 'SELL'
                else:
                    signal = 'HOLD'
                
                signal_data.update({
                    'Signal': signal,
                    'Score': round(score, 2),
                    'Daily_Range_Pct': round(daily_range, 2),
                    'Close_Position': round(close_position, 2),
                    'Volume_Ratio': round(volume_ratio, 2) if avg_volume else 'N/A',
                    'Reasons': '; '.join(reasons) if reasons else 'No significant signals'
                })
                
                signals.append(signal_data)
                
            except Exception as e:
                print(f"Error analyzing {stock.get('StockCode', 'Unknown')}: {e}")
                continue
        
        return pd.DataFrame(signals)
    
    def run_analysis(self, days_to_analyze=5):
        """Run the complete analysis"""
        print(f"📊 Loading last {days_to_analyze} days of IDX trading data...")
        
        # Load historical data for comparison
        historical_data = self.load_multiple_days(days_to_analyze)
        if historical_data is None:
            print("❌ Could not load any data files")
            return None
        
        # Get the most recent day's data
        latest_date = historical_data['Date'].max()
        today_data = historical_data[historical_data['Date'] == latest_date]
        
        print(f"📈 Analyzing {len(today_data)} stocks from {latest_date.strftime('%Y-%m-%d')}")
        
        # Generate signals
        signals_df = self.analyze_daily_signals(today_data, historical_data)
        
        if signals_df.empty:
            print("❌ No signals generated")
            return None
        
        # Sort by signal strength and score
        signal_order = ['STRONG BUY', 'BUY', 'HOLD', 'SELL', 'STRONG SELL']
        signals_df['Signal_Order'] = signals_df['Signal'].map({sig: i for i, sig in enumerate(signal_order)})
        signals_df = signals_df.sort_values(['Signal_Order', 'Score'], ascending=[True, False])
        
        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.signals_folder, f"idx_trading_signals_{timestamp}.xlsx")
        
        # Prepare final dataframe for export
        export_df = signals_df[[
            'StockCode', 'StockName', 'Signal', 'Score', 'Close', 'Change', 'Change_Pct',
            'Volume', 'Value', 'Daily_Range_Pct', 'Close_Position', 'Volume_Ratio', 'Reasons'
        ]].copy()
        
        export_df.to_excel(output_file, index=False)
        
        # Display results
        print("\n" + "="*120)
        print(f"IDX DAILY TRADING SIGNALS - {latest_date.strftime('%Y-%m-%d')}")
        print("="*120)
        
        for signal_type in signal_order:
            stocks = signals_df[signals_df['Signal'] == signal_type]
            if len(stocks) > 0:
                print(f"\n🔸 {signal_type} ({len(stocks)} stocks):")
                for _, stock in stocks.head(10).iterrows():  # Show top 10 per category
                    print(f"  • {stock['StockCode']} ({stock['StockName'][:30]}): "
                          f"Score {stock['Score']}, Price {stock['Close']:,}, "
                          f"Change {stock['Change_Pct']:+.1f}%")
                
                if len(stocks) > 10:
                    print(f"    ... and {len(stocks) - 10} more")
        
        # Summary statistics
        print(f"\n📊 Summary Statistics:")
        signal_counts = signals_df['Signal'].value_counts()
        for signal, count in signal_counts.items():
            percentage = (count / len(signals_df) * 100)
            print(f"   {signal}: {count} stocks ({percentage:.1f}%)")
        
        print(f"\n💾 Detailed report saved: {output_file}")
        
        return signals_df
    
    def get_top_picks(self, signals_df, signal_type='BUY', top_n=10):
        """Get top stock picks for a specific signal type"""
        if signals_df is None:
            return None
        
        filtered = signals_df[signals_df['Signal'].isin([signal_type, f'STRONG {signal_type}'])]
        return filtered.head(top_n)

if __name__ == "__main__":
    print("🚀 IDX Daily Trading Signal Analyzer")
    print("="*50)
    
    analyzer = IDXTradingSignalAnalyzer()
    
    # Ask user for number of days to analyze
    try:
        days = int(input("How many days of data to analyze for comparison? (default 5): ") or "5")
    except ValueError:
        days = 5
    
    results = analyzer.run_analysis(days_to_analyze=days)
    
    if results is not None:
        print(f"\n✅ Analysis complete! Analyzed {len(results)} stocks.")
        
        # Show top picks
        print("\n🏆 TOP BUY RECOMMENDATIONS:")
        top_buys = analyzer.get_top_picks(results, 'BUY', 5)
        if top_buys is not None and len(top_buys) > 0:
            for _, stock in top_buys.iterrows():
                print(f"  🔥 {stock['StockCode']} - Score: {stock['Score']}, "
                      f"Change: {stock['Change_Pct']:+.1f}%, Price: {stock['Close']:,}")
        else:
            print("  No strong buy signals found today")
            
        print(f"\n📁 Check '{analyzer.signals_folder}' folder for detailed Excel report")
    else:
        print("\n❌ Analysis failed. Check your data files in 'Stock Summary Folder'")