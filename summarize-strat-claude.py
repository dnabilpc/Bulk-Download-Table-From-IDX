import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
import xlsxwriter
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
                volume_ratio = 0
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
                
                # Calculate prediction metrics
                predictions = self.calculate_predictions(signal, score, change_pct, daily_range, close_position, stock['Close'])
                
                signal_data.update({
                    'Signal': signal,
                    'Score': round(score, 2),
                    'Daily_Range_Pct': round(daily_range, 2),
                    'Close_Position': round(close_position, 2),
                    'Volume_Ratio': round(volume_ratio, 2) if avg_volume else 0,
                    'Reasons': '; '.join(reasons) if reasons else 'No significant signals',
                    'Predicted_Gain_Pct': predictions['predicted_gain'],
                    'Target_Price': predictions['target_price'],
                    'Risk_Level': predictions['risk_level'],
                    'Confidence_Score': predictions['confidence'],
                    'Time_Horizon': predictions['time_horizon']
                })
                
                signals.append(signal_data)
                
            except Exception as e:
                print(f"Error analyzing {stock.get('StockCode', 'Unknown')}: {e}")
                continue
        
        return pd.DataFrame(signals)
    
    def calculate_predictions(self, signal, score, change_pct, daily_range, close_position, current_price):
        """Calculate prediction metrics based on signal analysis"""
        predictions = {
            'predicted_gain': 0,
            'target_price': current_price,
            'risk_level': 'MEDIUM',
            'confidence': 50,
            'time_horizon': '3-5 days'
        }
        
        # Base prediction on signal strength and score
        base_gain = 0
        confidence_base = 50
        
        if signal == 'STRONG BUY':
            base_gain = 8 + (score * 0.5)  # 8-12% expected gain
            confidence_base = 80
            predictions['time_horizon'] = '2-4 days'
        elif signal == 'BUY':
            base_gain = 4 + (score * 0.8)  # 4-8% expected gain
            confidence_base = 65
            predictions['time_horizon'] = '3-5 days'
        elif signal == 'STRONG SELL':
            base_gain = -8 + (score * 0.5)  # -8 to -12% expected loss
            confidence_base = 75
            predictions['time_horizon'] = '2-4 days'
        elif signal == 'SELL':
            base_gain = -4 + (score * 0.8)  # -4 to -8% expected loss
            confidence_base = 60
            predictions['time_horizon'] = '3-5 days'
        else:  # HOLD
            base_gain = -1 + (score * 0.3)  # -1 to +1% neutral movement
            confidence_base = 45
            predictions['time_horizon'] = '5-7 days'
        
        # Adjust prediction based on momentum (recent change)
        momentum_factor = 1.0
        if abs(change_pct) >= 5:  # Strong momentum
            momentum_factor = 1.2 if change_pct > 0 else 1.3
            confidence_base += 10
        elif abs(change_pct) >= 2:  # Moderate momentum
            momentum_factor = 1.1 if change_pct > 0 else 1.15
            confidence_base += 5
        
        # Adjust based on volatility (daily range)
        volatility_factor = 1.0
        if daily_range >= 8:  # High volatility
            volatility_factor = 1.3
            confidence_base -= 10
            predictions['risk_level'] = 'HIGH'
        elif daily_range >= 5:  # Moderate volatility
            volatility_factor = 1.15
            predictions['risk_level'] = 'MEDIUM'
        elif daily_range < 2:  # Low volatility
            volatility_factor = 0.8
            predictions['risk_level'] = 'LOW'
            confidence_base += 5
        
        # Adjust based on close position (where stock closed in daily range)
        position_factor = 1.0
        if signal in ['BUY', 'STRONG BUY']:
            if close_position >= 0.8:  # Closed near high - bullish continuation
                position_factor = 1.1
                confidence_base += 5
            elif close_position <= 0.3:  # Closed near low - might reverse
                position_factor = 0.9
                confidence_base -= 5
        elif signal in ['SELL', 'STRONG SELL']:
            if close_position <= 0.2:  # Closed near low - bearish continuation
                position_factor = 1.1
                confidence_base += 5
            elif close_position >= 0.7:  # Closed near high - might reverse
                position_factor = 0.9
                confidence_base -= 5
        
        # Calculate final predicted gain
        final_gain = base_gain * momentum_factor * volatility_factor * position_factor
        
        # Apply market constraints (Indonesian stocks often limited by daily limits)
        final_gain = max(min(final_gain, 20), -20)  # Cap at +/-20%
        
        # Calculate target price
        target_price = current_price * (1 + final_gain / 100)
        
        # Calculate confidence score
        confidence = min(max(confidence_base, 20), 95)  # Between 20-95%
        
        # Determine risk level based on multiple factors
        risk_score = 0
        if abs(final_gain) >= 10:
            risk_score += 2
        if daily_range >= 8:
            risk_score += 2
        if confidence < 60:
            risk_score += 1
        if abs(change_pct) >= 5:
            risk_score += 1
        
        if risk_score >= 4:
            risk_level = 'VERY HIGH'
        elif risk_score >= 3:
            risk_level = 'HIGH'
        elif risk_score >= 2:
            risk_level = 'MEDIUM'
        else:
            risk_level = 'LOW'
        
        predictions.update({
            'predicted_gain': round(final_gain, 2),
            'target_price': round(target_price, 0),
            'risk_level': risk_level,
            'confidence': round(confidence, 0),
        })
        
        return predictions
    
    def save_formatted_excel(self, signals_df, output_file):
        """Save DataFrame to Excel with proper formatting"""
        try:
            # Create a workbook and worksheet
            workbook = xlsxwriter.Workbook(output_file)
            worksheet = workbook.add_worksheet('Trading Signals')
            
            # Define formats
            formats = {
                'header': workbook.add_format({
                    'bold': True,
                    'font_size': 12,
                    'bg_color': '#4472C4',
                    'font_color': 'white',
                    'align': 'center',
                    'valign': 'vcenter',
                    'border': 1,
                    'text_wrap': True
                }),
                'currency': workbook.add_format({
                    'num_format': '"Rp"#,##0',
                    'align': 'right',
                    'border': 1
                }),
                'percentage': workbook.add_format({
                    'num_format': '0.00%',
                    'align': 'center',
                    'border': 1
                }),
                'number': workbook.add_format({
                    'num_format': '#,##0',
                    'align': 'right',
                    'border': 1
                }),
                'decimal': workbook.add_format({
                    'num_format': '0.00',
                    'align': 'center',
                    'border': 1
                }),
                'text': workbook.add_format({
                    'align': 'left',
                    'border': 1,
                    'text_wrap': True
                }),
                'center_text': workbook.add_format({
                    'align': 'center',
                    'border': 1
                }),
                'strong_buy': workbook.add_format({
                    'bg_color': '#00B050',
                    'font_color': 'white',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                'buy': workbook.add_format({
                    'bg_color': '#92D050',
                    'font_color': 'black',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                'hold': workbook.add_format({
                    'bg_color': '#FFC000',
                    'font_color': 'black',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                'sell': workbook.add_format({
                    'bg_color': '#FF6600',
                    'font_color': 'white',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                'strong_sell': workbook.add_format({
                    'bg_color': '#C00000',
                    'font_color': 'white',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                'risk_low': workbook.add_format({
                    'bg_color': '#D5E8D4',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'risk_medium': workbook.add_format({
                    'bg_color': '#FFF2CC',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'risk_high': workbook.add_format({
                    'bg_color': '#F8CECC',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'risk_very_high': workbook.add_format({
                    'bg_color': '#C00000',
                    'font_color': 'white',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                'positive_percentage': workbook.add_format({
                    'num_format': '0.00%',
                    'align': 'center',
                    'border': 1,
                    'font_color': '#008000'
                }),
                'negative_percentage': workbook.add_format({
                    'num_format': '0.00%',
                    'align': 'center',
                    'border': 1,
                    'font_color': '#C00000'
                })
            }
            
            # Column definitions with formatting
            columns = [
                ('StockCode', 'Stock Code', 12, 'center_text'),
                ('StockName', 'Stock Name', 25, 'text'),
                ('Signal', 'Signal', 15, 'signal'),  # Special handling for signal colors
                ('Score', 'Score', 10, 'decimal'),
                ('Close', 'Close Price', 15, 'currency'),
                ('Change', 'Change', 12, 'currency'),
                ('Change_Pct', 'Change %', 12, 'percentage'),
                ('Predicted_Gain_Pct', 'Predicted Gain %', 16, 'percentage'),
                ('Target_Price', 'Target Price', 15, 'currency'),
                ('Confidence_Score', 'Confidence', 12, 'decimal'),
                ('Risk_Level', 'Risk Level', 12, 'risk_level'),  # Special handling for risk colors
                ('Time_Horizon', 'Time Frame', 12, 'center_text'),
                ('Volume', 'Volume', 15, 'number'),
                ('Value', 'Trading Value', 20, 'currency'),
                ('Daily_Range_Pct', 'Daily Range %', 15, 'percentage'),
                ('Close_Position', 'Close Position', 15, 'decimal'),
                ('Volume_Ratio', 'Volume Ratio', 15, 'decimal'),
                ('Reasons', 'Analysis Reasons', 40, 'text')
            ]
            
            # Write headers
            for col_idx, (col_name, header_text, width, format_type) in enumerate(columns):
                worksheet.write(0, col_idx, header_text, formats['header'])
                worksheet.set_column(col_idx, col_idx, width)
            
            # Write data rows
            for row_idx, (_, row_data) in enumerate(signals_df.iterrows(), start=1):
                for col_idx, (col_name, header_text, width, format_type) in enumerate(columns):
                    value = row_data[col_name]
                    
                    # Handle special formatting for Signal column
                    if col_name == 'Signal':
                        if value == 'STRONG BUY':
                            worksheet.write(row_idx, col_idx, value, formats['strong_buy'])
                        elif value == 'BUY':
                            worksheet.write(row_idx, col_idx, value, formats['buy'])
                        elif value == 'HOLD':
                            worksheet.write(row_idx, col_idx, value, formats['hold'])
                        elif value == 'SELL':
                            worksheet.write(row_idx, col_idx, value, formats['sell'])
                        elif value == 'STRONG SELL':
                            worksheet.write(row_idx, col_idx, value, formats['strong_sell'])
                    # Handle Risk Level formatting
                    elif col_name == 'Risk_Level':
                        if value == 'LOW':
                            worksheet.write(row_idx, col_idx, value, formats['risk_low'])
                        elif value == 'MEDIUM':
                            worksheet.write(row_idx, col_idx, value, formats['risk_medium'])
                        elif value == 'HIGH':
                            worksheet.write(row_idx, col_idx, value, formats['risk_high'])
                        elif value == 'VERY HIGH':
                            worksheet.write(row_idx, col_idx, value, formats['risk_very_high'])
                    # Handle Predicted Gain with color coding
                    elif col_name == 'Predicted_Gain_Pct':
                        if value >= 0:
                            worksheet.write(row_idx, col_idx, value / 100, formats['positive_percentage'])
                        else:
                            worksheet.write(row_idx, col_idx, value / 100, formats['negative_percentage'])
                    # Handle percentage conversion for Change_Pct and Daily_Range_Pct
                    elif col_name in ['Change_Pct', 'Daily_Range_Pct']:
                        worksheet.write(row_idx, col_idx, value / 100, formats[format_type])
                    # Handle Volume_Ratio display (show as N/A if 0)
                    elif col_name == 'Volume_Ratio':
                        if value == 0:
                            worksheet.write(row_idx, col_idx, 'N/A', formats['center_text'])
                        else:
                            worksheet.write(row_idx, col_idx, value, formats[format_type])
                    else:
                        worksheet.write(row_idx, col_idx, value, formats[format_type])
            
            # Add autofilter
            worksheet.autofilter(0, 0, len(signals_df), len(columns) - 1)
            
            # Freeze the header row
            worksheet.freeze_panes(1, 0)
            
            # Add summary information in a separate area
            summary_start_row = len(signals_df) + 3
            
            # Summary header
            worksheet.merge_range(summary_start_row, 0, summary_start_row, 2, 
                                'SIGNAL SUMMARY', formats['header'])
            
            # Signal counts
            signal_counts = signals_df['Signal'].value_counts()
            summary_row = summary_start_row + 2
            
            for signal, count in signal_counts.items():
                percentage = (count / len(signals_df) * 100)
                worksheet.write(summary_row, 0, signal, formats['center_text'])
                worksheet.write(summary_row, 1, count, formats['number'])
                worksheet.write(summary_row, 2, f"{percentage:.1f}%", formats['center_text'])
                summary_row += 1
            
            workbook.close()
            print(f"✅ Formatted Excel file saved: {output_file}")
            
        except Exception as e:
            print(f"❌ Error creating formatted Excel file: {e}")
            # Fallback to regular pandas Excel export
            export_df = signals_df[[
                'StockCode', 'StockName', 'Signal', 'Score', 'Close', 'Change', 'Change_Pct',
                'Predicted_Gain_Pct', 'Target_Price', 'Confidence_Score', 'Risk_Level', 'Time_Horizon',
                'Volume', 'Value', 'Daily_Range_Pct', 'Close_Position', 'Volume_Ratio', 'Reasons'
            ]].copy()
            export_df.to_excel(output_file, index=False)
            print(f"⚠️ Saved basic Excel file as fallback: {output_file}")
    
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
        
        # Save formatted results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.signals_folder, f"idx_trading_signals_{timestamp}.xlsx")
        
        # Prepare final dataframe for export
        export_df = signals_df[[
            'StockCode', 'StockName', 'Signal', 'Score', 'Close', 'Change', 'Change_Pct',
            'Predicted_Gain_Pct', 'Target_Price', 'Confidence_Score', 'Risk_Level', 'Time_Horizon',
            'Volume', 'Value', 'Daily_Range_Pct', 'Close_Position', 'Volume_Ratio', 'Reasons'
        ]].copy()
        
        # Save with formatting
        self.save_formatted_excel(export_df, output_file)
        
        # Display results
        print("\n" + "="*120)
        print(f"IDX DAILY TRADING SIGNALS - {latest_date.strftime('%Y-%m-%d')}")
        print("="*120)
        
        for signal_type in signal_order:
            stocks = signals_df[signals_df['Signal'] == signal_type]
            if len(stocks) > 0:
                print(f"\n🔸 {signal_type} ({len(stocks)} stocks):")
                for _, stock in stocks.head(10).iterrows():  # Show top 10 per category
                    predicted_gain = stock.get('Predicted_Gain_Pct', 0)
                    target_price = stock.get('Target_Price', stock['Close'])
                    confidence = stock.get('Confidence_Score', 0)
                    risk = stock.get('Risk_Level', 'N/A')
                    
                    print(f"  • {stock['StockCode']} ({stock['StockName'][:25]}): "
                          f"Score {stock['Score']}, Current: Rp{stock['Close']:,.0f}, "
                          f"Target: Rp{target_price:,.0f} ({predicted_gain:+.1f}%), "
                          f"Risk: {risk}, Confidence: {confidence}%")
                
                if len(stocks) > 10:
                    print(f"    ... and {len(stocks) - 10} more")
        
        # Summary statistics
        print(f"\n📊 Summary Statistics:")
        signal_counts = signals_df['Signal'].value_counts()
        for signal, count in signal_counts.items():
            percentage = (count / len(signals_df) * 100)
            print(f"   {signal}: {count} stocks ({percentage:.1f}%)")
        
        print(f"\n💾 Detailed formatted report saved: {output_file}")
        
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
    
    # Check if xlsxwriter is available
    try:
        import xlsxwriter
        print("✅ XlsxWriter library detected - Excel formatting will be applied")
    except ImportError:
        print("⚠️  XlsxWriter library not found. Install it with: pip install xlsxwriter")
        print("    Fallback: Basic Excel export will be used")
    
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
                predicted_gain = stock.get('Predicted_Gain_Pct', 0)
                target_price = stock.get('Target_Price', stock['Close'])
                confidence = stock.get('Confidence_Score', 0)
                risk = stock.get('Risk_Level', 'N/A')
                time_frame = stock.get('Time_Horizon', 'N/A')
                
                print(f"  🔥 {stock['StockCode']} - Current: Rp{stock['Close']:,.0f}, "
                      f"Target: Rp{target_price:,.0f} ({predicted_gain:+.1f}%)")
                print(f"     Risk: {risk}, Confidence: {confidence}%, Timeframe: {time_frame}")
        else:
            print("  No strong buy signals found today")
            
        print(f"\n📁 Check '{analyzer.signals_folder}' folder for detailed formatted Excel report")
    else:
        print("\n❌ Analysis failed. Check your data files in 'Stock Summary Folder'")