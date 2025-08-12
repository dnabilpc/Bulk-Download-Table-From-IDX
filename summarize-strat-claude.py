import pandas as pd
import numpy as np
import os
from datetime import datetime
import warnings
import xlsxwriter
warnings.filterwarnings('ignore')

class EnhancedIDXTradingSignalAnalyzer:
    def __init__(self, data_folder="Stock Summary Folder"):
        self.data_folder = data_folder
        self.signals_folder = "Trading Signals"
        self.idx_daily_limit = 0.25  # 25% daily limit for IDX
        self.profit_taking_threshold = 0.15  # 15% gain threshold for profit-taking risk
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
    
    def calculate_price_position(self, df_historical, current_price, stock_code):
        """Calculate where current price sits relative to recent highs/lows"""
        try:
            stock_hist = df_historical[df_historical['StockCode'] == stock_code].copy()
            if len(stock_hist) < 10:  # Need sufficient history
                return {
                    'position_pct': 50, 
                    'near_high': False, 
                    'near_low': False, 
                    'risk_level': 'UNKNOWN',
                    'period_high': current_price,
                    'period_low': current_price
                }
            
            # Calculate 20-day high/low
            period_high = stock_hist['High'].max()
            period_low = stock_hist['Low'].min()
            
            if period_high == period_low:
                return {
                    'position_pct': 50, 
                    'near_high': False, 
                    'near_low': False, 
                    'risk_level': 'LOW',
                    'period_high': period_high,
                    'period_low': period_low
                }
            
            # Position in range (0-100%)
            position_pct = ((current_price - period_low) / (period_high - period_low)) * 100
            
            # Risk assessment
            near_high = position_pct >= 85  # Within 15% of recent high
            near_low = position_pct <= 15   # Within 15% of recent low
            
            # Profit-taking risk level
            if position_pct >= 90:
                risk_level = 'VERY_HIGH'  # Extreme profit-taking risk
            elif position_pct >= 80:
                risk_level = 'HIGH'       # High profit-taking risk
            elif position_pct <= 20:
                risk_level = 'LOW'        # Low risk, potential bounce
            else:
                risk_level = 'MEDIUM'     # Moderate risk
                
            return {
                'position_pct': round(position_pct, 1),
                'near_high': near_high,
                'near_low': near_low,
                'risk_level': risk_level,
                'period_high': period_high,
                'period_low': period_low
            }
            
        except Exception:
            return {
                'position_pct': 50, 
                'near_high': False, 
                'near_low': False, 
                'risk_level': 'UNKNOWN',
                'period_high': current_price,
                'period_low': current_price
            }
    
    def detect_profit_taking_signals(self, stock, historical_data, volume_ratio):
        """Detect if stock shows profit-taking behavior"""
        warnings = []
        risk_score = 0
        
        try:
            change_pct = (stock['Change'] / stock['Previous'] * 100) if stock['Previous'] > 0 else 0
            close_position = ((stock['Close'] - stock['Low']) / (stock['High'] - stock['Low'])) if (stock['High'] - stock['Low']) > 0 else 0.5
            
            # Get price position
            price_position = self.calculate_price_position(historical_data, stock['Close'], stock['StockCode'])
            
            # 1. Check if price is near recent highs with high volume
            if price_position['near_high'] and volume_ratio > 1.5:
                warnings.append("⚠️ High volume near recent highs - potential distribution")
                risk_score += 3
            
            # 2. Check for profit-taking after recent gains
            if historical_data is not None:
                recent_hist = historical_data[
                    (historical_data['StockCode'] == stock['StockCode']) & 
                    (historical_data['Date'] >= historical_data['Date'].max() - pd.Timedelta(days=5))
                ].sort_values('Date')
                
                if len(recent_hist) >= 3:
                    # Check for consecutive gains followed by high volume
                    recent_changes = recent_hist['Change'].iloc[-3:].sum()
                    if len(recent_hist) > 0:
                        recent_change_pct = (recent_changes / recent_hist['Previous'].iloc[-3]) * 100
                        
                        if recent_change_pct >= 10 and volume_ratio > 2:
                            warnings.append(f"⚠️ {recent_change_pct:.1f}% gain in 3 days + very high volume")
                            risk_score += 2
            
            # 3. Check for exhaustion patterns
            if change_pct >= 5 and close_position <= 0.4:
                warnings.append("⚠️ Strong opening but weak close - possible exhaustion")
                risk_score += 2
            
            # 4. Check for gap up with high volume (often followed by profit-taking)
            if change_pct >= 8 and volume_ratio > 2:
                warnings.append("⚠️ Large gap + high volume - watch for profit-taking")
                risk_score += 2
                
            # 5. Check proximity to daily limit (Indonesian market characteristic)
            daily_limit_up = self.idx_daily_limit * 100  # 25%
            if change_pct >= daily_limit_up * 0.8:  # Within 80% of daily limit
                warnings.append(f"⚠️ Near daily limit ({change_pct:.1f}%) - high volatility risk")
                risk_score += 1
            
            # 6. Check for very high price position
            if price_position['position_pct'] >= 95:
                warnings.append("⚠️ At/near recent high - extreme profit-taking risk")
                risk_score += 2
            elif price_position['position_pct'] >= 90:
                warnings.append("⚠️ Very close to recent high - high profit-taking risk")
                risk_score += 1
            
            return {
                'warnings': warnings,
                'profit_taking_risk_score': min(risk_score, 10),  # Cap at 10
                'has_warnings': len(warnings) > 0,
                'price_position': price_position
            }
            
        except Exception as e:
            return {
                'warnings': [f"Error in profit-taking analysis: {str(e)}"],
                'profit_taking_risk_score': 0,
                'has_warnings': False,
                'price_position': {'position_pct': 50, 'near_high': False, 'near_low': False, 'risk_level': 'UNKNOWN'}
            }
    
    def calculate_support_resistance_levels(self, historical_data, stock_code):
        """Calculate basic support and resistance levels"""
        try:
            stock_hist = historical_data[historical_data['StockCode'] == stock_code].copy()
            if len(stock_hist) < 10:
                return {'support': None, 'resistance': None, 'pivot': None}
            
            # Sort by date
            stock_hist = stock_hist.sort_values('Date')
            
            # Simple support/resistance calculation
            recent_highs = stock_hist['High'].tail(10)
            recent_lows = stock_hist['Low'].tail(10)
            
            # Resistance = 80th percentile of recent highs
            resistance = recent_highs.quantile(0.8)
            
            # Support = 20th percentile of recent lows  
            support = recent_lows.quantile(0.2)
            
            # Pivot point (simplified)
            last_data = stock_hist.iloc[-1]
            pivot = (last_data['High'] + last_data['Low'] + last_data['Close']) / 3
            
            return {
                'support': round(support, 0),
                'resistance': round(resistance, 0), 
                'pivot': round(pivot, 0)
            }
            
        except Exception:
            return {'support': None, 'resistance': None, 'pivot': None}
    
    def analyze_daily_signals(self, df_today, df_historical=None):
        """Enhanced analyze today's trading data with profit-taking detection"""
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
                
                # 🔥 NEW: Enhanced Profit-Taking Analysis
                profit_taking_analysis = self.detect_profit_taking_signals(stock, df_historical, volume_ratio)
                
                # Apply profit-taking risk adjustments to score
                original_score = score
                if score > 0:  # Only adjust positive signals
                    risk_reduction = 0
                    
                    # Reduce score based on profit-taking risk
                    pt_risk = profit_taking_analysis['profit_taking_risk_score']
                    if pt_risk >= 7:
                        risk_reduction += 3
                        reasons.append("⚠️ Extreme profit-taking risk detected")
                    elif pt_risk >= 5:
                        risk_reduction += 2
                        reasons.append("⚠️ High profit-taking risk detected")
                    elif pt_risk >= 3:
                        risk_reduction += 1
                        reasons.append("⚠️ Moderate profit-taking risk detected")
                    
                    # Reduce score based on price position
                    price_pos = profit_taking_analysis['price_position']
                    if price_pos['position_pct'] >= 95:
                        risk_reduction += 2
                        reasons.append("⚠️ At recent high - extreme risk")
                    elif price_pos['position_pct'] >= 85:
                        risk_reduction += 1
                        reasons.append("⚠️ Near recent high - high risk")
                    
                    # Apply reduction
                    score = max(score - risk_reduction, -2)
                
                # Get support/resistance levels
                sr_levels = self.calculate_support_resistance_levels(df_historical, stock['StockCode'])
                
                # Determine signal with enhanced logic
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
                
                # Final override for extreme profit-taking risk
                if (profit_taking_analysis['profit_taking_risk_score'] >= 7 and 
                    signal in ['BUY', 'STRONG BUY']):
                    signal = 'HOLD'  # Downgrade due to extreme profit-taking risk
                    reasons.append("Signal downgraded due to profit-taking risk")
                
                # Calculate enhanced predictions
                predictions = self.calculate_enhanced_predictions(
                    signal, score, change_pct, daily_range, close_position, 
                    stock['Close'], profit_taking_analysis
                )
                
                signal_data.update({
                    'Signal': signal,
                    'Original_Score': round(original_score, 2),
                    'Adjusted_Score': round(score, 2),
                    'Score': round(score, 2),  # Keep for compatibility
                    'Daily_Range_Pct': round(daily_range, 2),
                    'Close_Position': round(close_position, 2),
                    'Volume_Ratio': round(volume_ratio, 2) if avg_volume else 0,
                    'Reasons': '; '.join(reasons) if reasons else 'No significant signals',
                    'Predicted_Gain_Pct': predictions['predicted_gain'],
                    'Target_Price': predictions['target_price'],
                    'Risk_Level': predictions['risk_level'],
                    'Confidence_Score': predictions['confidence'],
                    'Time_Horizon': predictions['time_horizon'],
                    
                    # 🔥 NEW: Enhanced metrics
                    'Price_Position_Pct': profit_taking_analysis['price_position']['position_pct'],
                    'Profit_Taking_Risk': profit_taking_analysis['profit_taking_risk_score'],
                    'Position_Risk_Level': profit_taking_analysis['price_position']['risk_level'],
                    'Warnings': '; '.join(profit_taking_analysis['warnings']) if profit_taking_analysis['warnings'] else 'None',
                    'Support_Level': sr_levels['support'] if sr_levels['support'] else 0,
                    'Resistance_Level': sr_levels['resistance'] if sr_levels['resistance'] else 0,
                    'Near_Support': stock['Close'] <= sr_levels['support'] * 1.05 if sr_levels['support'] else False,
                    'Near_Resistance': stock['Close'] >= sr_levels['resistance'] * 0.95 if sr_levels['resistance'] else False,
                    'Period_High': profit_taking_analysis['price_position']['period_high'],
                    'Period_Low': profit_taking_analysis['price_position']['period_low']
                })
                
                signals.append(signal_data)
                
            except Exception as e:
                print(f"Error analyzing {stock.get('StockCode', 'Unknown')}: {e}")
                continue
        
        return pd.DataFrame(signals)
    
    def calculate_enhanced_predictions(self, signal, score, change_pct, daily_range, 
                                     close_position, current_price, profit_taking_analysis):
        """Enhanced prediction calculation with profit-taking risk adjustment"""
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
        
        # 🔥 NEW: Adjust for profit-taking risk
        pt_risk = profit_taking_analysis['profit_taking_risk_score']
        price_position = profit_taking_analysis['price_position']['position_pct']
        
        # Reduce expected gains if high profit-taking risk
        profit_taking_factor = 1.0
        if pt_risk >= 7:
            profit_taking_factor = 0.3  # Severely reduce expected gains
            confidence_base -= 30
            predictions['time_horizon'] = '1-2 days'  # Shorter horizon due to risk
        elif pt_risk >= 5:
            profit_taking_factor = 0.5
            confidence_base -= 20
        elif pt_risk >= 3:
            profit_taking_factor = 0.7
            confidence_base -= 10
        
        # Adjust for price position
        position_factor = 1.0
        if price_position >= 95:
            position_factor = 0.2  # Very low expected gains near highs
            confidence_base -= 25
        elif price_position >= 90:
            position_factor = 0.4
            confidence_base -= 15
        elif price_position >= 85:
            position_factor = 0.7
            confidence_base -= 10
        
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
        close_position_factor = 1.0
        if signal in ['BUY', 'STRONG BUY']:
            if close_position >= 0.8:  # Closed near high - bullish continuation
                close_position_factor = 1.1
                confidence_base += 5
            elif close_position <= 0.3:  # Closed near low - might reverse
                close_position_factor = 0.9
                confidence_base -= 5
        elif signal in ['SELL', 'STRONG SELL']:
            if close_position <= 0.2:  # Closed near low - bearish continuation
                close_position_factor = 1.1
                confidence_base += 5
            elif close_position >= 0.7:  # Closed near high - might reverse
                close_position_factor = 0.9
                confidence_base -= 5
        
        # Calculate final predicted gain
        final_gain = (base_gain * momentum_factor * volatility_factor * 
                     close_position_factor * profit_taking_factor * position_factor)
        
        # Apply market constraints (Indonesian stocks often limited by daily limits)
        final_gain = max(min(final_gain, 20), -20)  # Cap at +/-20%
        
        # Calculate target price
        target_price = current_price * (1 + final_gain / 100)
        
        # Calculate confidence score
        confidence = min(max(confidence_base, 15), 95)  # Between 15-95%
        
        # Enhanced risk level determination
        risk_score = 0
        if abs(final_gain) >= 10:
            risk_score += 2
        if daily_range >= 8:
            risk_score += 2
        if confidence < 50:
            risk_score += 1
        if abs(change_pct) >= 5:
            risk_score += 1
        if pt_risk >= 5:  # Add profit-taking risk
            risk_score += 2
        if price_position >= 90:  # Add position risk
            risk_score += 1
        
        if risk_score >= 6:
            risk_level = 'VERY HIGH'
        elif risk_score >= 4:
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
        """Save DataFrame to Excel with enhanced formatting including profit-taking metrics"""
        try:
            # Create a workbook and worksheet
            workbook = xlsxwriter.Workbook(output_file)
            worksheet = workbook.add_worksheet('Enhanced Trading Signals')
            
            # Define formats
            formats = {
                'header': workbook.add_format({
                    'bold': True,
                    'font_size': 11,
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
                # Signal formats
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
                # Risk level formats
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
                # Profit-taking risk formats
                'pt_risk_low': workbook.add_format({
                    'bg_color': '#E6F3E6',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'pt_risk_medium': workbook.add_format({
                    'bg_color': '#FFF5CC',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'pt_risk_high': workbook.add_format({
                    'bg_color': '#FFE6CC',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'pt_risk_extreme': workbook.add_format({
                    'bg_color': '#FF9999',
                    'font_color': 'black',
                    'bold': True,
                    'align': 'center',
                    'border': 1
                }),
                # Price position formats
                'position_low': workbook.add_format({
                    'bg_color': '#E6F7FF',
                    'font_color': 'black',
                    'align': 'center',
                    'border': 1
                }),
                'position_high': workbook.add_format({
                    'bg_color': '#FFE6E6',
                    'font_color': 'black',
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
                }),
                'warning_text': workbook.add_format({
                    'align': 'left',
                    'border': 1,
                    'text_wrap': True,
                    'font_color': '#CC6600'
                })
            }
            
            # Enhanced column definitions with new metrics
            columns = [
                ('StockCode', 'Stock Code', 12, 'center_text'),
                ('StockName', 'Stock Name', 20, 'text'),
                ('Signal', 'Signal', 12, 'signal'),
                ('Original_Score', 'Original Score', 12, 'decimal'),
                ('Adjusted_Score', 'Adjusted Score', 12, 'decimal'),
                ('Close', 'Close Price', 12, 'currency'),
                ('Change', 'Change', 10, 'currency'),
                ('Change_Pct', 'Change %', 10, 'percentage'),
                ('Price_Position_Pct', 'Price Position %', 14, 'price_position'),
                ('Profit_Taking_Risk', 'PT Risk Score', 12, 'pt_risk'),
                ('Predicted_Gain_Pct', 'Predicted Gain %', 14, 'percentage'),
                ('Target_Price', 'Target Price', 12, 'currency'),
                ('Confidence_Score', 'Confidence', 10, 'decimal'),
                ('Risk_Level', 'Risk Level', 12, 'risk_level'),
                ('Time_Horizon', 'Time Frame', 10, 'center_text'),
                ('Support_Level', 'Support', 10, 'currency'),
                ('Resistance_Level', 'Resistance', 10, 'currency'),
                ('Volume', 'Volume', 12, 'number'),
                ('Volume_Ratio', 'Vol Ratio', 10, 'decimal'),
                ('Daily_Range_Pct', 'Range %', 10, 'percentage'),
                ('Close_Position', 'Close Pos', 10, 'decimal'),
                ('Warnings', 'Profit-Taking Warnings', 35, 'warning_text'),
                ('Reasons', 'Analysis Reasons', 35, 'text')
            ]
            
            # Write headers
            for col_idx, (col_name, header_text, width, format_type) in enumerate(columns):
                worksheet.write(0, col_idx, header_text, formats['header'])
                worksheet.set_column(col_idx, col_idx, width)
            
            # Write data rows
            for row_idx, (_, row_data) in enumerate(signals_df.iterrows(), start=1):
                for col_idx, (col_name, header_text, width, format_type) in enumerate(columns):
                    value = row_data.get(col_name, 0)
                    
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
                    
                    # Handle Profit-Taking Risk formatting
                    elif col_name == 'Profit_Taking_Risk':
                        if value >= 7:
                            worksheet.write(row_idx, col_idx, value, formats['pt_risk_extreme'])
                        elif value >= 5:
                            worksheet.write(row_idx, col_idx, value, formats['pt_risk_high'])
                        elif value >= 3:
                            worksheet.write(row_idx, col_idx, value, formats['pt_risk_medium'])
                        else:
                            worksheet.write(row_idx, col_idx, value, formats['pt_risk_low'])
                    
                    # Handle Price Position formatting
                    elif col_name == 'Price_Position_Pct':
                        if value >= 85:
                            worksheet.write(row_idx, col_idx, value / 100, formats['position_high'])
                        elif value <= 20:
                            worksheet.write(row_idx, col_idx, value / 100, formats['position_low'])
                        else:
                            worksheet.write(row_idx, col_idx, value / 100, formats['percentage'])
                    
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
                    
                    # Handle Support/Resistance levels
                    elif col_name in ['Support_Level', 'Resistance_Level']:
                        if value == 0:
                            worksheet.write(row_idx, col_idx, 'N/A', formats['center_text'])
                        else:
                            worksheet.write(row_idx, col_idx, value, formats[format_type])
                    
                    # Handle warning text
                    elif col_name == 'Warnings':
                        if value == 'None' or not value:
                            worksheet.write(row_idx, col_idx, 'None', formats['center_text'])
                        else:
                            worksheet.write(row_idx, col_idx, value, formats[format_type])
                    
                    else:
                        worksheet.write(row_idx, col_idx, value, formats[format_type])
            
            # Add autofilter
            worksheet.autofilter(0, 0, len(signals_df), len(columns) - 1)
            
            # Freeze the header row
            worksheet.freeze_panes(1, 0)
            
            # Add enhanced summary information
            summary_start_row = len(signals_df) + 3
            
            # Main summary header
            worksheet.merge_range(summary_start_row, 0, summary_start_row, 4, 
                                'ENHANCED SIGNAL SUMMARY', formats['header'])
            
            # Signal counts
            signal_counts = signals_df['Signal'].value_counts()
            summary_row = summary_start_row + 2
            
            worksheet.write(summary_row, 0, 'Signal Type', formats['header'])
            worksheet.write(summary_row, 1, 'Count', formats['header'])
            worksheet.write(summary_row, 2, 'Percentage', formats['header'])
            summary_row += 1
            
            for signal, count in signal_counts.items():
                percentage = (count / len(signals_df) * 100)
                worksheet.write(summary_row, 0, signal, formats['center_text'])
                worksheet.write(summary_row, 1, count, formats['number'])
                worksheet.write(summary_row, 2, f"{percentage:.1f}%", formats['center_text'])
                summary_row += 1
            
            # Profit-taking risk summary
            summary_row += 2
            worksheet.merge_range(summary_row, 0, summary_row, 4, 
                                'PROFIT-TAKING RISK ANALYSIS', formats['header'])
            summary_row += 2
            
            # High risk stocks
            high_risk_stocks = signals_df[signals_df['Profit_Taking_Risk'] >= 5]
            very_high_risk = signals_df[signals_df['Profit_Taking_Risk'] >= 7]
            near_highs = signals_df[signals_df['Price_Position_Pct'] >= 85]
            
            worksheet.write(summary_row, 0, 'High PT Risk (≥5)', formats['center_text'])
            worksheet.write(summary_row, 1, len(high_risk_stocks), formats['number'])
            worksheet.write(summary_row, 2, f"{len(high_risk_stocks)/len(signals_df)*100:.1f}%", formats['center_text'])
            summary_row += 1
            
            worksheet.write(summary_row, 0, 'Very High PT Risk (≥7)', formats['center_text'])
            worksheet.write(summary_row, 1, len(very_high_risk), formats['number'])
            worksheet.write(summary_row, 2, f"{len(very_high_risk)/len(signals_df)*100:.1f}%", formats['center_text'])
            summary_row += 1
            
            worksheet.write(summary_row, 0, 'Near Recent Highs (≥85%)', formats['center_text'])
            worksheet.write(summary_row, 1, len(near_highs), formats['number'])
            worksheet.write(summary_row, 2, f"{len(near_highs)/len(signals_df)*100:.1f}%", formats['center_text'])
            
            workbook.close()
            print(f"✅ Enhanced formatted Excel file saved: {output_file}")
            
        except Exception as e:
            print(f"❌ Error creating formatted Excel file: {e}")
            # Fallback to regular pandas Excel export
            try:
                signals_df.to_excel(output_file, index=False)
                print(f"⚠️ Saved basic Excel file as fallback: {output_file}")
            except Exception as e2:
                print(f"❌ Fallback export also failed: {e2}")
    
    def run_analysis(self, days_to_analyze=5):
        """Run the complete enhanced analysis"""
        print(f"📊 Loading last {days_to_analyze} days of IDX trading data...")
        print("🔧 Enhanced version with profit-taking detection active!")
        
        # Load historical data for comparison
        historical_data = self.load_multiple_days(days_to_analyze)
        if historical_data is None:
            print("❌ Could not load any data files")
            return None
        
        # Get the most recent day's data
        latest_date = historical_data['Date'].max()
        today_data = historical_data[historical_data['Date'] == latest_date]
        
        print(f"📈 Analyzing {len(today_data)} stocks from {latest_date.strftime('%Y-%m-%d')}")
        print("🔍 Running enhanced analysis with profit-taking detection...")
        
        # Generate enhanced signals
        signals_df = self.analyze_daily_signals(today_data, historical_data)
        
        if signals_df.empty:
            print("❌ No signals generated")
            return None
        
        # Sort by signal strength and adjusted score
        signal_order = ['STRONG BUY', 'BUY', 'HOLD', 'SELL', 'STRONG SELL']
        signals_df['Signal_Order'] = signals_df['Signal'].map({sig: i for i, sig in enumerate(signal_order)})
        signals_df = signals_df.sort_values(['Signal_Order', 'Adjusted_Score'], ascending=[True, False])
        
        # Save enhanced results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(self.signals_folder, f"enhanced_idx_signals_{timestamp}.xlsx")
        
        # Save with enhanced formatting
        self.save_formatted_excel(signals_df, output_file)
        
        # Display enhanced results
        print("\n" + "="*140)
        print(f"🔥 ENHANCED IDX TRADING SIGNALS - {latest_date.strftime('%Y-%m-%d')}")
        print("="*140)
        
        # Show profit-taking warnings first
        high_risk_stocks = signals_df[signals_df['Profit_Taking_Risk'] >= 5]
        if len(high_risk_stocks) > 0:
            print(f"\n⚠️  HIGH PROFIT-TAKING RISK DETECTED ({len(high_risk_stocks)} stocks):")
            for _, stock in high_risk_stocks.head(10).iterrows():
                warnings_preview = stock['Warnings'][:60] + "..." if len(str(stock['Warnings'])) > 60 else stock['Warnings']
                print(f"  🚨 {stock['StockCode']} (Risk: {stock['Profit_Taking_Risk']}/10, "
                      f"Position: {stock['Price_Position_Pct']:.1f}%): {warnings_preview}")
        
        # Show signals with enhanced metrics
        for signal_type in signal_order:
            stocks = signals_df[signals_df['Signal'] == signal_type]
            if len(stocks) > 0:
                print(f"\n🔸 {signal_type} ({len(stocks)} stocks):")
                for _, stock in stocks.head(10).iterrows():
                    predicted_gain = stock.get('Predicted_Gain_Pct', 0)
                    target_price = stock.get('Target_Price', stock['Close'])
                    confidence = stock.get('Confidence_Score', 0)
                    risk = stock.get('Risk_Level', 'N/A')
                    pt_risk = stock.get('Profit_Taking_Risk', 0)
                    price_pos = stock.get('Price_Position_Pct', 50)
                    original_score = stock.get('Original_Score', 0)
                    adjusted_score = stock.get('Adjusted_Score', 0)
                    
                    # Add risk indicators
                    risk_indicators = []
                    if pt_risk >= 5:
                        risk_indicators.append(f"⚠️PT:{pt_risk}")
                    if price_pos >= 85:
                        risk_indicators.append(f"📊{price_pos:.0f}%")
                    if original_score != adjusted_score:
                        risk_indicators.append(f"📉{original_score:.1f}→{adjusted_score:.1f}")
                    
                    risk_text = f" [{', '.join(risk_indicators)}]" if risk_indicators else ""
                    
                    print(f"  • {stock['StockCode']} ({stock['StockName'][:20]}): "
                          f"Current: Rp{stock['Close']:,.0f}, Target: Rp{target_price:,.0f} "
                          f"({predicted_gain:+.1f}%), Risk: {risk}, Confidence: {confidence}%{risk_text}")
                
                if len(stocks) > 10:
                    print(f"    ... and {len(stocks) - 10} more")
        
        # Enhanced summary statistics
        print(f"\n📊 Enhanced Summary Statistics:")
        
        # Signal distribution
        signal_counts = signals_df['Signal'].value_counts()
        for signal, count in signal_counts.items():
            percentage = (count / len(signals_df) * 100)
            print(f"   {signal}: {count} stocks ({percentage:.1f}%)")
        
        # Risk analysis summary
        print(f"\n🔍 Risk Analysis Summary:")
        very_high_pt_risk = len(signals_df[signals_df['Profit_Taking_Risk'] >= 7])
        high_pt_risk = len(signals_df[signals_df['Profit_Taking_Risk'] >= 5])
        near_highs = len(signals_df[signals_df['Price_Position_Pct'] >= 85])
        score_adjustments = len(signals_df[signals_df['Original_Score'] != signals_df['Adjusted_Score']])
        
        print(f"   Very High Profit-Taking Risk (≥7): {very_high_pt_risk} stocks ({very_high_pt_risk/len(signals_df)*100:.1f}%)")
        print(f"   High Profit-Taking Risk (≥5): {high_pt_risk} stocks ({high_pt_risk/len(signals_df)*100:.1f}%)")
        print(f"   Near Recent Highs (≥85%): {near_highs} stocks ({near_highs/len(signals_df)*100:.1f}%)")
        print(f"   Signals Adjusted for Risk: {score_adjustments} stocks ({score_adjustments/len(signals_df)*100:.1f}%)")
        
        print(f"\n💾 Enhanced detailed report saved: {output_file}")
        print("🔥 New features: Profit-taking risk detection, price position analysis, enhanced predictions!")
        
        return signals_df
    
    def get_top_picks(self, signals_df, signal_type='BUY', top_n=10, exclude_high_risk=True):
        """Get top stock picks for a specific signal type with enhanced filtering"""
        if signals_df is None:
            return None
        
        filtered = signals_df[signals_df['Signal'].isin([signal_type, f'STRONG {signal_type}'])]
        
        # Option to exclude high profit-taking risk stocks
        if exclude_high_risk:
            filtered = filtered[filtered['Profit_Taking_Risk'] < 7]  # Exclude very high risk
            print(f"🔍 Filtering out stocks with very high profit-taking risk (≥7)")
        
        # Sort by adjusted score and confidence
        filtered = filtered.sort_values(['Adjusted_Score', 'Confidence_Score'], ascending=[False, False])
        
        return filtered.head(top_n)
    
    def get_risk_analysis_summary(self, signals_df):
        """Get detailed risk analysis summary"""
        if signals_df is None:
            return None
        
        summary = {
            'total_stocks': len(signals_df),
            'high_pt_risk': len(signals_df[signals_df['Profit_Taking_Risk'] >= 5]),
            'very_high_pt_risk': len(signals_df[signals_df['Profit_Taking_Risk'] >= 7]),
            'near_highs': len(signals_df[signals_df['Price_Position_Pct'] >= 85]),
            'score_adjustments': len(signals_df[signals_df['Original_Score'] != signals_df['Adjusted_Score']]),
            'avg_pt_risk': signals_df['Profit_Taking_Risk'].mean(),
            'avg_price_position': signals_df['Price_Position_Pct'].mean()
        }
        
        return summary

if __name__ == "__main__":
    print("🚀 Enhanced IDX Daily Trading Signal Analyzer")
    print("🔥 NOW WITH PROFIT-TAKING DETECTION!")
    print("="*60)
    
    # Check if xlsxwriter is available
    try:
        import xlsxwriter
        print("✅ XlsxWriter library detected - Enhanced Excel formatting will be applied")
    except ImportError:
        print("⚠️  XlsxWriter library not found. Install it with: pip install xlsxwriter")
        print("    Fallback: Basic Excel export will be used")
    
    analyzer = EnhancedIDXTradingSignalAnalyzer()
    
    # Ask user for number of days to analyze
    try:
        days = int(input("How many days of data to analyze for comparison? (default 5): ") or "5")
    except ValueError:
        days = 5
    
    results = analyzer.run_analysis(days_to_analyze=days)
    
    if results is not None:
        print(f"\n✅ Enhanced analysis complete! Analyzed {len(results)} stocks.")
        
        # Show enhanced top picks with risk filtering
        print("\n🏆 TOP BUY RECOMMENDATIONS (Low Profit-Taking Risk):")
        top_buys = analyzer.get_top_picks(results, 'BUY', 5, exclude_high_risk=True)
        if top_buys is not None and len(top_buys) > 0:
            for _, stock in top_buys.iterrows():
                predicted_gain = stock.get('Predicted_Gain_Pct', 0)
                target_price = stock.get('Target_Price', stock['Close'])
                confidence = stock.get('Confidence_Score', 0)
                risk = stock.get('Risk_Level', 'N/A')
                time_frame = stock.get('Time_Horizon', 'N/A')
                pt_risk = stock.get('Profit_Taking_Risk', 0)
                price_pos = stock.get('Price_Position_Pct', 50)
                
                print(f"  🔥 {stock['StockCode']} - Current: Rp{stock['Close']:,.0f}, "
                      f"Target: Rp{target_price:,.0f} ({predicted_gain:+.1f}%)")
                print(f"     Risk: {risk}, Confidence: {confidence}%, Timeframe: {time_frame}")
                print(f"     PT Risk: {pt_risk}/10, Price Position: {price_pos:.1f}%")
        else:
            print("  No low-risk buy signals found today")
        
        # Show risk analysis summary
        risk_summary = analyzer.get_risk_analysis_summary(results)
        if risk_summary:
            print(f"\n📊 Risk Analysis Summary:")
            print(f"   Total stocks analyzed: {risk_summary['total_stocks']}")
            print(f"   High profit-taking risk: {risk_summary['high_pt_risk']} "
                  f"({risk_summary['high_pt_risk']/risk_summary['total_stocks']*100:.1f}%)")
            print(f"   Average profit-taking risk: {risk_summary['avg_pt_risk']:.1f}/10")
            print(f"   Average price position: {risk_summary['avg_price_position']:.1f}%")
            
        print(f"\n📁 Check '{analyzer.signals_folder}' folder for enhanced Excel report")
        print("🔥 New Excel features: Color-coded profit-taking risk, price position analysis, enhanced warnings!")
    else:
        print("\n❌ Analysis failed. Check your data files in 'Stock Summary Folder'")