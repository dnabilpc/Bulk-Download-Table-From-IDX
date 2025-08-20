import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
import warnings
import xlsxwriter
from scipy import stats
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")


class EnhancedIDXTechnicalAnalyzer:
    def __init__(self, data_folder="Stock Summary Folder"):
        self.data_folder = data_folder
        self.signals_folder = "Trading Signals"
        os.makedirs(self.signals_folder, exist_ok=True)

        # Enhanced parameters
        self.bb_periods = [10, 20, 30]  # Multiple BB timeframes
        self.volume_lookback = 15
        self.trend_strength_period = 20

    def load_daily_summary(self, filename):
        """Load daily trading summary from Excel file with improved error handling"""
        try:
            filepath = os.path.join(self.data_folder, filename)
            if not os.path.exists(filepath):
                logger.warning(f"File not found: {filepath}")
                return None

            df = pd.read_excel(filepath)

            if df.empty:
                logger.warning(f"Empty dataframe from {filename}")
                return None

            # Clean column names
            df.columns = df.columns.str.strip()

            # Check for required columns
            required_columns = ["Close", "Volume"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                logger.error(
                    f"Missing required columns in {filename}: {missing_columns}"
                )
                return None

            # Convert numeric columns with better error handling
            numeric_columns = [
                "Previous",
                "OpenPrice",
                "High",
                "Low",
                "Close",
                "Change",
                "Volume",
                "Value",
                "Frequency",
                "OfferVolume",
                "BidVolume"
            ]

            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce")

            # Extract date from filename with better parsing
            date_str = filename.split("_")[-1].replace(".xlsx", "")
            try:
                # Try multiple date formats
                for fmt in ["%Y%m%d", "%Y-%m-%d", "%d%m%Y"]:
                    try:
                        df["Date"] = pd.to_datetime(date_str, format=fmt)
                        break
                    except ValueError:
                        continue
                else:
                    logger.warning(
                        f"Could not parse date from filename {filename}, using current date"
                    )
                    df["Date"] = pd.to_datetime("today").normalize()
            except Exception as e:
                logger.error(f"Date parsing error for {filename}: {e}")
                df["Date"] = pd.to_datetime("today").normalize()

            # Clean data with validation
            original_len = len(df)
            df = df.dropna(subset=["Close", "Volume"])
            df = df[(df["Close"] > 0) & (df["Volume"] > 0)]

            # Fill missing Previous values with Close
            if "Previous" in df.columns:
                df["Previous"] = df["Previous"].fillna(df["Close"])
            else:
                df["Previous"] = df["Close"]

            # Fill missing High/Low with Close if needed
            if "High" not in df.columns or df["High"].isna().all():
                df["High"] = df["Close"]
            if "Low" not in df.columns or df["Low"].isna().all():
                df["Low"] = df["Close"]

            df["High"] = df["High"].fillna(df["Close"])
            df["Low"] = df["Low"].fillna(df["Close"])

            # Ensure High >= Close >= Low
            df["High"] = np.maximum(df["High"], df["Close"])
            df["Low"] = np.minimum(df["Low"], df["Close"])

            cleaned_len = len(df)
            if cleaned_len < original_len * 0.5:
                logger.warning(
                    f"Too much data lost during cleaning in {filename}: {original_len} -> {cleaned_len}"
                )

            return df if not df.empty else None

        except Exception as e:
            logger.error(f"Error loading {filename}: {e}")
            return None

    def load_multiple_days(self, days_to_load=35):
        """Load multiple days of data with improved validation"""
        if not os.path.exists(self.data_folder):
            logger.error(f"Data folder not found: {self.data_folder}")
            return None

        excel_files = sorted(
            [
                f
                for f in os.listdir(self.data_folder)
                if f.endswith(".xlsx") and not f.startswith("~")
            ]
        )

        if not excel_files:
            logger.error(f"No Excel files found in {self.data_folder}")
            return None

        recent_files = excel_files[-days_to_load:]
        logger.info(
            f"Loading {len(recent_files)} files from the last {days_to_load} days"
        )

        all_data = []
        successful_loads = 0

        for filename in recent_files:
            daily_data = self.load_daily_summary(filename)
            if daily_data is not None and not daily_data.empty:
                all_data.append(daily_data)
                successful_loads += 1
                logger.info(f"Loaded {filename}: {len(daily_data)} stocks")
            else:
                logger.warning(f"Failed to load or empty data: {filename}")

        if not all_data:
            logger.error("No data could be loaded from any files")
            return None

        logger.info(f"Successfully loaded {successful_loads}/{len(recent_files)} files")

        try:
            combined_df = pd.concat(all_data, ignore_index=True)
            # Sort by date and stock code for consistency
            combined_df = combined_df.sort_values(["Date", "StockCode"]).reset_index(
                drop=True
            )
            return combined_df
        except Exception as e:
            logger.error(f"Error combining data: {e}")
            return None

    def safe_divide(self, numerator, denominator, fill_value=0):
        """Safely divide two series, handling division by zero"""
        try:
            # Handle scalar denominator
            if np.isscalar(denominator):
                if denominator == 0:
                    return pd.Series([fill_value] * len(numerator), index=numerator.index)
                return numerator / denominator
            
            # Handle series denominator
            result = numerator.copy()
            mask = (denominator != 0) & (~denominator.isna())
            result.loc[mask] = numerator.loc[mask] / denominator.loc[mask]
            result.loc[~mask] = fill_value
            return result
        except Exception as e:
            logger.warning(f"Error in safe_divide: {e}")
            return pd.Series([fill_value] * len(numerator), index=numerator.index)

    def calculate_advanced_moving_averages(self, stock_data):
        """Calculate advanced moving averages with better error handling"""
        try:
            if len(stock_data) < 5:
                logger.warning("Insufficient data for moving averages")
                return stock_data

            # Multiple timeframe SMAs
            for period in [5, 10, 15, 20, 30]:
                if len(stock_data) >= period:
                    stock_data[f"SMA_{period}"] = (
                        stock_data["Close"].rolling(window=period, min_periods=1).mean()
                    )
                else:
                    stock_data[f"SMA_{period}"] = stock_data["Close"]

            # Multiple timeframe EMAs
            for period in [5, 10, 15, 20]:
                if len(stock_data) >= period:
                    stock_data[f"EMA_{period}"] = (
                        stock_data["Close"].ewm(span=period, adjust=False).mean()
                    )
                else:
                    stock_data[f"EMA_{period}"] = stock_data["Close"]

            # Adaptive Moving Average (AMA) with safety checks
            stock_data["Price_Change"] = stock_data["Close"].diff().fillna(0)
            stock_data["Volatility"] = (
                stock_data["Price_Change"]
                .rolling(window=min(10, len(stock_data)))
                .std()
                .fillna(0)
            )

            # Calculate efficiency ratio with safety checks
            if len(stock_data) >= 10:
                direction = stock_data["Close"].diff(10).fillna(0)
                volatility_sum = (
                    stock_data["Price_Change"].abs().rolling(window=10).sum().fillna(1)
                )
                # Use safe division
                stock_data["Efficiency_Ratio"] = self.safe_divide(
                    direction.abs(), volatility_sum, fill_value=0.5
                )
            else:
                stock_data["Efficiency_Ratio"] = 0.5

            # Smoothing constants
            fastest_sc = 2 / (2 + 1)
            slowest_sc = 2 / (30 + 1)
            stock_data["SC"] = (
                stock_data["Efficiency_Ratio"] * (fastest_sc - slowest_sc) + slowest_sc
            ) ** 2

            # Calculate AMA with improved initialization
            stock_data["AMA"] = np.nan
            start_idx = min(19, len(stock_data) - 1)
            if start_idx >= 0:
                stock_data.loc[stock_data.index[start_idx], "AMA"] = stock_data[
                    "Close"
                ].iloc[start_idx]

                for i in range(start_idx + 1, len(stock_data)):
                    if pd.notna(stock_data["SC"].iloc[i]) and pd.notna(
                        stock_data["AMA"].iloc[i - 1]
                    ):
                        stock_data.loc[stock_data.index[i], "AMA"] = stock_data[
                            "AMA"
                        ].iloc[i - 1] + stock_data["SC"].iloc[i] * (
                            stock_data["Close"].iloc[i] - stock_data["AMA"].iloc[i - 1]
                        )
                    else:
                        stock_data.loc[stock_data.index[i], "AMA"] = stock_data[
                            "Close"
                        ].iloc[i]

            # Fill remaining NaN values
            stock_data["AMA"] = stock_data["AMA"].fillna(stock_data["Close"])

            # Trend strength indicator with safety checks
            ema_10 = stock_data.get("EMA_10", stock_data["Close"])
            ema_20 = stock_data.get("EMA_20", stock_data["Close"])

            # Use safe division
            stock_data["Trend_Strength"] = self.safe_divide(
                (ema_10 - ema_20).abs(), ema_20, fill_value=0
            ) * 100

            # Enhanced MA signals with null checks
            stock_data["MA_Bullish"] = (
                (stock_data.get("EMA_5", 0) > stock_data.get("EMA_10", 0))
                & (stock_data.get("EMA_10", 0) > stock_data.get("EMA_20", 0))
                & (stock_data["Close"] > stock_data.get("AMA", stock_data["Close"]))
            )

            stock_data["MA_Bearish"] = (
                (stock_data.get("EMA_5", 0) < stock_data.get("EMA_10", 0))
                & (stock_data.get("EMA_10", 0) < stock_data.get("EMA_20", 0))
                & (stock_data["Close"] < stock_data.get("AMA", stock_data["Close"]))
            )

            return stock_data

        except Exception as e:
            logger.error(f"Error calculating moving averages: {e}")
            return stock_data

    def calculate_enhanced_rsi(self, stock_data):
        """Calculate multiple RSI periods with improved error handling"""
        try:
            for period in [9, 14, 21]:
                if len(stock_data) >= period:
                    delta = stock_data["Close"].diff(1).fillna(0)
                    gain = (
                        (delta.where(delta > 0, 0))
                        .rolling(window=period, min_periods=1)
                        .mean()
                    )
                    loss = (
                        (-delta.where(delta < 0, 0))
                        .rolling(window=period, min_periods=1)
                        .mean()
                    )

                    # Use safe division
                    rs = self.safe_divide(gain, loss, fill_value=1)
                    stock_data[f"RSI_{period}"] = (100 - (100 / (1 + rs))).fillna(50)
                else:
                    stock_data[f"RSI_{period}"] = 50  # Neutral RSI for insufficient data

            # RSI Stochastic (StochRSI) with safety checks
            rsi_14 = stock_data.get("RSI_14", pd.Series([50] * len(stock_data)))
            if len(stock_data) >= 14:
                rsi_min = rsi_14.rolling(window=14, min_periods=1).min()
                rsi_max = rsi_14.rolling(window=14, min_periods=1).max()
                rsi_range = rsi_max - rsi_min
                
                # Use safe division
                stock_data["StochRSI"] = self.safe_divide(
                    (rsi_14 - rsi_min), rsi_range, fill_value=0.5
                ) * 100
                stock_data["StochRSI"] = stock_data["StochRSI"].fillna(50)
            else:
                stock_data["StochRSI"] = 50

            # RSI divergence detection with improved logic
            if len(stock_data) >= 10:
                stock_data["Price_Higher_High"] = (
                    stock_data["High"] > stock_data["High"].shift(5)
                ) & (stock_data["High"].shift(5) > stock_data["High"].shift(10))
                stock_data["RSI_Lower_High"] = (
                    stock_data["RSI_14"] < stock_data["RSI_14"].shift(5)
                ) & (stock_data["RSI_14"].shift(5) < stock_data["RSI_14"].shift(10))
                stock_data["Bearish_Divergence"] = (
                    stock_data["Price_Higher_High"] & stock_data["RSI_Lower_High"]
                )
            else:
                stock_data["Price_Higher_High"] = False
                stock_data["RSI_Lower_High"] = False
                stock_data["Bearish_Divergence"] = False

            return stock_data

        except Exception as e:
            logger.error(f"Error calculating RSI: {e}")
            return stock_data

    def calculate_multi_timeframe_bollinger_bands(self, stock_data):
        """Calculate Bollinger Bands with improved error handling"""
        try:
            for period in self.bb_periods:
                if len(stock_data) >= period:
                    sma = (
                        stock_data["Close"].rolling(window=period, min_periods=1).mean()
                    )
                    std = (
                        stock_data["Close"]
                        .rolling(window=period, min_periods=1)
                        .std()
                        .fillna(0)
                    )

                    stock_data[f"BB_Upper_{period}"] = sma + (2 * std)
                    stock_data[f"BB_Lower_{period}"] = sma - (2 * std)
                    stock_data[f"BB_Middle_{period}"] = sma

                    # Calculate BB Width with safety check
                    stock_data[f"BB_Width_{period}"] = self.safe_divide(
                        (stock_data[f"BB_Upper_{period}"] - stock_data[f"BB_Lower_{period}"]),
                        sma,
                        fill_value=0.04
                    ) * 100

                    # Calculate %B with safety check
                    bb_range = (
                        stock_data[f"BB_Upper_{period}"]
                        - stock_data[f"BB_Lower_{period}"]
                    )
                    stock_data[f"BB_PercentB_{period}"] = self.safe_divide(
                        (stock_data["Close"] - stock_data[f"BB_Lower_{period}"]),
                        bb_range,
                        fill_value=0.5
                    ) * 100

                    # Multi-standard deviation bands
                    stock_data[f"BB_Upper_{period}_1std"] = sma + std
                    stock_data[f"BB_Lower_{period}_1std"] = sma - std
                    stock_data[f"BB_Upper_{period}_3std"] = sma + (3 * std)
                    stock_data[f"BB_Lower_{period}_3std"] = sma - (3 * std)
                else:
                    # Fallback for insufficient data
                    close_price = stock_data["Close"]
                    stock_data[f"BB_Upper_{period}"] = close_price * 1.02
                    stock_data[f"BB_Lower_{period}"] = close_price * 0.98
                    stock_data[f"BB_Middle_{period}"] = close_price
                    stock_data[f"BB_Width_{period}"] = 4.0  # 2% width
                    stock_data[f"BB_PercentB_{period}"] = 50  # Middle of bands

            # BB Squeeze detection with improved logic
            try:
                bb_widths = []
                for period in self.bb_periods:
                    width_col = f"BB_Width_{period}"
                    if width_col in stock_data.columns:
                        width = stock_data[width_col]
                        if len(width.dropna()) > 10:
                            width_ma = width.rolling(window=min(10, len(width))).mean()
                            # Handle potential NaN values
                            width_ma = width_ma.fillna(width)
                            squeeze_condition = (width < (width_ma * 0.8)).fillna(False)
                            bb_widths.append(squeeze_condition)

                if bb_widths:
                    # All timeframes showing squeeze
                    stock_data["Multi_BB_Squeeze"] = pd.concat(bb_widths, axis=1).all(
                        axis=1
                    )
                else:
                    stock_data["Multi_BB_Squeeze"] = False
            except Exception as e:
                logger.warning(f"Error calculating BB squeeze: {e}")
                stock_data["Multi_BB_Squeeze"] = False

            # BB confluence calculation
            try:
                bb_signals = []
                for period in self.bb_periods:
                    percentb_col = f"BB_PercentB_{period}"
                    if percentb_col in stock_data.columns:
                        # Handle potential NaN values in BB signals
                        bb_signal = stock_data[percentb_col].fillna(50)  # Neutral value for NaN
                        bb_signals.append(bb_signal)

                if bb_signals:
                    # Calculate average %B across timeframes
                    bb_df = pd.concat(bb_signals, axis=1)
                    stock_data["BB_Confluence"] = bb_df.mean(axis=1)

                    # Strong signals when all timeframes agree
                    oversold_signals = bb_df < 20
                    overbought_signals = bb_df > 80

                    stock_data["BB_Strong_Oversold"] = oversold_signals.all(axis=1)
                    stock_data["BB_Strong_Overbought"] = overbought_signals.all(axis=1)
                else:
                    stock_data["BB_Confluence"] = 50
                    stock_data["BB_Strong_Oversold"] = False
                    stock_data["BB_Strong_Overbought"] = False
            except Exception as e:
                logger.warning(f"Error calculating BB confluence: {e}")
                stock_data["BB_Confluence"] = 50
                stock_data["BB_Strong_Oversold"] = False
                stock_data["BB_Strong_Overbought"] = False

            return stock_data

        except Exception as e:
            logger.error(f"Error calculating Bollinger Bands: {e}")
            return stock_data

    def calculate_advanced_volume_analysis(self, stock_data):
        """Enhanced volume analysis with better error handling"""
        try:
            # Volume moving averages
            for period in [5, 10, 20, 30]:
                if len(stock_data) >= period:
                    stock_data[f"Volume_MA_{period}"] = (
                        stock_data["Volume"]
                        .rolling(window=period, min_periods=1)
                        .mean()
                    )
                else:
                    stock_data[f"Volume_MA_{period}"] = stock_data["Volume"]

            # Volume Rate of Change
            if len(stock_data) >= 5:
                stock_data["Volume_ROC"] = (
                    stock_data["Volume"].pct_change(5).fillna(0) * 100
                )
            else:
                stock_data["Volume_ROC"] = 0

            # Money Flow Index (MFI) with improved calculation
            try:
                typical_price = (
                    stock_data["High"] + stock_data["Low"] + stock_data["Close"]
                ) / 3
                money_flow = typical_price * stock_data["Volume"]

                if len(stock_data) >= 14:
                    price_diff = typical_price.diff().fillna(0)
                    positive_flow = (
                        money_flow.where(price_diff > 0, 0)
                        .rolling(window=14, min_periods=1)
                        .sum()
                    )
                    negative_flow = (
                        money_flow.where(price_diff < 0, 0)
                        .rolling(window=14, min_periods=1)
                        .sum()
                    )

                    # Use safe division
                    money_ratio = self.safe_divide(positive_flow, negative_flow, fill_value=1)
                    stock_data["MFI"] = (100 - (100 / (1 + money_ratio))).fillna(50)
                else:
                    stock_data["MFI"] = 50
            except Exception as e:
                logger.warning(f"Error calculating MFI: {e}")
                stock_data["MFI"] = 50

            # Accumulation/Distribution Line
            try:
                high_low_diff = stock_data["High"] - stock_data["Low"]
                
                # Use safe division for AD multiplier
                numerator = (
                    (stock_data["Close"] - stock_data["Low"])
                    - (stock_data["High"] - stock_data["Close"])
                )
                stock_data["AD_Multiplier"] = self.safe_divide(
                    numerator, high_low_diff, fill_value=0
                )

                stock_data["AD_Line"] = (
                    stock_data["AD_Multiplier"] * stock_data["Volume"]
                ).cumsum()

                # Chaikin Money Flow
                if len(stock_data) >= 20:
                    stock_data["CMF"] = (
                        stock_data["AD_Multiplier"]
                        .rolling(window=20, min_periods=1)
                        .mean()
                    )
                else:
                    stock_data["CMF"] = stock_data["AD_Multiplier"].mean()
            except Exception as e:
                logger.warning(f"Error calculating A/D indicators: {e}")
                stock_data["AD_Multiplier"] = 0
                stock_data["AD_Line"] = 0
                stock_data["CMF"] = 0

            # Volume Profile Analysis
            stock_data["Volume_Profile_Score"] = 0

            # High volume breakouts
            vol_ma_20 = stock_data.get("Volume_MA_20", stock_data["Volume"])
            price_change = stock_data["Close"].pct_change().fillna(0).abs()

            stock_data["Volume_Breakout"] = (stock_data["Volume"] > vol_ma_20 * 2) & (
                price_change > 0.03
            )

            # Volume confirmation patterns
            vol_ma_10 = stock_data.get("Volume_MA_10", stock_data["Volume"])
            price_up = stock_data["Close"] > stock_data["Close"].shift(1)
            price_down = stock_data["Close"] < stock_data["Close"].shift(1)
            high_volume = stock_data["Volume"] > vol_ma_10

            stock_data["Volume_Confirmation"] = high_volume & (price_up | price_down)

            return stock_data

        except Exception as e:
            logger.error(f"Error calculating volume analysis: {e}")
            return stock_data

    def calculate_momentum_oscillators(self, stock_data):
        """Advanced momentum indicators with better error handling"""
        try:
            # Stochastic Oscillator
            if len(stock_data) >= 14:
                low_14 = stock_data["Low"].rolling(window=14, min_periods=1).min()
                high_14 = stock_data["High"].rolling(window=14, min_periods=1).max()

                high_low_range = high_14 - low_14

                # Use safe division for Stochastic K
                stock_data["Stoch_K"] = self.safe_divide(
                    (stock_data["Close"] - low_14), high_low_range, fill_value=0.5
                ) * 100
                stock_data["Stoch_K"] = stock_data["Stoch_K"].fillna(50)
                
                stock_data["Stoch_D"] = (
                    stock_data["Stoch_K"].rolling(window=3, min_periods=1).mean()
                )

                # Williams %R
                stock_data["Williams_R"] = self.safe_divide(
                    (high_14 - stock_data["Close"]), high_low_range, fill_value=0.5
                ) * -100
                stock_data["Williams_R"] = stock_data["Williams_R"].fillna(-50)
            else:
                stock_data["Stoch_K"] = 50
                stock_data["Stoch_D"] = 50
                stock_data["Williams_R"] = -50

            # Commodity Channel Index (CCI)
            try:
                if len(stock_data) >= 20:
                    tp = (
                        stock_data["High"] + stock_data["Low"] + stock_data["Close"]
                    ) / 3
                    sma_tp = tp.rolling(window=20, min_periods=1).mean()

                    # Calculate mean deviation manually to avoid lambda function issues
                    deviations = []
                    for i in range(len(tp)):
                        start_idx = max(0, i - 19)
                        window_data = tp.iloc[start_idx : i + 1]
                        window_mean = window_data.mean()
                        mean_dev = abs(window_data - window_mean).mean()
                        deviations.append(mean_dev if mean_dev > 0 else 1)

                    mean_dev_series = pd.Series(deviations, index=tp.index)
                    stock_data["CCI"] = self.safe_divide(
                        (tp - sma_tp), (0.015 * mean_dev_series), fill_value=0
                    )
                else:
                    stock_data["CCI"] = 0
            except Exception as e:
                logger.warning(f"Error calculating CCI: {e}")
                stock_data["CCI"] = 0

            # Rate of Change for multiple periods
            for period in [5, 10, 20]:
                if len(stock_data) > period:
                    shifted_close = stock_data["Close"].shift(period)
                    # Use safe division
                    stock_data[f"ROC_{period}"] = self.safe_divide(
                        (stock_data["Close"] - shifted_close), shifted_close, fill_value=0
                    ) * 100
                else:
                    stock_data[f"ROC_{period}"] = 0

            # Momentum
            if len(stock_data) >= 10:
                stock_data["Momentum_10"] = stock_data["Close"] - stock_data[
                    "Close"
                ].shift(10)
            else:
                stock_data["Momentum_10"] = 0

            return stock_data

        except Exception as e:
            logger.error(f"Error calculating momentum oscillators: {e}")
            return stock_data

    def detect_market_regime(self, stock_data):
        """Detect market regime with improved error handling"""
        try:
            if len(stock_data) >= 14:
                # ADX calculation with safety checks
                high_diff = stock_data["High"].diff().fillna(0)
                low_diff = stock_data["Low"].diff().fillna(0)

                plus_dm = high_diff.where((high_diff > low_diff) & (high_diff > 0), 0)
                minus_dm = (-low_diff).where((low_diff > high_diff) & (low_diff < 0), 0)

                # True Range calculation
                tr1 = stock_data["High"] - stock_data["Low"]
                tr2 = (stock_data["High"] - stock_data["Close"].shift()).abs()
                tr3 = (stock_data["Low"] - stock_data["Close"].shift()).abs()

                tr_df = pd.concat([tr1, tr2, tr3], axis=1).fillna(0)
                true_range = tr_df.max(axis=1)

                atr = true_range.rolling(window=14, min_periods=1).mean()

                # Use safe division for DI calculations
                plus_di = 100 * self.safe_divide(
                    plus_dm.rolling(window=14, min_periods=1).mean(), atr, fill_value=0.25
                )
                minus_di = 100 * self.safe_divide(
                    minus_dm.rolling(window=14, min_periods=1).mean(), atr, fill_value=0.25
                )

                # DX calculation with safety check
                di_sum = plus_di + minus_di
                dx = 100 * self.safe_divide((plus_di - minus_di).abs(), di_sum, fill_value=0.2)

                stock_data["ADX"] = dx.rolling(window=14, min_periods=1).mean()
                stock_data["Plus_DI"] = plus_di
                stock_data["Minus_DI"] = minus_di
            else:
                stock_data["ADX"] = 20  # Neutral trend strength
                stock_data["Plus_DI"] = 25
                stock_data["Minus_DI"] = 25

            # Market regime classification
            adx = stock_data.get("ADX", pd.Series([20] * len(stock_data)))
            stock_data["Market_Regime"] = "RANGE"
            stock_data.loc[adx > 25, "Market_Regime"] = "TREND"
            stock_data.loc[adx > 40, "Market_Regime"] = "STRONG_TREND"

            # Volatility regime with better calculation
            if len(stock_data) >= 20:
                price_returns = stock_data["Close"].pct_change().fillna(0)
                stock_data["Volatility_20"] = (
                    price_returns.rolling(window=20, min_periods=1).std()
                    * np.sqrt(252)
                    * 100
                )

                # Calculate volatility quantiles
                vol_60_window = min(60, len(stock_data))
                if vol_60_window >= 20:
                    vol_80th = (
                        stock_data["Volatility_20"]
                        .rolling(window=vol_60_window, min_periods=20)
                        .quantile(0.8)
                    )
                    vol_20th = (
                        stock_data["Volatility_20"]
                        .rolling(window=vol_60_window, min_periods=20)
                        .quantile(0.2)
                    )

                    stock_data["Vol_Regime"] = "NORMAL"
                    stock_data.loc[
                        stock_data["Volatility_20"] > vol_80th, "Vol_Regime"
                    ] = "HIGH"
                    stock_data.loc[
                        stock_data["Volatility_20"] < vol_20th, "Vol_Regime"
                    ] = "LOW"
                else:
                    stock_data["Vol_Regime"] = "NORMAL"
            else:
                stock_data["Volatility_20"] = 20  # Default volatility
                stock_data["Vol_Regime"] = "NORMAL"

            return stock_data

        except Exception as e:
            logger.error(f"Error detecting market regime: {e}")
            # Fallback values
            stock_data["ADX"] = 20
            stock_data["Plus_DI"] = 25
            stock_data["Minus_DI"] = 25
            stock_data["Market_Regime"] = "RANGE"
            stock_data["Volatility_20"] = 20
            stock_data["Vol_Regime"] = "NORMAL"
            return stock_data

    def apply_all_indicators(self, stock_data):
        """Apply all technical indicators with comprehensive error handling"""
        try:
            if stock_data is None or stock_data.empty:
                logger.error("Empty stock data provided to apply_all_indicators")
                return None

            # Ensure we have minimum required data
            if len(stock_data) < 5:
                logger.warning(
                    f"Insufficient data points ({len(stock_data)}) for comprehensive analysis"
                )

            stock_data = self.calculate_advanced_moving_averages(stock_data)
            stock_data = self.calculate_enhanced_rsi(stock_data)
            stock_data = self.calculate_multi_timeframe_bollinger_bands(stock_data)
            stock_data = self.calculate_advanced_volume_analysis(stock_data)
            stock_data = self.calculate_momentum_oscillators(stock_data)
            stock_data = self.detect_market_regime(stock_data)

            return stock_data

        except Exception as e:
            logger.error(f"Error applying indicators: {e}")
            return stock_data

    def generate_enhanced_signals(self, stock_data):
        """Generate enhanced trading signals with improved error handling"""
        try:
            if stock_data is None or stock_data.empty:
                return self._generate_fallback_signal(pd.DataFrame())

            if len(stock_data) < 10:
                return self._generate_fallback_signal(stock_data)

            latest = stock_data.iloc[-1]
            prev = stock_data.iloc[-2] if len(stock_data) > 1 else latest

            reasons = []

            # Initialize component scores (weighted system)
            trend_score = 0  # 35% weight
            momentum_score = 0  # 25% weight
            volume_score = 0  # 20% weight
            volatility_score = 0  # 20% weight

            # 1. TREND ANALYSIS (35% weight)
            # Moving Average Analysis with safety checks
            ma_bullish_count = 0
            ma_columns = ["EMA_5", "EMA_10", "EMA_20"]

            for ma in ma_columns:
                ma_value = latest.get(ma)
                if ma_value is not None and not pd.isna(ma_value) and ma_value > 0:
                    if latest["Close"] > ma_value:
                        ma_bullish_count += 1

            # Check MA hierarchy
            ema_5 = latest.get("EMA_5", 0)
            ema_10 = latest.get("EMA_10", 0)
            ema_20 = latest.get("EMA_20", 0)

            ma_hierarchy_bull = (
                not pd.isna(ema_5)
                and not pd.isna(ema_10)
                and not pd.isna(ema_20)
                and ema_5 > ema_10 > ema_20
                and ema_5 > 0
            )

            if ma_hierarchy_bull and ma_bullish_count >= 2:
                trend_score += 4
                reasons.append(
                    f"Strong bullish MA alignment ({ma_bullish_count}/3 MAs above price)"
                )
            elif ma_bullish_count >= 2:
                trend_score += 2
                reasons.append(f"Bullish MA trend ({ma_bullish_count}/3 MAs)")
            elif ma_bullish_count == 0:
                trend_score -= 3
                reasons.append("Bearish MA alignment")

            # Trend Strength with safety checks
            trend_strength = latest.get("Trend_Strength", 0)
            if trend_strength is not None and not pd.isna(trend_strength):
                if trend_strength > 3:
                    trend_score += 2
                    reasons.append(f"Strong trend momentum ({trend_strength:.1f}%)")
                elif trend_strength < 1:
                    trend_score -= 1
                    reasons.append("Weak trend strength")

            # ADX Trend Confirmation
            adx = latest.get("ADX", 0)
            plus_di = latest.get("Plus_DI", 0)
            minus_di = latest.get("Minus_DI", 0)

            if (
                adx is not None
                and not pd.isna(adx)
                and adx > 25
                and plus_di is not None
                and not pd.isna(plus_di)
                and minus_di is not None
                and not pd.isna(minus_di)
            ):
                if plus_di > minus_di:
                    trend_score += 2
                    reasons.append(f"ADX confirms uptrend (ADX:{adx:.1f})")
                else:
                    trend_score -= 2
                    reasons.append(f"ADX confirms downtrend (ADX:{adx:.1f})")

            # 2. MOMENTUM ANALYSIS (25% weight)
            # Multi-timeframe RSI with safety checks
            rsi_scores = []
            for period in [9, 14, 21]:
                rsi = latest.get(f"RSI_{period}")
                if rsi is not None and not pd.isna(rsi):
                    if rsi < 30:
                        rsi_scores.append(3)
                    elif rsi < 40:
                        rsi_scores.append(1)
                    elif rsi > 70:
                        rsi_scores.append(-3)
                    elif rsi > 60:
                        rsi_scores.append(-1)
                    else:
                        rsi_scores.append(0)

            if rsi_scores:
                avg_rsi_score = sum(rsi_scores) / len(rsi_scores)
                momentum_score += avg_rsi_score

                main_rsi = latest.get("RSI_14")
                if main_rsi is not None and not pd.isna(main_rsi):
                    if main_rsi < 30:
                        reasons.append(f"RSI oversold ({main_rsi:.1f})")
                    elif main_rsi > 70:
                        reasons.append(f"RSI overbought ({main_rsi:.1f})")

            # Stochastic with safety checks
            stoch_k = latest.get("Stoch_K")
            stoch_d = latest.get("Stoch_D")
            if (
                stoch_k is not None
                and not pd.isna(stoch_k)
                and stoch_d is not None
                and not pd.isna(stoch_d)
            ):
                if stoch_k < 20 and stoch_d < 20:
                    momentum_score += 2
                    reasons.append(f"Stochastic oversold (K:{stoch_k:.1f})")
                elif stoch_k > 80 and stoch_d > 80:
                    momentum_score -= 2
                    reasons.append(f"Stochastic overbought (K:{stoch_k:.1f})")

                # Stochastic crossover
                prev_stoch_k = prev.get("Stoch_K")
                prev_stoch_d = prev.get("Stoch_D")
                if (
                    prev_stoch_k is not None
                    and not pd.isna(prev_stoch_k)
                    and prev_stoch_d is not None
                    and not pd.isna(prev_stoch_d)
                    and stoch_k > stoch_d
                    and prev_stoch_k <= prev_stoch_d
                    and stoch_k < 80
                ):
                    momentum_score += 1
                    reasons.append("Bullish stochastic crossover")

            # MFI (Money Flow Index)
            mfi = latest.get("MFI")
            if mfi is not None and not pd.isna(mfi):
                if mfi < 20:
                    momentum_score += 2
                    reasons.append(f"MFI oversold ({mfi:.1f})")
                elif mfi > 80:
                    momentum_score -= 2
                    reasons.append(f"MFI overbought ({mfi:.1f})")

            # 3. VOLUME ANALYSIS (20% weight)
            # Volume confirmation with safety checks
            current_volume = latest.get("Volume", 0)
            volume_ma_20 = latest.get("Volume_MA_20", current_volume)

            volume_ratio_20 = 1.0
            if volume_ma_20 > 0:
                volume_ratio_20 = current_volume / volume_ma_20

            if volume_ratio_20 > 2:
                volume_score += 3
                reasons.append(f"Very high volume ({volume_ratio_20:.1f}x average)")
            elif volume_ratio_20 > 1.5:
                volume_score += 2
                reasons.append(f"High volume ({volume_ratio_20:.1f}x average)")
            elif volume_ratio_20 < 0.5:
                volume_score -= 1
                reasons.append(f"Low volume ({volume_ratio_20:.1f}x average)")

            # Chaikin Money Flow
            cmf = latest.get("CMF")
            if cmf is not None and not pd.isna(cmf):
                if cmf > 0.1:
                    volume_score += 1
                    reasons.append("Positive money flow")
                elif cmf < -0.1:
                    volume_score -= 1
                    reasons.append("Negative money flow")

            # Volume breakout
            if latest.get("Volume_Breakout", False):
                volume_score += 2
                reasons.append("Volume breakout pattern")

            # 4. VOLATILITY/BB ANALYSIS (20% weight)
            bb_scores = []
            bb_info = []

            for period in self.bb_periods:
                percentb = latest.get(f"BB_PercentB_{period}")
                width = latest.get(f"BB_Width_{period}")

                if percentb is not None and not pd.isna(percentb):
                    if percentb <= 0:
                        bb_scores.append(3)
                    elif percentb < 20:
                        bb_scores.append(2)
                    elif percentb >= 100:
                        bb_scores.append(-3)
                    elif percentb > 80:
                        bb_scores.append(-2)
                    else:
                        bb_scores.append(0)

                    bb_info.append(f"BB{period}:%B:{percentb:.0f}")

                # Squeeze detection
                if width is not None and not pd.isna(width) and width < 10:
                    volatility_score += 1
                    reasons.append(f"BB{period} squeeze (width:{width:.1f}%)")

            if bb_scores:
                avg_bb_score = sum(bb_scores) / len(bb_scores)
                volatility_score += avg_bb_score

                # BB confluence
                unique_signals = len(
                    set(
                        [
                            1 if score > 1 else -1 if score < -1 else 0
                            for score in bb_scores
                        ]
                    )
                )
                if unique_signals == 1:
                    if avg_bb_score > 1:
                        volatility_score += 1
                        reasons.append("BB confluence - oversold across timeframes")
                    elif avg_bb_score < -1:
                        volatility_score -= 1
                        reasons.append("BB confluence - overbought across timeframes")

            # Multi-timeframe squeeze
            if latest.get("Multi_BB_Squeeze", False):
                volatility_score += 2
                reasons.append("Multi-timeframe BB squeeze - breakout imminent")

            # Calculate weighted final score
            final_score = (
                trend_score * 0.35
                + momentum_score * 0.25
                + volume_score * 0.20
                + volatility_score * 0.20
            )

            # Market regime adjustment
            market_regime = latest.get("Market_Regime", "RANGE")
            vol_regime = latest.get("Vol_Regime", "NORMAL")

            if market_regime == "STRONG_TREND":
                final_score *= 1.2
                reasons.append("Strong trending market - amplified signals")
            elif market_regime == "RANGE":
                final_score *= 0.8
                reasons.append("Range-bound market - reduced conviction")

            if vol_regime == "HIGH":
                final_score *= 0.9
                reasons.append("High volatility environment")

            # Generate final signal with enhanced thresholds
            if final_score >= 8:
                signal = "STRONG BUY"
            elif final_score >= 5:
                signal = "BUY"
            elif final_score <= -8:
                signal = "STRONG SELL"
            elif final_score <= -5:
                signal = "SELL"
            else:
                signal = "HOLD"

            # Calculate enhanced metrics
            previous_price = latest.get("Previous", latest["Close"])
            if previous_price > 0:
                change_pct = (latest["Close"] - previous_price) / previous_price * 100
            else:
                change_pct = 0

            return {
                "signal": signal,
                "score": final_score,
                "reasons": reasons,
                "trend_score": trend_score,
                "momentum_score": momentum_score,
                "volume_score": volume_score,
                "volatility_score": volatility_score,
                "market_regime": market_regime,
                "vol_regime": vol_regime,
                "rsi_14": latest.get("RSI_14", np.nan),
                "bb_confluence": latest.get("BB_Confluence", np.nan),
                "adx": latest.get("ADX", np.nan),
                "mfi": latest.get("MFI", np.nan),
                "volume_ratio": volume_ratio_20,
                "change_pct": change_pct,
                "bb_info": bb_info,
            }

        except Exception as e:
            logger.error(f"Error generating signals: {e}")
            return self._generate_fallback_signal(stock_data)

    def _generate_fallback_signal(self, stock_data):
        """Fallback signal for insufficient data or errors"""
        try:
            if stock_data.empty:
                change_pct = 0
                close_price = 0
            else:
                latest = stock_data.iloc[-1]
                previous_price = latest.get("Previous", latest.get("Close", 0))
                close_price = latest.get("Close", 0)

                if previous_price > 0:
                    change_pct = (close_price - previous_price) / previous_price * 100
                else:
                    change_pct = 0

            return {
                "signal": "HOLD",
                "score": 0,
                "reasons": ["Insufficient historical data for comprehensive analysis"],
                "trend_score": 0,
                "momentum_score": 0,
                "volume_score": 0,
                "volatility_score": 0,
                "market_regime": "UNKNOWN",
                "vol_regime": "UNKNOWN",
                "rsi_14": np.nan,
                "bb_confluence": np.nan,
                "adx": np.nan,
                "mfi": np.nan,
                "volume_ratio": np.nan,
                "change_pct": change_pct,
                "bb_info": [],
            }
        except Exception as e:
            logger.error(f"Error in fallback signal generation: {e}")
            return {
                "signal": "HOLD",
                "score": 0,
                "reasons": ["Error in analysis"],
                "trend_score": 0,
                "momentum_score": 0,
                "volume_score": 0,
                "volatility_score": 0,
                "market_regime": "UNKNOWN",
                "vol_regime": "UNKNOWN",
                "rsi_14": np.nan,
                "bb_confluence": np.nan,
                "adx": np.nan,
                "mfi": np.nan,
                "volume_ratio": np.nan,
                "change_pct": 0,
                "bb_info": [],
            }

    def calculate_enhanced_predictions(
        self, signal, score_components, change_pct, current_price, latest_data
    ):
        """Calculate enhanced predictions with comprehensive error handling"""
        try:
            predictions = {
                "predicted_gain": 0,
                "target_price": current_price,
                "stop_loss": current_price,
                "risk_level": "MEDIUM",
                "confidence": 50,
                "time_horizon": "3-5 days",
                "position_size": 1.0,
                "risk_reward_ratio": 1.0,
            }

            # Validate inputs
            if current_price <= 0:
                logger.warning("Invalid current price, using fallback predictions")
                return predictions

            signal_type = signal
            final_score = score_components.get("score", 0)
            market_regime = score_components.get("market_regime", "RANGE")
            vol_regime = score_components.get("vol_regime", "NORMAL")

            # Base prediction based on signal strength
            base_gain = 0
            base_confidence = 50
            base_time = 5

            if signal_type == "STRONG BUY":
                base_gain = 10 + max(0, (final_score - 8) * 0.8)
                base_confidence = 85
                base_time = 3
            elif signal_type == "BUY":
                base_gain = 6 + max(0, (final_score - 5) * 0.6)
                base_confidence = 70
                base_time = 4
            elif signal_type == "STRONG SELL":
                base_gain = -10 + min(0, (final_score + 8) * 0.8)
                base_confidence = 80
                base_time = 3
            elif signal_type == "SELL":
                base_gain = -6 + min(0, (final_score + 5) * 0.6)
                base_confidence = 65
                base_time = 4
            else:  # HOLD
                base_gain = final_score * 0.4
                base_confidence = 45
                base_time = 6

            # Market regime adjustments
            regime_multiplier = 1.0
            if market_regime == "STRONG_TREND":
                regime_multiplier = 1.3
                base_confidence += 10
                base_time = max(1, base_time - 1)
            elif market_regime == "TREND":
                regime_multiplier = 1.15
                base_confidence += 5
            elif market_regime == "RANGE":
                regime_multiplier = 0.8
                base_confidence -= 5
                base_time += 1

            # Volatility adjustments
            volatility_adj = 1.0
            if vol_regime == "HIGH":
                volatility_adj = 1.2
                base_confidence -= 10
                base_time = max(1, base_time - 1)
            elif vol_regime == "LOW":
                volatility_adj = 0.85
                base_confidence += 5
                base_time += 1

            # Component-specific adjustments
            trend_strength = min(abs(score_components.get("trend_score", 0)) / 4, 1.0)
            volume_confirmation = min(
                abs(score_components.get("volume_score", 0)) / 3, 1.0
            )
            momentum_strength = min(
                abs(score_components.get("momentum_score", 0)) / 3, 1.0
            )

            # Multi-factor adjustment
            factor_strength = (
                trend_strength + volume_confirmation + momentum_strength
            ) / 3
            multi_factor_adj = 0.8 + (factor_strength * 0.4)

            # Calculate final prediction
            final_gain = (
                base_gain * regime_multiplier * volatility_adj * multi_factor_adj
            )
            final_gain = max(min(final_gain, 20), -20)  # Cap at +/-20%

            # Calculate target and stop loss
            target_price = current_price * (1 + final_gain / 100)

            # Dynamic stop loss based on volatility
            volatility_factor = 0.05  # Default 5%
            if vol_regime == "HIGH":
                volatility_factor = 0.08
            elif vol_regime == "LOW":
                volatility_factor = 0.03

            if final_gain > 0:
                stop_loss = current_price * (1 - volatility_factor)
                risk_reward = (
                    abs(final_gain) / (volatility_factor * 100)
                    if volatility_factor > 0
                    else 1.0
                )
            else:
                stop_loss = current_price * (1 + volatility_factor)
                risk_reward = (
                    abs(final_gain) / (volatility_factor * 100)
                    if volatility_factor > 0
                    else 1.0
                )

            # Confidence calculation
            final_confidence = base_confidence
            final_confidence += volume_confirmation * 10
            final_confidence += trend_strength * 8
            final_confidence += momentum_strength * 7

            # ADX trend strength bonus
            adx = score_components.get("adx")
            if adx is not None and not pd.isna(adx) and adx > 30:
                final_confidence += 5

            final_confidence = max(min(final_confidence, 95), 25)

            # Risk assessment
            risk_factors = 0

            if abs(final_gain) > 15:
                risk_factors += 2
            elif abs(final_gain) > 10:
                risk_factors += 1

            if vol_regime == "HIGH":
                risk_factors += 2

            if final_confidence < 60:
                risk_factors += 1

            volume_ratio = score_components.get("volume_ratio", 1.0)
            if pd.isna(volume_ratio) or volume_ratio < 0.8:
                risk_factors += 1

            if market_regime == "RANGE":
                risk_factors += 1

            risk_levels = ["VERY LOW", "LOW", "MEDIUM", "HIGH", "VERY HIGH", "EXTREME"]
            risk_level = risk_levels[min(risk_factors, 5)]

            # Position sizing
            position_size = 1.0
            if final_confidence > 80 and risk_factors <= 2:
                position_size = 1.5
            elif final_confidence < 60 or risk_factors >= 4:
                position_size = 0.5
            elif risk_factors >= 3:
                position_size = 0.7

            # Time horizon
            final_time = max(min(base_time, 10), 1)

            predictions.update(
                {
                    "predicted_gain": round(final_gain, 2),
                    "target_price": round(target_price, 0),
                    "stop_loss": round(stop_loss, 0),
                    "risk_level": risk_level,
                    "confidence": round(final_confidence, 0),
                    "time_horizon": f"{final_time}-{final_time+2} days",
                    "position_size": round(position_size, 1),
                    "risk_reward_ratio": round(risk_reward, 1),
                }
            )

            return predictions

        except Exception as e:
            logger.error(f"Error calculating predictions: {e}")
            return {
                "predicted_gain": 0,
                "target_price": current_price,
                "stop_loss": current_price,
                "risk_level": "HIGH",
                "confidence": 25,
                "time_horizon": "5-7 days",
                "position_size": 0.5,
                "risk_reward_ratio": 1.0,
            }

    def analyze_enhanced_signals(self, df_today, df_historical=None):
        """Analyze stocks using enhanced multi-factor approach with improved error handling"""
        signals = []

        try:
            if df_today is None or df_today.empty:
                logger.error("No current day data provided")
                return pd.DataFrame()

            if df_historical is not None and not df_historical.empty:
                stock_groups = df_historical.groupby("StockCode")
            else:
                stock_groups = {}
                logger.warning("No historical data provided, analysis will be limited")

            logger.info(
                f"Analyzing {len(df_today)} stocks with enhanced multi-factor model..."
            )

            processed_count = 0
            for idx, stock in df_today.iterrows():
                try:
                    stock_code = stock.get("StockCode", f"UNKNOWN_{idx}")

                    # Get historical data for this stock
                    technical_analysis = None
                    if stock_code in stock_groups.groups:
                        stock_history = (
                            stock_groups.get_group(stock_code)
                            .sort_values("Date")
                            .reset_index(drop=True)
                        )

                        if len(stock_history) >= 15:  # Reduced minimum requirement
                            # Apply all indicators
                            stock_history = self.apply_all_indicators(stock_history)

                            if stock_history is not None:
                                # Generate enhanced signals
                                technical_analysis = self.generate_enhanced_signals(
                                    stock_history
                                )

                    # Fallback to basic analysis if no historical data
                    if technical_analysis is None:
                        technical_analysis = self._generate_fallback_signal(
                            pd.DataFrame([stock])
                        )

                    # Calculate enhanced predictions
                    predictions = self.calculate_enhanced_predictions(
                        technical_analysis["signal"],
                        technical_analysis,
                        technical_analysis["change_pct"],
                        stock.get("Close", 0),
                        stock,
                    )

                    # Calculate additional metrics
                    high_price = stock.get("High", stock.get("Close", 0))
                    low_price = stock.get("Low", stock.get("Close", 0))
                    previous_price = stock.get("Previous", stock.get("Close", 0))

                    if previous_price > 0:
                        daily_range = (high_price - low_price) / previous_price * 100
                    else:
                        daily_range = 0

                    if (high_price - low_price) > 0:
                        close_position = (stock.get("Close", 0) - low_price) / (
                            high_price - low_price
                        )
                    else:
                        close_position = 0.5

                    # Compile enhanced signal data
                    signal_data = {
                        "StockCode": stock_code,
                        "StockName": stock.get("StockName", "N/A"),
                        "Close": stock.get("Close", 0),
                        "Change": stock.get("Change", 0),
                        "Change_Pct": technical_analysis["change_pct"],
                        "Volume": stock.get("Volume", 0),
                        "Value": stock.get("Value", 0),
                        "High": high_price,
                        "Low": low_price,
                        "Date": stock.get("Date", pd.Timestamp.now()),
                        # Enhanced Signal Data
                        "Signal": technical_analysis["signal"],
                        "Total_Score": round(technical_analysis["score"], 2),
                        "Trend_Score": round(technical_analysis["trend_score"], 2),
                        "Momentum_Score": round(
                            technical_analysis["momentum_score"], 2
                        ),
                        "Volume_Score": round(technical_analysis["volume_score"], 2),
                        "Volatility_Score": round(
                            technical_analysis["volatility_score"], 2
                        ),
                        # Market Context
                        "Market_Regime": technical_analysis["market_regime"],
                        "Vol_Regime": technical_analysis["vol_regime"],
                        # Technical Indicators
                        "RSI_14": (
                            round(technical_analysis["rsi_14"], 1)
                            if not pd.isna(technical_analysis["rsi_14"])
                            else np.nan
                        ),
                        "BB_Confluence": (
                            round(technical_analysis["bb_confluence"], 1)
                            if not pd.isna(technical_analysis["bb_confluence"])
                            else np.nan
                        ),
                        "ADX": (
                            round(technical_analysis["adx"], 1)
                            if not pd.isna(technical_analysis["adx"])
                            else np.nan
                        ),
                        "MFI": (
                            round(technical_analysis["mfi"], 1)
                            if not pd.isna(technical_analysis["mfi"])
                            else np.nan
                        ),
                        "Volume_Ratio": (
                            round(technical_analysis["volume_ratio"], 2)
                            if not pd.isna(technical_analysis["volume_ratio"])
                            else np.nan
                        ),
                        # Enhanced Predictions
                        "Predicted_Gain_Pct": predictions["predicted_gain"],
                        "Target_Price": predictions["target_price"],
                        "Stop_Loss": predictions["stop_loss"],
                        "Risk_Level": predictions["risk_level"],
                        "Confidence_Score": predictions["confidence"],
                        "Time_Horizon": predictions["time_horizon"],
                        "Position_Size": predictions["position_size"],
                        "Risk_Reward_Ratio": predictions["risk_reward_ratio"],
                        # Additional metrics
                        "Daily_Range_Pct": round(daily_range, 2),
                        "Close_Position": round(close_position, 2),
                        "BB_Info": (
                            "; ".join(technical_analysis["bb_info"])
                            if technical_analysis["bb_info"]
                            else "N/A"
                        ),
                        "Reasons": "; ".join(technical_analysis["reasons"]),
                    }

                    signals.append(signal_data)
                    processed_count += 1

                    # Progress indicator
                    if processed_count % 50 == 0:
                        logger.info(
                            f"Processed {processed_count}/{len(df_today)} stocks..."
                        )

                except Exception as e:
                    logger.error(
                        f"Error analyzing {stock.get('StockCode', 'Unknown')}: {e}"
                    )
                    continue

            logger.info(f"Successfully processed {processed_count} stocks")
            return pd.DataFrame(signals)

        except Exception as e:
            logger.error(f"Critical error in analyze_enhanced_signals: {e}")
            return pd.DataFrame()

    def get_elite_opportunities(self, signals_df):
        """Get elite trading opportunities with comprehensive filtering"""
        try:
            if signals_df.empty:
                return None

            # Elite buy opportunities with improved filtering
            elite_buys = signals_df[
                (signals_df["Signal"].isin(["STRONG BUY", "BUY"]))
                & (signals_df["Confidence_Score"] > 75)
                & (signals_df["Risk_Reward_Ratio"] > 2.0)
                & (signals_df["Volume_Score"] > 1)
                & (signals_df["Trend_Score"] > 2)
                & (
                    (signals_df["Market_Regime"] == "STRONG_TREND")
                    | (signals_df["ADX"] > 25)
                )
            ].copy()

            if elite_buys.empty:
                return None

            return elite_buys.sort_values(
                ["Confidence_Score", "Risk_Reward_Ratio"], ascending=[False, False]
            )

        except Exception as e:
            logger.error(f"Error getting elite opportunities: {e}")
            return None

    def get_momentum_breakouts(self, signals_df):
        """Get high-momentum breakout candidates with improved filtering"""
        try:
            if signals_df.empty:
                return None

            breakouts = signals_df[
                (signals_df["Volume_Ratio"] > 1.8)
                & (signals_df["Momentum_Score"] > 2)
                & (signals_df["Change_Pct"] > 3)
                & (signals_df["Signal"].str.contains("BUY", na=False))
            ].copy()

            if breakouts.empty:
                return None

            return breakouts.sort_values(
                ["Volume_Ratio", "Momentum_Score"], ascending=[False, False]
            )

        except Exception as e:
            logger.error(f"Error getting momentum breakouts: {e}")
            return None

    def get_value_opportunities(self, signals_df):
        """Get oversold value opportunities with improved filtering"""
        try:
            if signals_df.empty:
                return None

            value_opps = signals_df[
                (signals_df["Signal"].isin(["STRONG BUY", "BUY"]))
                & (signals_df["RSI_14"] < 35)
                & (signals_df["BB_Confluence"] < 25)
                & (signals_df["Volume_Score"] > 0)
                & (signals_df["Volatility_Score"] > 1)
            ].copy()

            if value_opps.empty:
                return None

            return value_opps.sort_values(
                ["Volatility_Score", "Volume_Score"], ascending=[False, False]
            )

        except Exception as e:
            logger.error(f"Error getting value opportunities: {e}")
            return None

    def create_comprehensive_excel_report(self, signals_df, output_file):
        """Create comprehensive Excel report with enhanced error handling"""
        try:
            if signals_df.empty:
                logger.warning("No data to export to Excel")
                return

            with pd.ExcelWriter(output_file, engine="xlsxwriter") as writer:
                # Main signals sheet
                signals_df.to_excel(
                    writer, sheet_name="All Enhanced Signals", index=False
                )

                # Elite opportunities
                elite_opps = self.get_elite_opportunities(signals_df)
                if elite_opps is not None and len(elite_opps) > 0:
                    elite_opps.to_excel(
                        writer, sheet_name="Elite Opportunities", index=False
                    )

                # Momentum breakouts
                momentum_breakouts = self.get_momentum_breakouts(signals_df)
                if momentum_breakouts is not None and len(momentum_breakouts) > 0:
                    momentum_breakouts.to_excel(
                        writer, sheet_name="Momentum Breakouts", index=False
                    )

                # Value opportunities
                value_opps = self.get_value_opportunities(signals_df)
                if value_opps is not None and len(value_opps) > 0:
                    value_opps.to_excel(
                        writer, sheet_name="Value Opportunities", index=False
                    )

                # Market regime analysis
                try:
                    regime_summary = (
                        signals_df.groupby(["Market_Regime", "Vol_Regime"])
                        .agg(
                            {
                                "StockCode": "count",
                                "Confidence_Score": "mean",
                                "Total_Score": "mean",
                                "Risk_Reward_Ratio": "mean",
                            }
                        )
                        .round(2)
                    )
                    regime_summary.to_excel(writer, sheet_name="Market Regime Analysis")
                except Exception as e:
                    logger.warning(f"Could not create regime analysis: {e}")

                # Performance by signal type
                try:
                    signal_performance = (
                        signals_df.groupby("Signal")
                        .agg(
                            {
                                "StockCode": "count",
                                "Predicted_Gain_Pct": "mean",
                                "Confidence_Score": "mean",
                                "Risk_Reward_Ratio": "mean",
                                "Position_Size": "mean",
                            }
                        )
                        .round(2)
                    )
                    signal_performance.to_excel(writer, sheet_name="Signal Performance")
                except Exception as e:
                    logger.warning(f"Could not create signal performance analysis: {e}")

            logger.info("Comprehensive Excel report created successfully")

        except Exception as e:
            logger.error(f"Error creating comprehensive Excel file: {e}")
            # Fallback - create basic Excel file
            try:
                signals_df.to_excel(output_file, index=False)
                logger.info("Basic Excel file created as fallback")
            except Exception as e2:
                logger.error(f"Even fallback Excel creation failed: {e2}")

    def display_elite_opportunities(self, signals_df, latest_date):
        """Display elite trading opportunities with improved formatting"""
        try:
            print(f"\n" + "🏆" * 60)
            print("ELITE TRADING OPPORTUNITIES")
            print("🏆" * 60)

            # Elite opportunities
            elite_opps = self.get_elite_opportunities(signals_df)
            if elite_opps is not None and len(elite_opps) > 0:
                print(f"\n⭐ ELITE HIGH-CONVICTION TRADES ({len(elite_opps)} stocks):")
                for i, (_, stock) in enumerate(elite_opps.head(5).iterrows(), 1):
                    print(f"   {i}. {stock['StockCode']} - {stock['Signal']}")
                    print(
                        f"      💰 Price: Rp{stock['Close']:,.0f} → Target: Rp{stock['Target_Price']:,.0f} ({stock['Predicted_Gain_Pct']:+.1f}%)"
                    )
                    print(
                        f"      📊 Score: {stock['Total_Score']:.1f} | Confidence: {stock['Confidence_Score']:.0f}% | R/R: {stock['Risk_Reward_Ratio']:.1f}"
                    )
                    print(
                        f"      📈 Trend: {stock['Trend_Score']:.1f} | Volume: {stock['Volume_Score']:.1f} | Regime: {stock['Market_Regime']}"
                    )
            else:
                print("\n⭐ No elite opportunities found with current criteria")

            # Momentum breakouts
            momentum_breakouts = self.get_momentum_breakouts(signals_df)
            if momentum_breakouts is not None and len(momentum_breakouts) > 0:
                print(
                    f"\n🚀 MOMENTUM BREAKOUT PLAYS ({len(momentum_breakouts)} stocks):"
                )
                for i, (_, stock) in enumerate(
                    momentum_breakouts.head(5).iterrows(), 1
                ):
                    vol_ratio = stock.get("Volume_Ratio", 0)
                    momentum_score = stock.get("Momentum_Score", 0)
                    print(
                        f"   {i}. {stock['StockCode']} - Vol: {vol_ratio:.1f}x, Mom: {momentum_score:.1f}"
                    )
                    print(
                        f"      💹 Change: {stock['Change_Pct']:+.1f}% | Target: {stock['Predicted_Gain_Pct']:+.1f}%"
                    )
            else:
                print("\n🚀 No momentum breakouts found")

            # Value opportunities
            value_opps = self.get_value_opportunities(signals_df)
            if value_opps is not None and len(value_opps) > 0:
                print(f"\n💎 VALUE REVERSAL OPPORTUNITIES ({len(value_opps)} stocks):")
                for i, (_, stock) in enumerate(value_opps.head(5).iterrows(), 1):
                    rsi_14 = stock.get("RSI_14", np.nan)
                    bb_confluence = stock.get("BB_Confluence", np.nan)

                    rsi_info = (
                        f"RSI: {rsi_14:.1f}" if not pd.isna(rsi_14) else "RSI: N/A"
                    )
                    bb_info = (
                        f"BB: {bb_confluence:.0f}"
                        if not pd.isna(bb_confluence)
                        else "BB: N/A"
                    )
                    print(f"   {i}. {stock['StockCode']} - {rsi_info}, {bb_info}")
                    print(
                        f"      🔄 Oversold reversal potential: {stock['Predicted_Gain_Pct']:+.1f}%"
                    )
            else:
                print("\n💎 No value opportunities found")

        except Exception as e:
            logger.error(f"Error displaying elite opportunities: {e}")

    def display_enhanced_results(self, signals_df, latest_date):
        """Display enhanced analysis results with improved error handling"""
        try:
            print(f"\n" + "=" * 100)
            print(
                f"ENHANCED MULTI-FACTOR IDX ANALYSIS - {latest_date.strftime('%Y-%m-%d')}"
            )
            print("=" * 100)

            # Market regime summary
            try:
                regime_counts = signals_df["Market_Regime"].value_counts()
                vol_counts = signals_df["Vol_Regime"].value_counts()

                print(f"\n📊 Market Environment Analysis:")
                print(f"   Market Regimes: {dict(regime_counts)}")
                print(f"   Volatility Regimes: {dict(vol_counts)}")
            except Exception as e:
                logger.warning(f"Could not display market regime summary: {e}")

            # Signal distribution with scores
            print(f"\n📈 Signal Distribution & Quality:")
            signal_order = ["STRONG BUY", "BUY", "HOLD", "SELL", "STRONG SELL"]

            for signal_type in signal_order:
                try:
                    stocks = signals_df[signals_df["Signal"] == signal_type]
                    if len(stocks) > 0:
                        avg_confidence = stocks["Confidence_Score"].mean()
                        avg_rr = stocks["Risk_Reward_Ratio"].mean()
                        high_confidence = len(stocks[stocks["Confidence_Score"] > 75])

                        print(f"\n🔸 {signal_type} ({len(stocks)} stocks):")
                        print(
                            f"   Avg Confidence: {avg_confidence:.1f}% | Avg R/R: {avg_rr:.1f} | High Confidence: {high_confidence}"
                        )

                        # Show top stocks for each signal
                        for i, (_, stock) in enumerate(stocks.head(3).iterrows(), 1):
                            trend_info = f"T:{stock['Trend_Score']:.1f}"
                            momentum_info = f"M:{stock['Momentum_Score']:.1f}"
                            volume_info = f"V:{stock['Volume_Score']:.1f}"
                            volatility_info = f"Vol:{stock['Volatility_Score']:.1f}"

                            print(
                                f"      {i}. {stock['StockCode']} - Score: {stock['Total_Score']:.1f} "
                                f"[{trend_info}|{momentum_info}|{volume_info}|{volatility_info}]"
                            )
                            print(
                                f"         Price: Rp{stock['Close']:,.0f} → {stock['Predicted_Gain_Pct']:+.1f}% "
                                f"(Conf: {stock['Confidence_Score']:.0f}%, Risk: {stock['Risk_Level']})"
                            )
                except Exception as e:
                    logger.warning(f"Error displaying signal type {signal_type}: {e}")

            # Summary statistics
            try:
                print(f"\n📋 Enhanced Analysis Summary:")
                total_analyzed = len(signals_df)
                high_confidence = len(signals_df[signals_df["Confidence_Score"] > 75])
                strong_signals = len(
                    signals_df[signals_df["Signal"].str.contains("STRONG")]
                )
                trending_market = len(
                    signals_df[signals_df["Market_Regime"].str.contains("TREND")]
                )
                high_vol = len(signals_df[signals_df["Vol_Regime"] == "HIGH"])

                print(f"   Total Stocks Analyzed: {total_analyzed}")
                print(
                    f"   High Confidence Signals (>75%): {high_confidence} ({high_confidence/total_analyzed*100:.1f}%)"
                )
                print(
                    f"   Strong Buy/Sell Signals: {strong_signals} ({strong_signals/total_analyzed*100:.1f}%)"
                )
                print(
                    f"   Stocks in Trending Markets: {trending_market} ({trending_market/total_analyzed*100:.1f}%)"
                )
                print(
                    f"   High Volatility Stocks: {high_vol} ({high_vol/total_analyzed*100:.1f}%)"
                )

                # Risk-reward analysis
                print(f"\n⚖️ Risk-Reward Profile:")
                excellent_rr = len(signals_df[signals_df["Risk_Reward_Ratio"] > 3])
                good_rr = len(signals_df[signals_df["Risk_Reward_Ratio"].between(2, 3)])
                avg_position_size = signals_df["Position_Size"].mean()

                print(f"   Excellent Risk/Reward (>3:1): {excellent_rr} stocks")
                print(f"   Good Risk/Reward (2-3:1): {good_rr} stocks")
                print(f"   Average Recommended Position Size: {avg_position_size:.1f}x")
            except Exception as e:
                logger.warning(f"Error displaying summary statistics: {e}")

        except Exception as e:
            logger.error(f"Error in display_enhanced_results: {e}")

    def run_enhanced_analysis(self, days_to_analyze=35):
        """Run the enhanced multi-factor analysis with comprehensive error handling"""
        try:
            print(f"🚀 ENHANCED IDX TECHNICAL ANALYSIS - Multi-Factor Model")
            print("=" * 70)
            print("🔧 Advanced Features:")
            print("   ✅ Multi-timeframe Bollinger Bands (10, 20, 30 periods)")
            print("   ✅ Adaptive Moving Average (AMA) with efficiency ratio")
            print("   ✅ ADX trend strength & market regime detection")
            print("   ✅ Money Flow Index (MFI) & Chaikin Money Flow")
            print("   ✅ Multi-RSI periods (9, 14, 21) with StochRSI")
            print("   ✅ Stochastic, Williams %R, CCI oscillators")
            print(
                "   ✅ Weighted scoring: Trend(35%) + Momentum(25%) + Volume(20%) + Volatility(20%)"
            )
            print("   ✅ Risk-adjusted position sizing & dynamic stop losses")
            print("   ✅ Volatility regime classification")
            print("   ✅ Enhanced error handling and data validation")
            print("   ✅ Improved division-by-zero protection")

            # Load historical data
            historical_data = self.load_multiple_days(days_to_analyze)
            if historical_data is None:
                logger.error("Could not load any data files")
                return None

            # Get the most recent day's data
            latest_date = historical_data["Date"].max()
            today_data = historical_data[historical_data["Date"] == latest_date]

            if today_data.empty:
                logger.error("No data for the latest date")
                return None

            logger.info(
                f"Analyzing {len(today_data)} stocks from {latest_date.strftime('%Y-%m-%d')}"
            )
            logger.info(f"Using {days_to_analyze} days of historical data for context")

            # Generate enhanced signals
            signals_df = self.analyze_enhanced_signals(today_data, historical_data)

            if signals_df.empty:
                logger.error("No signals generated")
                return None

            # Sort results by total score
            try:
                signal_order = ["STRONG BUY", "BUY", "HOLD", "SELL", "STRONG SELL"]
                signals_df["Signal_Order"] = signals_df["Signal"].map(
                    {sig: i for i, sig in enumerate(signal_order)}
                )
                signals_df = signals_df.sort_values(
                    ["Signal_Order", "Total_Score"], ascending=[True, False]
                )
            except Exception as e:
                logger.warning(f"Error sorting results: {e}")

            # Save comprehensive results
            try:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                output_file = os.path.join(
                    self.signals_folder, f"enhanced_idx_multifactor_{timestamp}.xlsx"
                )

                self.create_comprehensive_excel_report(signals_df, output_file)
                logger.info(f"Report saved: {output_file}")
            except Exception as e:
                logger.error(f"Error saving report: {e}")

            # Display results
            try:
                self.display_elite_opportunities(signals_df, latest_date)
                self.display_enhanced_results(signals_df, latest_date)
            except Exception as e:
                logger.error(f"Error displaying results: {e}")

            print(
                f"\n💾 Analysis complete! Report saved with {len(signals_df)} analyzed stocks"
            )
            print("✅ Enhanced multi-factor analysis complete with improved error handling!")

            return signals_df

        except Exception as e:
            logger.error(f"Critical error in run_enhanced_analysis: {e}")
            return None


# Main execution with improved error handling
if __name__ == "__main__":
    try:
        print("🚀 ENHANCED IDX Technical Analysis - Multi-Factor Model (FIXED)")
        print("=" * 70)

        analyzer = EnhancedIDXTechnicalAnalyzer()

        # Ask user for parameters with validation
        try:
            days_input = (
                input("\nDays of historical data to analyze? (default 35, min 15): ")
                or "35"
            )
            days = int(days_input)
            days = max(days, 15)  # Reduced minimum from 30 to 15
        except ValueError:
            logger.warning("Invalid input, using default of 35 days")
            days = 35

        results = analyzer.run_enhanced_analysis(days_to_analyze=days)

        if results is not None and not results.empty:
            print(
                f"\n✅ Enhanced analysis complete! {len(results)} stocks analyzed with multi-factor model."
            )
            print("🎯 Key improvements in this fixed version:")
            print("   • Fixed division-by-zero errors with safe_divide() function")
            print("   • Improved pandas Series.replace() compatibility")
            print("   • Enhanced error handling and data validation")
            print("   • Better missing data handling with fallback values")
            print("   • Robust calculation with null value protection")
            print("   • Comprehensive logging system")
            print("   • Reduced minimum data requirements")
        else:
            print("\n❌ No results generated. Please check the logs for errors.")
            print("💡 Troubleshooting tips:")
            print("   • Ensure Excel files are in 'Stock Summary Folder'")
            print("   • Check file format and column names")
            print("   • Verify data quality and completeness")

    except Exception as e:
        logger.error(f"Critical application error: {e}")
        print(f"\n❌ Application error: {e}")
        print("Please check the logs for detailed error information.")