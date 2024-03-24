from datetime import date, timedelta
import yfinance as yf
import pandas_ta as ta
import pandas as pd
from helpers.logger import error_logger

# Calculate Technical Indicators


def CalcTechIndicators(df):
    df.columns = df.columns.str.lower()

    def calc_10Days(df):
        # MOM
        df.ta.mom(close="close", length=1, append=True)
        df["MOM_1"] = df["MOM_1"].fillna(0)
        # # WMA 10
        df.ta.wma(close="close", length=10, append=True)
        df["WMA_10"] = df["WMA_10"].fillna(df["close"])
        # # EMA 10
        df.ta.ema(close="close", length=10, append=True)
        df["EMA_10"] = df["EMA_10"].fillna(df["close"])
        # Supertrend
        df.ta.supertrend(high="high", low="low", close="close",
                         length=10, multiplier=2.0, append=True)
        df["SUPERT_10_2.0"] = df["SUPERT_10_2.0"].fillna(0)
        df["SUPERTd_10_2.0"] = df["SUPERTd_10_2.0"].fillna(0)
        df["SUPERTl_10_2.0"] = df["SUPERTl_10_2.0"].fillna(0)
        df["SUPERTs_10_2.0"] = df["SUPERTs_10_2.0"].fillna(0)
        # VWMA 10
        df.ta.vwma(close="close", length=10, volume="volume", append=True)
        df["VWMA_10"] = df["VWMA_10"].fillna(df["close"])
        # SMA 10
        df.ta.sma(close="close", length=10, append=True)
        df["SMA_10"] = df["SMA_10"].fillna(df["close"])
        # Candlestick Patterns
        df.ta.cdl_pattern(open="open", high="high", low="low",
                          close="close", name="all", append=True)
        return df

    def calc_20Days(df):
        # Stochastic
        df.ta.stoch(high="high", low="low", close="close", append=True)
        df["STOCHk_14_3_3"] = df["STOCHk_14_3_3"].fillna(df["close"])
        df["STOCHd_14_3_3"] = df["STOCHd_14_3_3"].fillna(df["close"])
        # Correlation Trend Indicator
        df.ta.cti(close="close", length=14, append=True)
        df["CTI_14"] = df["CTI_14"].fillna(0)
        # RSI
        df.ta.rsi(close="close", length=14, append=True)
        df["RSI_14"] = df["RSI_14"].fillna(0)
        # MFI
        df.ta.mfi(high="high", low="low", close="close",
                  volume="volume", length=14, append=True)
        df["MFI_14"] = df["MFI_14"].fillna(0)
        # Aroon
        df.ta.aroon(high="high", low="low", length=14, scalar=100, append=True)
        df["AROOND_14"] = df["AROOND_14"].fillna(0)
        df["AROONU_14"] = df["AROONU_14"].fillna(0)
        df["AROONOSC_14"] = df["AROONOSC_14"].fillna(0)
        # # WMA 20
        df.ta.wma(close="close", length=20, append=True)
        df["WMA_20"] = df["WMA_20"].fillna(df["close"])
        # EMA 20
        df.ta.ema(close="close", length=20, append=True)
        df["EMA_20"] = df["EMA_20"].fillna(df["close"])
        # NATR
        df.ta.natr(high="high", low="low",
                   close="close", length=20, append=True)
        df["NATR_20"] = df["NATR_20"].fillna(0)
        # VWMA 20
        df.ta.vwma(close="close", length=20, volume="volume", append=True)
        df["VWMA_20"] = df["VWMA_20"].fillna(df["close"])
        # SMA 20
        df.ta.sma(close="close", length=20, append=True)
        df["SMA_20"] = df["SMA_20"].fillna(df["close"])
        return df

    def calc_50Days(df):
        # MACD
        df.ta.macd(close="close", append=True)
        df["MACD_12_26_9"] = df["MACD_12_26_9"].fillna(0)
        df["MACDh_12_26_9"] = df["MACDh_12_26_9"].fillna(0)
        df["MACDs_12_26_9"] = df["MACDs_12_26_9"].fillna(0)
        # BBands
        df.ta.bbands(close="close", periods=20, stds=2, ddof=0, append=True)
        df["BBL_5_2.0"] = df["BBL_5_2.0"].fillna(df["close"])
        df["BBM_5_2.0"] = df["BBM_5_2.0"].fillna(df["close"])
        df["BBU_5_2.0"] = df["BBU_5_2.0"].fillna(df["close"])
        df["BBB_5_2.0"] = df["BBB_5_2.0"].fillna(df["close"])
        df["BBP_5_2.0"] = df["BBP_5_2.0"].fillna(df["close"])
        # # WMA 50
        df.ta.wma(close="close", length=50, append=True)
        df["WMA_50"] = df["WMA_50"].fillna(df["close"])
        # EMA 50
        df.ta.ema(close="close", length=50, append=True)
        df["EMA_50"] = df["EMA_50"].fillna(df["close"])
        # VWMA 50
        df.ta.vwma(close="close", length=50, volume="volume", append=True)
        df["VWMA_50"] = df["VWMA_50"].fillna(df["close"])
        # SMA 50
        df.ta.sma(close="close", length=50, append=True)
        df["SMA_50"] = df["SMA_50"].fillna(df["close"])
        return df

    try:
        count = len(df)
        if count <= 10:
            df["MOM_1"] = 0
            df["WMA_10"] = 0
            df["EMA_10"] = 0
            df["SUPERT_10_2.0"] = 0
            df["SUPERTd_10_2.0"] = 0
            df["SUPERTl_10_2.0"] = 0
            df["SUPERTs_10_2.0"] = 0
            df["VWMA_10"] = 0
            df["SMA_10"] = 0
            df['cdl_doji_10_0.1'] = 0
            df['cdl_inside'] = 0
            df["STOCHk_14_3_3"] = 0
            df["STOCHd_14_3_3"] = 0
            df["CTI_14"] = 0
            df["RSI_14"] = 0
            df["MFI_14"] = 0
            df["AROOND_14"] = 0
            df["AROONU_14"] = 0
            df["AROONOSC_14"] = 0
            df["WMA_20"] = 0
            df["EMA_20"] = 0
            df["NATR_20"] = 0
            df["VWMA_20"] = 0
            df["SMA_20"] = 0
            df["MACD_12_26_9"] = 0
            df["MACDh_12_26_9"] = 0
            df["MACDs_12_26_9"] = 0
            df["BBL_5_2.0"] = 0
            df["BBM_5_2.0"] = 0
            df["BBU_5_2.0"] = 0
            df["BBB_5_2.0"] = 0
            df["BBP_5_2.0"] = 0
            df["WMA_50"] = 0
            df["EMA_50"] = 0
            df["VWMA_50"] = 0
            df["SMA_50"] = 0
        elif count < 20 and count > 10:
            df = calc_10Days(df)
            df["STOCHk_14_3_3"] = 0
            df["STOCHd_14_3_3"] = 0
            df["CTI_14"] = 0
            df["RSI_14"] = 0
            df["MFI_14"] = 0
            df["AROOND_14"] = 0
            df["AROONU_14"] = 0
            df["AROONOSC_14"] = 0
            df["WMA_20"] = 0
            df["EMA_20"] = 0
            df["NATR_20"] = 0
            df["VWMA_20"] = 0
            df["SMA_20"] = 0
            df["MACD_12_26_9"] = 0
            df["MACDh_12_26_9"] = 0
            df["MACDs_12_26_9"] = 0
            df["BBL_5_2.0"] = 0
            df["BBM_5_2.0"] = 0
            df["BBU_5_2.0"] = 0
            df["BBB_5_2.0"] = 0
            df["BBP_5_2.0"] = 0
            df["WMA_50"] = 0
            df["EMA_50"] = 0
            df["VWMA_50"] = 0
            df["SMA_50"] = 0
        elif count < 50 and count >= 20:
            df = calc_10Days(df)
            df = calc_20Days(df)
            df["MACD_12_26_9"] = 0
            df["MACDh_12_26_9"] = 0
            df["MACDs_12_26_9"] = 0
            df["BBL_5_2.0"] = 0
            df["BBM_5_2.0"] = 0
            df["BBU_5_2.0"] = 0
            df["BBB_5_2.0"] = 0
            df["BBP_5_2.0"] = 0
            df["WMA_50"] = 0
            df["EMA_50"] = 0
            df["VWMA_50"] = 0
            df["SMA_50"] = 0
        elif count >= 50:
            df = calc_10Days(df)
            df = calc_20Days(df)
            df = calc_50Days(df)
        df = df.iloc[[-1]].reset_index(drop=True)
        df.columns = [i.replace('.', '_') for i in df.columns.to_list()]
        df.drop(columns=['date', 'open', 'low',
                'high', 'close', 'volume'], inplace=True)
        asset_name = df.pop('asset_id')
        df.insert(0, 'asset_id', asset_name)
        df.insert(1, 'date', date.today())
        df.columns = df.columns.str.lower()
        return df

    except Exception as e:
        error_logger.exception(
            'Exception occurred while calculating technical indicators!')
        return None
