import yfinance as yf
from datetime import timedelta, date
import pandas as pd
from helpers.logger import error_logger, script_logger, log_execution_time


def ExtractStockData(stock, start_date=date.today(), end_date=date.today()):
    if start_date == end_date:
        start_date = date.today() - timedelta(days=400)
        # doesnt include end date in final data
        end_date = date.today() + timedelta(days=1)
    try:
        df = yf.download(stock, start=start_date, end=end_date)
        df = df.reset_index()
        df.insert(1, 'asset_id', stock.strip('.ns'))
        df.drop(columns=['Adj Close'], inplace=True)
        df.columns = df.columns.str.lower()
        return df

    except Exception as e:
        error_logger.exception(
            f'Stockprice extraction failed for symbol {stock}')
        return None


@log_execution_time(script_logger)
def ExtractMinuteStockPrice(stock):
    pf = pd.DataFrame()
    for i in stock:
        try:
            df = yf.download(i, interval='1m', period='1d')
            df = df.reset_index()
            df.insert(1, 'asset_id', i.strip('.ns'))
            df.rename(columns={'Adj Close': 'adj_close'}, inplace=True)
            df.columns = df.columns.str.lower()
            if df.empty == False:
                pf = pd.concat([pf, df.iloc[[-1]]], axis=0)
            return pf

        except Exception as e:
            error_logger.exception(
                f'Stockprice extraction failed for symbol {i}')
            return pd.DataFrame()




###########################