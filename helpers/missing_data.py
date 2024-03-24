import exchange_calendars as tc
import pandas as pd
import itertools
from datetime import datetime
from data.database.postgres import create_connection, read_table_from_database


def get_trading_days(start: str, end: str, exchange: str) -> list:
    nyse = tc.get_calendar(exchange)
    df = nyse.sessions_in_range(start, end)
    trading_days = []
    for day in df:
        trading_days.append(str(day)[:10])

    return trading_days


def find_missing_dates(start_date, end_date, symbols, table_name, country):
    if country == 'INDIA':
        trading_days = get_trading_days(start_date, end_date, 'BSE')
        trading_days = [datetime.strptime(
            date, "%Y-%m-%d").date() for date in trading_days]
        print(trading_days)
        date_symbol_array = list(itertools.product(trading_days, symbols))

        if table_name == 'weather_preprocessed':
            dataframe = read_table_from_database(conn=create_connection(), table_name=table_name, country=country, start_date=start_date, end_date=end_date)
            date_array = list(dataframe['date'].values)
            print(date_array)
            missing_dates_array = [
                i for i in trading_days if i not in date_array]

        else:
            dataframe = read_table_from_database(conn=create_connection(), table_name=table_name, start_date=start_date, end_date=end_date)
            dataframe = dataframe.loc[dataframe['asset_id'].isin(symbols)]
            dataframe_array = list(
                zip(dataframe['date'].values, dataframe['asset_id'].values))
            missing_dates_array = [
                i for i in date_symbol_array if i not in dataframe_array]

    elif country == 'US':
        trading_days = get_trading_days(start_date, end_date, 'NYSE')
        trading_days = [datetime.strptime(
            date, "%Y-%m-%d").date() for date in trading_days]
        date_symbol_array = list(itertools.product(trading_days, symbols))

        if table_name == 'weather_preprocessed':
            dataframe = read_table_from_database(conn=create_connection(), table_name=table_name, country=country, start_date=start_date, end_date=end_date)
            date_array = list(dataframe['date'].values)
            missing_dates_array = [
                i for i in trading_days if i not in date_array]

        else:
            dataframe = read_table_from_database(conn=create_connection(), table_name=table_name, start_date=start_date, end_date=end_date)
            dataframe = dataframe.loc[dataframe['asset_id'].isin(symbols)]
            dataframe_array = list(
                zip(dataframe['date'].values, dataframe['asset_id'].values))
            missing_dates_array = [
                i for i in date_symbol_array if i not in dataframe_array]

    return missing_dates_array
