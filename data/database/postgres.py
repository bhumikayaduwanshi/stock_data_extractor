# import packages
import pandas as pd
from sqlalchemy import create_engine, text
from config.config import CONFIG
from helpers.logger import script_logger, error_logger, log_execution_time


def create_connection():
    try:
        username = CONFIG.get('database').get('user')
        password = CONFIG.get('database').get('password')
        host = CONFIG.get('database').get('host')
        port = CONFIG.get('database').get('port')
        database = CONFIG.get('database').get('database')
        conn = create_engine(
            f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
        return conn
    except Exception as e:
        error_logger.exception('Could not connect to database!')


def read_table_from_database(conn, table_name, country=None, start_date=None, end_date=None):
    try:
        with conn.begin() as engine:
            if country == None:
                dataframe = pd.read_sql(text(f"SELECT * FROM public.{table_name} WHERE date BETWEEN '{start_date}' AND '{end_date}'"), engine)
                dataframe.drop_duplicates(keep='first', inplace=True)
            elif start_date == None:
                dataframe = pd.read_sql(text(f"SELECT * FROM public.{table_name} WHERE country = '{country}'"), engine)
                dataframe.drop_duplicates(keep='first', inplace=True)
            else:
                dataframe = pd.read_sql(text(f"SELECT * FROM public.{table_name} WHERE date BETWEEN '{start_date}' AND '{end_date}' AND country = '{country}'"), engine)
                dataframe.drop_duplicates(keep='first', inplace=True)
        return dataframe
    except Exception as e:
        error_logger.exception(f'Could not read table {table_name}!')


@log_execution_time(script_logger)
def insert_values_in_database(conn, df, tablename, country):
    script_logger.info(
        f'Data insertion for {tablename} started for {country}!')
    try:
        with conn.begin() as engine:
            df.to_sql(tablename, con=engine, if_exists='append',
                      schema='public', index=False, chunksize=200)
    except Exception as e:
        error_logger.exception(
            f'Data insertion into table {tablename} aborted for {country}!')
