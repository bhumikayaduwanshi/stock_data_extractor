#!/usr/bin/env python3

import pandas as pd
import numpy as np
import warnings
import multiprocessing
from config.config import CONFIG
from pathlib import Path
from psycopg2.extensions import register_adapter, AsIs
from data.database.postgres import create_connection, insert_values_in_database
from data.data_extraction.stocks_price import ExtractMinuteStockPrice

warnings.filterwarnings("ignore")
register_adapter(np.int64, AsIs)


def IND_stockprice_extraction():

    IndexFile = pd.read_csv(
        Path(CONFIG.get('directory').get('extraction').get('Nifty_50')))
    StocksList = [i+".ns" for i in IndexFile.Symbol.to_list()]

    dataframe = pd.read_csv(
        Path(CONFIG.get('directory').get('extraction').get('Nifty_500')))
    Symbols = [i+'.ns' for i in dataframe['SYMBOL \n'].to_list()]

    num_processes = multiprocessing.cpu_count()
    chunk_size = len(Symbols) // num_processes
    chunks1 = [StocksList[i:i+chunk_size]
               for i in range(0, len(StocksList), chunk_size)]
    chunks2 = [Symbols[i:i+chunk_size]
               for i in range(0, len(Symbols), chunk_size)]

    pool = multiprocessing.Pool(processes=num_processes)
    results1 = pool.map(ExtractMinuteStockPrice, chunks1)
    results2 = pool.map(ExtractMinuteStockPrice, chunks2)

    conn = create_connection()
    insert_values_in_database(conn, results1[0],
                              'daily_minute_prices', 'INDIA Nifty_50')

    for result in results2:
        insert_values_in_database(conn, result,
                                  'ext_daily_minute_prices', 'INDIA Nifty_500')

    pool.close()
    pool.join()
