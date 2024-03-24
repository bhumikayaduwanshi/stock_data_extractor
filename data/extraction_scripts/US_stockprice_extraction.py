#!/usr/bin/env python3

import numpy as np
import warnings
import multiprocessing
from psycopg2.extensions import register_adapter, AsIs
from data.database.postgres import create_connection, insert_values_in_database
from data.data_extraction.stocks_price import ExtractMinuteStockPrice
from config.config import US_25, US_symbols

warnings.filterwarnings("ignore")
register_adapter(np.int64, AsIs)


def US_stockprice_extraction():

    num_processes = multiprocessing.cpu_count()
    chunk_size = len(US_symbols) // num_processes
    chunks = [US_symbols[i:i+chunk_size]
              for i in range(0, len(US_symbols), chunk_size)]

    pool = multiprocessing.Pool(processes=num_processes)

    result1 = ExtractMinuteStockPrice(US_25)
    results = pool.map(ExtractMinuteStockPrice, chunks)

    conn = create_connection()
    insert_values_in_database(conn, result1,
                              'daily_minute_prices', 'US 25 stocks')

    for result in results:
        insert_values_in_database(conn, result,
                                  'ext_daily_minute_prices', 'US Stocks')

    pool.close()
    pool.join()
