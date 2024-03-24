#!/usr/bin/env python3

import pandas as pd
import numpy as np
import warnings
import time
import multiprocessing
from pathlib import Path
from psycopg2.extensions import register_adapter, AsIs
from data.database.postgres import create_connection, insert_values_in_database
from data.data_extraction.stocks_price import ExtractStockData
from data.data_extraction.calculate_ratios import extractRatio, ratio
from data.data_extraction.weather import ExtractWeatherData
from data.data_extraction.news_tweets import getTweets, ExtractNewsData, getSentiment
from data.data_extraction.technical_indicators import CalcTechIndicators
from helpers.logger import script_logger, log_execution_time
from config.config import location_list_IND, CONFIG

warnings.filterwarnings("ignore")
register_adapter(np.int64, AsIs)


@log_execution_time(script_logger)
def StartMining(IndexFile):

    # Read Symbol From CSV FILE
    Symbols = IndexFile["Symbol"].to_list()
    StocksList = [i+".ns" for i in Symbols]
    CompanyName = [i.replace(" ", "-").lower().strip('.')
                   for i in IndexFile['Company Name'].values.tolist()]
    id = IndexFile['ID'].to_list()

    RATIO_DF = pd.DataFrame()
    STOCK_DF = pd.DataFrame()
    TECH_DF = pd.DataFrame()
    TWEET_DF = pd.DataFrame()
    TWEET_SENTIMENT = pd.DataFrame()
    NEWS_DF = pd.DataFrame()
    NEWS_SENTIMENT = pd.DataFrame()

    for i, stock in enumerate(StocksList):
        start_time_stock = time.perf_counter()
        script_logger.info(
            f'Daily data extraction for stock {stock} started')
        company_name = StocksList[i].strip('.ns')

        # extract ratios and merge it with final ratios dataframe
        ratio_table = extractRatio(CompanyName[i], Symbols[i], id[i])
        ratio_df = ratio([5, 8], ratio_table, Symbols[i])
        RATIO_DF = pd.concat([RATIO_DF, ratio_df], axis=0)

        # Extract StockData and merge it with final stockprice dataframe
        stock_df = ExtractStockData(stock)
        STOCK_DF = pd.concat([STOCK_DF, stock_df.iloc[[-1]]], axis=0)

        # Calculate Technical Indicators and merge it with final technical indicators dataframe
        tech_df = CalcTechIndicators(stock_df)
        TECH_DF = pd.concat([TECH_DF, tech_df], axis=0)

        # Extract tweets and merge it with final tweets dataframe
        tweet_df = getTweets(company_name)
        TWEET_DF = pd.concat([TWEET_DF, tweet_df], axis=0)

        # calculate tweets sentiment and merge it with final tweets sentiment dataframe
        tweet_sentiment = getSentiment(tweet_df, col_name='tweets')
        TWEET_SENTIMENT = pd.concat([TWEET_SENTIMENT, tweet_sentiment], axis=0)

        # Extract NewsData and merge it with final news dataframe
        news_df = ExtractNewsData(CompanyName[i], Symbols[i])
        NEWS_DF = pd.concat([NEWS_DF, news_df], axis=0)

        # calculate news sentiment and merge it with final news sentiment dataframe
        news_sentiment = getSentiment(news_df, col_name='news')
        NEWS_SENTIMENT = pd.concat([NEWS_SENTIMENT, news_sentiment], axis=0)

        script_logger.info(
            f'Daily data extraction for stock {stock} is done! Execution time is {time.perf_counter() - start_time_stock:.2f} seconds')

    conn = create_connection()

    insert_values_in_database(conn=conn, df=RATIO_DF,
                              tablename='ratios', country='INDIA')
    insert_values_in_database(conn=conn, df=STOCK_DF,
                              tablename='stocks_prices', country='INDIA')
    insert_values_in_database(
        conn=conn, df=TECH_DF, tablename='technical_indicators', country='INDIA')
    insert_values_in_database(conn=conn, df=TWEET_DF,
                              tablename='tweets', country='INDIA')
    insert_values_in_database(
        conn=conn, df=TWEET_SENTIMENT, tablename='tweets_sentiment', country='INDIA')
    insert_values_in_database(conn=conn, df=NEWS_DF,
                              tablename='news', country='INDIA')
    insert_values_in_database(
        conn=conn, df=NEWS_SENTIMENT, tablename='news_sentiment', country='INDIA')

    return True


def IND_dailydata_extraction():

    conn = create_connection()
    # Extract WeatherData
    weatherDF1, weatherDF = ExtractWeatherData(location_list_IND, 'INDIA')

    insert_values_in_database(conn=conn, df=weatherDF1,
                              tablename='weather', country='INDIA')
    insert_values_in_database(conn=conn,
                              df=weatherDF, tablename='weather_preprocessed', country='INDIA')

    IndexFile = pd.read_csv(
        Path(CONFIG.get('directory').get('extraction').get('Nifty_50')))

    # Split the DataFrame into chunks
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(IndexFile) // num_processes
    chunks = [IndexFile.iloc[i:i+chunk_size]
              for i in range(0, len(IndexFile), chunk_size)]

    # Create multiprocessing pool
    pool = multiprocessing.Pool(processes=num_processes)

    # Execute the processing task in parallel
    pool.map(StartMining, chunks)

    # Clean up
    pool.close()
    pool.join()
