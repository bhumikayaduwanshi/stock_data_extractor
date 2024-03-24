#!/usr/bin/env python3

import warnings
import pandas as pd
import numpy as np
import time
import multiprocessing
from datetime import date
from psycopg2.extensions import register_adapter, AsIs
from data.data_extraction.technical_indicators import CalcTechIndicators
from data.data_extraction.news_tweets import getTweets, ExtractNewsData, getSentiment
from data.data_extraction.weather import ExtractWeatherData
from data.data_extraction.calculate_ratios import extractDailyRatioUS
from data.data_extraction.stocks_price import ExtractStockData
from data.database.postgres import create_connection, insert_values_in_database
from helpers.logger import script_logger
from config.config import US_25, location_list_US

warnings.filterwarnings("ignore")
register_adapter(np.int64, AsIs)


def StartMiningUS(StocksList):
    script_logger.info('DAILY DATA EXTRACTION FOR US STOCKS STARTED!')

    RATIO_DF = pd.DataFrame()
    STOCK_DF = pd.DataFrame()
    TECH_DF = pd.DataFrame()
    TWEET_DF = pd.DataFrame()
    TWEET_SENTIMENT = pd.DataFrame()
    NEWS_DF = pd.DataFrame()
    NEWS_SENTIMENT = pd.DataFrame()

    for i in range(len(StocksList)):
        start_time_stock = time.perf_counter()
        script_logger.info(
            f'Daily data extraction for stock {StocksList[i]} started')

        # calculate ratios and merge with final ratios dataframe
        ratio_df = extractDailyRatioUS(StocksList[i])
        RATIO_DF = pd.concat([RATIO_DF, ratio_df], axis=0)

        # Extract StockData and merge with final stockprice dataframe
        stock_df = ExtractStockData(StocksList[i])
        STOCK_DF = pd.concat([STOCK_DF, stock_df.iloc[[-1]]], axis=0)

        # Calculate Technical Indicators and merge with final technical indicators dataframe
        tech_df = CalcTechIndicators(stock_df)
        TECH_DF = pd.concat([TECH_DF, tech_df], axis=0)

        # Extract tweets and merge with final tweets dataframe
        tweet_df = getTweets(StocksList[i])
        TWEET_DF = pd.concat([TWEET_DF, tweet_df], axis=0)

        # calculate tweets sentiment and merge with final tweets sentiment dataframe
        tweet_sentiment = getSentiment(tweet_df, col_name='tweets')
        TWEET_SENTIMENT = pd.concat([TWEET_SENTIMENT, tweet_sentiment], axis=0)

        # Extract NewsData and merge with final news dataframe
        news_df = ExtractNewsData(StocksList[i], StocksList[i])
        NEWS_DF = pd.concat([NEWS_DF, news_df], axis=0)

        # calculate news sentiment and merge with final news sentiment dataframe
        news_sentiment = getSentiment(news_df, col_name='news')
        NEWS_SENTIMENT = pd.concat([NEWS_SENTIMENT, news_sentiment], axis=0)

        script_logger.info(
            f'Daily data extraction for stock {StocksList[i]} is done! Execution time is {time.perf_counter() - start_time_stock:.2f} seconds')

    insert_values_in_database(conn=create_connection(), df=RATIO_DF, tablename='ratios', country='US')
    insert_values_in_database(conn=create_connection(), df=STOCK_DF, tablename='stocks_prices', country='US')
    insert_values_in_database(conn=create_connection(), df=TECH_DF, tablename='technical_indicators', country='US')
    insert_values_in_database(conn=create_connection(), df=TWEET_DF, tablename='tweets', country='US')
    insert_values_in_database(conn=create_connection(), df=TWEET_SENTIMENT, tablename='tweets_sentiment', country='US')
    insert_values_in_database(conn=create_connection(), df=NEWS_DF, tablename='news', country='US')
    insert_values_in_database(conn=create_connection(), df=NEWS_SENTIMENT, tablename='news_sentiment', country='US')
    
    return True


def US_dailydata_extraction():

    # Extract WeatherData
    weatherDF1, weatherDF = ExtractWeatherData(location_list_US, 'US')
    insert_values_in_database(conn=create_connection(
    ), df=weatherDF1, tablename='weather', country='US')
    insert_values_in_database(conn=create_connection(
    ), df=weatherDF, tablename='weather_preprocessed', country='US')
    # Split the DataFrame into chunks
    num_processes = multiprocessing.cpu_count()
    chunk_size = len(US_25) // num_processes
    chunks = [US_25[i:i+chunk_size] for i in range(0, len(US_25), chunk_size)]

    # Create multiprocessing pool
    pool = multiprocessing.Pool(processes=num_processes)

    # Execute the processing task in parallel
    pool.map(StartMiningUS, chunks)

    # Clean up
    pool.close()
    pool.join()
