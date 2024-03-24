# from finrl.config import dir_config
import pandas as pd
import numpy as np
import re
from requests_html import HTMLSession
from datetime import date
from helpers.logger import error_logger


def ratio(index_list, table, symbol, date=date.today()):
    try:
        nf = pd.DataFrame(
            columns=['netprofit', 'eps', 'opm', 'roe', 'pbt', 'pe'])
        for k in index_list:
            columns = [table[k].pop(table[k].columns[0]).values]
            values = [table[k].pop(table[k].columns[0]).values]
            ratio = pd.DataFrame(values, columns=columns).T
            ratio.reset_index(inplace=True)
            ratio.dropna(inplace=True)
            ratio['level_0'] = [
                re.sub('[^a-zA-Z0-9]', '', x.lower().strip()) for x in ratio.level_0.tolist()]
            ratio.set_index(['level_0'], inplace=True)
            ratio = ratio.T
            cols = ['netprofit', 'eps', 'opm', 'roe', 'pbt', 'pe']
            ratio.drop(
                columns=[col for col in ratio.columns if col not in cols], inplace=True)
            if '-' in ratio.values:
                break
            nf = pd.concat([nf, ratio], axis=1)
            nf = nf.replace('--', 0, regex=True)
            nf.dropna(axis=1, inplace=True)

        nf.insert(0, 'asset_id', symbol)
        nf.insert(1, 'date', date)
        nf.reset_index(drop=True)
        return nf

    except Exception as e:
        error_logger.exception(
            'Exception occurred while extracting ratios data(INDIA)!')
        nf = pd.DataFrame(
            columns=['netprofit', 'eps', 'opm', 'roe', 'pbt', 'pe'])
        nf.insert(0, 'asset_id', symbol)
        nf.insert(1, 'date', date)
        return nf


def extractRatio(company_name, stock, id):
    try:
        URL = 'https://www.bseindia.com/stock-share-price/{}/{}/{}/financials-results/'.format(
            company_name, stock, id)
        session = HTMLSession()
        resp = session.get(URL)
        resp.html.render(timeout=30)
        table = pd.read_html(resp.html.html)
        return table
    except Exception as e:
        error_logger.exception(
            'Exception occurred while extracting ratios(INDIA)!')
        return []


def extractDailyRatioUS(symbol, date=date.today()):
    try:
        columns = {'Operating Margin': 'opm', 'PE Ratio': 'pe',
                   'EPS (Basic)': 'eps', 'Pretax Income': 'pbt', 'Net Income': 'netprofit', 'Return on Equity (ROE)': 'roe'}
        session = HTMLSession()
        resp = session.get(
            f'https://stockanalysis.com/stocks/{symbol.lower()}/financials/ratios/')
        resp.html.render()
        ratios = pd.read_html(resp.html.html)
        ratios = np.array(ratios)
        Shape = ratios.shape
        ratios = ratios.reshape(Shape[1], Shape[2])
        ratios = pd.DataFrame(ratios)
        ratios.set_index([0], inplace=True, drop=True)
        ratios = ratios.T
        ratios.drop(columns=[
                    column for column in ratios.columns if column not in columns.keys()], inplace=True)
        ratios = ratios.iloc[1, :]

        resp = session.get(
            f'https://stockanalysis.com/stocks/{symbol.lower().replace("-",".")}/financials/?p=quarterly')
        resp.html.render()
        financials = pd.read_html(resp.html.html)
        financials = np.array(financials)
        shape = financials.shape
        financials = financials.reshape(shape[1], shape[2])
        financials = pd.DataFrame(financials)
        financials.set_index([0], inplace=True, drop=True)
        financials = financials.T
        financials.drop(columns=[
                        column for column in financials.columns if column not in columns], inplace=True)
        financials = financials.iloc[1, :]
        ratioDF = pd.DataFrame(ratios.append(financials)).T
        ratioDF.rename(columns=columns, inplace=True)
        ratioDF.reset_index(drop=True, inplace=True)
        ratioDF = ratioDF.replace('%', '', regex=True)
        ratioDF.insert(0, 'asset_id', symbol)
        ratioDF.insert(1, 'date', date)
        ratioDF.reset_index(drop=True)
        return ratioDF

    except Exception as e:
        error_logger.exception(
            'Exception occurred while extracting ratio(US)!')

        return pd.DataFrame(
            columns=['netprofit', 'eps', 'opm', 'roe', 'pbt', 'pe'])
