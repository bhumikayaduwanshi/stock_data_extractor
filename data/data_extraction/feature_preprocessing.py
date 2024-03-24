import pandas as pd
import numpy as np
import itertools
import datetime as dt
from functools import reduce


def data_split(df, start, end, target_date_col="date"):
    """
    split the dataset into training or testing using date
    :param data: (df) pandas dataframe, start, end
    :return: (df) pandas dataframe
    """
    # start = datetime.datetime.strptime(start, '%Y-%m-%d')
    # end = datetime.datetime.strptime(end, '%Y-%m-%d')
    data = df[(df[target_date_col] >= start) & (df[target_date_col] <= end)]
    data = data.sort_values([target_date_col, "tic"], ignore_index=True)
    data.index = data[target_date_col].factorize()[0]
    return data


def preprocess(df, tech_list):
    df.rename(columns={"symbol": "tic"}, inplace=True)
    tech_list.extend(['date', 'tic'])
    df.columns = df.columns.str.lower()
    # df.drop_duplicates(subset=['tic', 'date'], keep='last', inplace = True)
    df.drop(columns=[
            col for col in df.columns if col not in tech_list], axis=1, inplace=True)
    unique_tickers = df["tic"].unique().tolist()
    date_arr = df['date'].unique().tolist()
    date_tic_arr = list(itertools.product(date_arr, unique_tickers))

    pf = pd.DataFrame(date_tic_arr, columns=["date", "tic"])
    pf['date'] = pd.to_datetime(pf['date'])
    pf = pd.merge(pf, df, on=["date", "tic"], how="left")
    pf = pf[pf['date'].isin(df['date'])]
    pf = pf.sort_values(by=['date', 'tic'])
    pf['date'] = pd.to_datetime(pf['date'], utc=False)
    pf['date'] = pd.to_datetime(pf['date']).dt.date
    pf = pf.fillna(0)
    pf = pf.replace('%', '', regex=True)
    tech_list.remove('tic')
    tech_list.remove('date')
    pf[tech_list] = pf[tech_list].astype(float)
    pf.sort_values(by=['date', 'tic'], ignore_index=True)
    return pf


def calculate_turbulence(df):
    """calculate turbulence index based on dow 30"""
    # can add other market assets
    df_price_pivot = df.pivot(index="Date", columns="asset_id", values="Close")
    # use returns to calculate turbulence
    df_price_pivot = df_price_pivot.pct_change()
    unique_date = df.Date.unique()
    # give it a start window for calculation
    start = 252
    turbulence_index = [0] * start
    count = 0
    for i in range(start, len(unique_date)):
        current_price = df_price_pivot[df_price_pivot.index == unique_date[i]]
        hist_price = df_price_pivot[[n in unique_date[0:i]
                                     for n in df_price_pivot.index]]
        cov_temp = hist_price.cov()
        current_temp = (current_price - np.mean(hist_price, axis=0))
        temp = current_temp.values.dot(
            np.linalg.inv(cov_temp)).dot(current_temp.values.T)
        if temp > 0:
            count += 1
            if count > 2:
                turbulence_temp = temp[0][0]
            else:
                # avoid large outlier because of the calculation just begins
                turbulence_temp = 0
        else:
            turbulence_temp = 0
        turbulence_index.append(turbulence_temp)

    try:
        turbulence_index = pd.DataFrame(
            {"Date": df_price_pivot.index, "turbulence": turbulence_index}
        )
    except ValueError:
        raise Exception("Turbulence information could not be added.")
    return turbulence_index


def combine_dataframes(dataframes_list):
    final_dataframe = reduce(lambda  left,right: pd.merge(left,right,on=['asset_id', 'date'], how='outer'), dataframes_list)
    return final_dataframe
