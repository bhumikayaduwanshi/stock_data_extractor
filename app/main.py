# Modules
from fastapi import FastAPI, Request
import pandas as pd
from pydantic import BaseModel
import uvicorn

# Libraries
from model.stablebaselines3.models import DRLAgent
from config.config import INDICATORS
from model.env_stock_trading.env_stocktrading import StockTradingEnv
from brokers.Autotrader import *

app = FastAPI()


def get_values_from_dict(input_dictionary):
    num_stock_shares = []

    for key in input_dictionary:
        num_stock_shares.append(input_dictionary[key])

    account_balance = num_stock_shares.pop(0)

    return num_stock_shares, account_balance


class StocksData(BaseModel):
    account_balance: int
    ADANIPORTS: int
    APOLLOHOSP: int
    ASIANPAINT: int
    AXISBANK: int
    BAJAJ_AUTO: int
    BAJFINANCE: int
    BAJAJFINSV: int
    BPCL: int
    BHARTIARTL: int
    BRITANNIA: int


@app.post("/placeorder")
async def ordertype(ordertype: str, order_params:  OrderParamsRegular |
                    OrderParamsBracket | OrderParamsAdvanced | OrderParamsCancel |
                    OrderParamsChildPlatform | OrderParamsCover |
                    OrderParamsModify | OrderParamsPlatform,
                    api_key: ApiParams):

    order_params = order_params.dict()
    status = autotrader_orders(ordertype, api_key.dict(), order_params)
    return status


@app.post("/predict")
async def predict(input_parameters: StocksData):

    num_stock_shares, account_balance = get_values_from_dict(
        input_parameters.dict())

    df = pd.read_csv('finalDF.csv')
    data = df.iloc[-20:, :]
    data.replace('--', 0, inplace=True)
    data = data.sort_values(['date', "tic"], ignore_index=True)
    data.index = data['date'].factorize()[0]
    stock_dimension = len(df.tic.unique())
    state_space = 1 + 2*stock_dimension + len(INDICATORS)*stock_dimension
    buy_cost_list = sell_cost_list = [0.001] * stock_dimension
    num_stock_shares = [0] * stock_dimension

    env_kwargs = {
        "hmax": 50,
        "initial_amount": account_balance,
        "num_stock_shares": num_stock_shares,
        "buy_cost_pct": buy_cost_list,
        "sell_cost_pct": sell_cost_list,
        "state_space": state_space,
        "stock_dim": stock_dimension,
        "tech_indicator_list": INDICATORS,
        "action_space": stock_dimension,
        "reward_scaling": 1e-4
    }

    env = StockTradingEnv(df=data, turbulence_threshold=100,
                          risk_indicator_col='turbulence', **env_kwargs)
    cwd = 'trained_model/PPO.zip'

    portfolio_value, actions, rewards = DRLAgent.DRL_prediction_load_from_file(
        'ppo', env, cwd, deterministic=True)

    return portfolio_value.to_dict(), actions.to_dict(), rewards.to_dict(), num_stock_shares, account_balance


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
