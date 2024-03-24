API_KEY = '12eb4643-4a1c-415f-8f17-37c4d0ceaed0'

from com.dakshata.autotrader.api.AutoTrader import AutoTrader
from pydantic import BaseModel, Field
from typing import Optional

class ApiParams(BaseModel):
    api_key : str

class OrderParamsRegular(BaseModel):
    pseudo_account : str
    exchange : str
    symbol : str
    tradeType : str
    orderType : str
    productType : str
    quantity : int
    price : float
    triggerPrice : float = Field(default=0.0)

class OrderParamsCover(BaseModel):
    pseudo_account : str
    exchange : str
    symbol : str
    tradeType : str
    orderType : str
    quantity : int
    price : int
    triggerPrice : float

class OrderParamsBracket(BaseModel):
    pseudo_account : str
    exchange : str
    symbol : str
    tradeType : str
    orderType : str
    quantity : int
    price : float
    triggerPrice : float
    target : float
    stoploss : float
    trailingStoploss : float = 0.0

class OrderParamsAdvanced(BaseModel):
    variety : str
    pseudo_account : str
    exchange : str
    symbol : str
    tradeType : str
    orderType : str
    productType : str
    quantity : float
    price : float
    triggerPrice : float
    target : float
    stoploss : float
    trailingStoploss : float
    disclosedQuantity : str
    validity : str
    amo : str
    strategyId : str
    comments : str
    publisherId : str

class OrderParamsPlatform(BaseModel):
    pseudo_account : str
    platform_id : str

class OrderParamsChildPlatform(BaseModel):
    pseudo_account : str
    platform_id : str

class OrderParamsCancel(BaseModel):
    pseudo_account : str

class OrderParamsModify(BaseModel):
    pseudo_account : str
    platform_id : str
    order_type : Optional[str] = None
    quantity : Optional[str] = None
    price : Optional[str] = None
    trigger_price : Optional[str] = None


def autotrader_orders(ordertype, api_key, order_kwargs):   
    try:
        autotrader = AutoTrader.create_instance(api_key['api_key'], AutoTrader.SERVER_URL)
        match ordertype:
            case 'Regular':   
                return autotrader.place_regular_order(**order_kwargs)
            
            case 'Cover':
                return autotrader.place_cover_order(**order_kwargs)
            
            case 'Bracket':
                return autotrader.place_bracket_order(**order_kwargs)
                
            case 'Advanced':
                return autotrader.place_advanced_order(**order_kwargs)
                
            case 'Cancel_order':
                return autotrader.cancel_order_by_platform_id(**order_kwargs)
            
            case 'Cancel_child_order':
                return autotrader.cancel_child_orders_by_platform_id(**order_kwargs)
            
            case 'Cancel_all_order':
                return autotrader.cancel_all_orders(**order_kwargs)
            
            case 'Modify_order':
                return autotrader.modify_order_by_platform_id(**order_kwargs)
        
    except Exception as e:
        return f'Exception Occured. {e}'



