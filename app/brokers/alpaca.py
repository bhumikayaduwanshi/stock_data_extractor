APCA_API_BASE_URL = 'https://alpaca.markets'
APCA_API_KEY_ID = 'PKYS6JRY7W8OBGBPLI41'
APCA_API_SECRET_KEY = '8zUkhNun0Hs7qVhh0Gqgyd1b0Iej5xzjfMXP2sPs'



from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest , LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetAssetsRequest
from alpaca.trading.enums import AssetClass
import alpaca_trade_api as tradeapi

api = tradeapi.REST()


API_KEY = "<Your API Key>"
SECRET_KEY = "<Your Secret Key>"

trading_client = TradingClient(API_KEY, SECRET_KEY, paper=True)

#get account information
account = trading_client.get_account()

# Setting parameters for our buy order
market_order_data = MarketOrderRequest(
                      symbol="BTC/USD",
                      qty=1,
                      side=OrderSide.BUY,
                      time_in_force=TimeInForce.GTC
                  )

market_order = trading_client.submit_order(market_order_data)

positions = trading_client.get_all_positions()


# Check if our account is restricted from trading.
if account.trading_blocked:
    print('Account is currently restricted from trading.')


# Check our current balance vs. our balance at the last market close
balance_change = float(account.equity) - float(account.last_equity)
print(f'Today\'s portfolio balance change: ${balance_change}')

# search for US equities
search_params = GetAssetsRequest(asset_class=AssetClass.US_EQUITY)

assets = trading_client.get_all_assets(search_params)

# search for AAPL
aapl_asset = trading_client.get_asset('AAPL')

if aapl_asset.tradable:
    print('We can trade AAPL.')

# preparing market order
market_order_data = MarketOrderRequest(
                    symbol="SPY",
                    qty=0.023,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                    )

# Market order
market_order = trading_client.submit_order(
                order_data=market_order_data
               )

# preparing limit order
limit_order_data = LimitOrderRequest(
                    symbol="BTC/USD",
                    limit_price=17000,
                    notional=4000,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.FOK
                   )

# Limit order
limit_order = trading_client.submit_order(
                order_data=limit_order_data
              )

# submit shorts
trading_client = TradingClient('api-key', 'secret-key', paper=True)

# preparing orders
market_order_data = MarketOrderRequest(
                    symbol="SPY",
                    qty=1,
                    side=OrderSide.SELL,
                    time_in_force=TimeInForce.GTC
                    )

# Market order
market_order = trading_client.submit_order(
                order_data=market_order_data
               )

# Check on our position
symbol='AAPL'
position = api.get_position(symbol)
if int(position.qty) < 0:
  print(f'Short position open for {symbol}')

# using client order id 
trading_client = TradingClient('api-key', 'secret-key', paper=True)

# preparing orders
market_order_data = MarketOrderRequest(
                    symbol="SPY",
                    qty=0.023,
                    side=OrderSide.BUY,
                    time_in_force=TimeInForce.DAY
                    )

# Market order
market_order = trading_client.submit_order(
                order_data=market_order_data
               )

# Get our order using its Client Order ID.
my_order = api.get_order_by_client_order_id('my_first_order')
print('Got order #{}'.format(my_order.id))


# submitting bracket orders
symbol = 'AAPL'
symbol_bars = api.get_barset(symbol, 'minute', 1).df.iloc[0]
symbol_price = symbol_bars[symbol]['close']

# We could buy a position and add a stop-loss and a take-profit of 5 %
api.submit_order(
    symbol=symbol,
    qty=1,
    side='buy',
    type='market',
    time_in_force='gtc',
    order_class='bracket',
    stop_loss={'stop_price': symbol_price * 0.95,
               'limit_price':  symbol_price * 0.94},
    take_profit={'limit_price': symbol_price * 1.05}
)

# We could buy a position and just add a stop loss of 5 % (OTO Orders)
api.submit_order(
    symbol=symbol,
    qty=1,
    side='buy',
    type='market',
    time_in_force='gtc',
    order_class='oto',
    stop_loss={'stop_price': symbol_price * 0.95}
)

# We could split it to 2 orders. first buy a stock,
# and then add the stop/profit prices (OCO Orders)
api.submit_order(symbol, 1, 'buy', 'limit', 'day', symbol_price)

# wait for it to buy position and then
api.submit_order(
    symbol=symbol,
    qty=1,
    side='sell',
    type='limit',
    time_in_force='gtc',
    order_class='oco',
    stop_loss={'stop_price': symbol_price * 0.95},
    take_profit={'limit_price': symbol_price * 1.05}
)

# submitting trailing stop orders 
# Submit a market order to buy 1 share of Apple at market price
api.submit_order(
    symbol='AAPL',
    qty=1,
    side='buy',
    type='market',
    time_in_force='gtc'
)

# Submit a trailing stop order to sell 1 share of Apple at a
# trailing stop of
api.submit_order(
    symbol='AAPL',
    qty=1,
    side='sell',
    type='trailing_stop',
    trail_price=1.00,  # stop price will be hwm - 1.00$
    time_in_force='gtc',
)

# Alternatively, you could use trail_percent:
api.submit_order(
    symbol='AAPL',
    qty=1,
    side='sell',
    type='trailing_stop',
    trail_percent=1.0,  # stop price will be hwm*0.99
    time_in_force='gtc',
)

# retrieve all orders
# Get the last 100 closed orders
closed_orders = api.list_orders(
    status='closed',
    limit=100,
    nested=True  # show nested multi-leg orders
)

# Get only the closed orders for a particular stock
closed_aapl_orders = [o for o in closed_orders if o.symbol == 'AAPL']
print(closed_aapl_orders)

# listen for updates to orders
conn = tradeapi.stream2.StreamConn()

# Handle updates on an order you've given a Client Order ID.
# The r indicates that we're listening for a regex pattern.
client_order_id = r'my_client_order_id'
@conn.on(client_order_id)
async def on_msg(conn, channel, data):
    # Print the update to the console.
    print("Update for {}. Event: {}.".format(client_order_id, data['event']))

# Start listening for updates.
conn.run(['trade_updates'])

# view open positions
# Get our position in AAPL.
aapl_position = api.get_position('AAPL')

# Get a list of all of our positions.
portfolio = api.list_positions()

# Print the quantity of shares for each position.
for position in portfolio:
    print("{} shares of {}".format(position.qty, position.symbol))