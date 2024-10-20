from rithmic import  RithmicOrderApi, RithmicEnvironment, RithmicTickerApi
import inspect
print("RithmicOrderApi: ",inspect.signature(RithmicOrderApi.__init__))
print("RithmicTickerApi: ",inspect.signature(RithmicTickerApi.__init__))




# from rithmic import RithmicOrderApi, RithmicEnvironment, RithmicTickerApi
# import time
#
# username = '23136617-DEMO'
# password = '110211082'
# app_name = 'joat:JA23136617'
# app_version = '10'
#
# api = RithmicOrderApi(
#     username=username,
#     password=password,
#     app_name=app_name,
#     app_version=app_version,
#     env=RithmicEnvironment.RITHMIC_PAPER_TRADING
# )
#
# # Initialize ticker API for market data
# ticker_api = RithmicTickerApi(
#     username=username,
#     password=password,
#     app_name=app_name,
#     app_version=app_version,
#     env=RithmicEnvironment.RITHMIC_PAPER_TRADING
# )
#
# # Set the security code and exchange
# security_code = 'ESZ3'  # Example symbol
# exchange_code = 'CME'
#
# # Stream market data
# tick_data = ticker_api.stream_market_data(security_code, exchange_code)
#
# # Wait for some ticks to accumulate
# while tick_data.tick_count < 5:
#     time.sleep(0.1)
#
# # Get the last price from the tick data
# last_price = tick_data.tick_dataframe.iloc[-1].close
# print(f"Last price: {last_price}")
