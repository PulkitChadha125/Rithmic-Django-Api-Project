import os
import asyncio
import time
from rithmic import RithmicOrderApi, RithmicTickerApi, RithmicEnvironment
from rithmic.callbacks.callbacks import CallbackManager

ticker_api = None

# Set the environment variable for the credentials path
os.environ['RITHMIC_CREDENTIALS_PATH'] = r"D:\Desktop\python projects\RythmicApi"

# Step 1: Set up the Rithmic environment
env = RithmicEnvironment.RITHMIC_PAPER_TRADING
print("env: ", env)

# Step 2: Initialize Callback Manager
callback_manager = CallbackManager()
print("callback_manager: ", callback_manager)

# Step 3: Initialize APIs asynchronously
async def subscribe_to_market_data():
    global ticker_api
    loop = asyncio.get_event_loop()
    print("loop: ", loop)

    try:
        # Initialize the ticker API
        ticker_api = RithmicTickerApi(env=env, callback_manager=callback_manager, loop=loop)
        print("Initialized ticker API successfully: ", ticker_api)

        # Step 4: Subscribe to Market Data for given symbols
        symbols = [("ES.CME", "CME"), ("YM.CBOT", "CBOT")]
        print("symbols: ", symbols)

        # Subscribe to each symbol asynchronously
        for symbol, exchange in symbols:
            try:
                response = await ticker_api.subscribe_market_data(symbol=symbol, exchange=exchange)
                if response.success:
                    print(f"Successfully subscribed to {symbol} on {exchange}")
                else:
                    print(f"Failed to subscribe to {symbol} on {exchange}: {response.error_message}")
            except Exception as e:
                print(f"Error subscribing to {symbol} on {exchange}: {e}")

    except Exception as e:
        print(f"Error initializing Ticker API: {e}")

# Run the subscription using asyncio.run to ensure the event loop starts and runs correctly
asyncio.run(subscribe_to_market_data())

# Step 5: Wait for Ticks to be received
print("Waiting for tick data...")
time.sleep(30)  # Extended wait time for data to arrive

# Step 6: Poll for tick data
try:
    for symbol, exchange in [("ES.CME", "CME"), ("YM.CBOT", "CBOT")]:
        tick_data = ticker_api.get_market_data(symbol=symbol, exchange=exchange)
        print(f"Polling tick_data for {symbol}: {tick_data}")
        if tick_data:
            last_price = tick_data[-1].trade_price  # Assuming you want the last trade price
            print(f"Last price for {symbol}: {last_price}")
        else:
            print(f"No data received for {symbol}")
except Exception as e:
    print(f"Error retrieving market data: {e}")

print("Polling completed.")
