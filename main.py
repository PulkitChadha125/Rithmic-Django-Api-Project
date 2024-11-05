# main.py
import asyncio
from SampleMD import fetch_market_data
from SampleOrder import execute_order

async def main():
    uri = "wss://rituz00100.rithmic.com:443"
    system_name = "Rithmic Test"
    user_id = "pulkitchadhaqwerty@gmail.com"
    password = "UBiKMPuY"
    exchange = "CME"
    symbol = "ESZ4"
    side = "S"

    # Fetch market data
    # await fetch_market_data(uri, system_name, user_id, password, exchange, symbol)

    # Place an order
    # await execute_order(uri, system_name, user_id, password, exchange, symbol, side)
    await execute_order(uri, system_name, user_id, password, exchange, symbol, side)

if __name__ == "__main__":
    asyncio.run(main())
