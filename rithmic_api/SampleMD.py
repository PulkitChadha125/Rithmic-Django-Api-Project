import asyncio
import pathlib
import ssl
import websockets
from rithmic_api.base_pb2 import Base
from rithmic_api.request_heartbeat_pb2 import RequestHeartbeat
from rithmic_api.request_login_pb2 import RequestLogin
from rithmic_api.request_market_data_update_pb2 import RequestMarketDataUpdate
from rithmic_api.response_login_pb2 import ResponseLogin
from rithmic_api.response_market_data_update_pb2 import ResponseMarketDataUpdate
from rithmic_api.best_bid_offer_pb2 import BestBidOffer
from rithmic_api.last_trade_pb2 import LastTrade


async def connect_to_rithmic(uri, ssl_context=None):
    """Connect to the Rithmic WebSocket."""
    ws = await websockets.connect(uri, ssl=ssl_context, ping_interval=3)
    print(f"Connected to {uri}")
    return ws


async def rithmic_login(ws, system_name, user_id, password):
    """Log in to the Rithmic system."""
    rq = RequestLogin()
    rq.template_id = 10
    rq.template_version = "3.9"
    rq.user = user_id
    rq.password = password
    rq.app_name = "SampleMD.py"
    rq.app_version = "0.3.0.0"
    rq.system_name = system_name
    rq.infra_type = RequestLogin.SysInfraType.TICKER_PLANT

    await ws.send(rq.SerializeToString())
    rp_buf = await ws.recv()

    rp = ResponseLogin()
    rp.ParseFromString(rp_buf)

    print(f"ResponseLogin:\nTemplate ID: {rp.template_id}\nTemplate Version: {rp.template_version}\n"
          f"User Msg: {rp.user_msg}\nRP Code: {rp.rp_code}\nFCM ID: {rp.fcm_id}\nIB ID: {rp.ib_id}")


async def subscribe(ws, exchange, symbol):
    """Subscribe to market data."""
    rq = RequestMarketDataUpdate()
    rq.template_id = 100
    rq.symbol = symbol
    rq.exchange = exchange
    rq.request = RequestMarketDataUpdate.Request.SUBSCRIBE
    rq.update_bits = RequestMarketDataUpdate.UpdateBits.LAST_TRADE | RequestMarketDataUpdate.UpdateBits.BBO

    await ws.send(rq.SerializeToString())
    print(f"Subscribed to market data for {symbol} on {exchange}")


async def consume(ws, max_num_msgs=100):
    """Consume and handle market data messages."""
    num_msgs = 0

    while num_msgs < max_num_msgs:
        try:
            msg_buf = await asyncio.wait_for(ws.recv(), timeout=5)
            num_msgs += 1
            print(f"Received message {num_msgs}/{max_num_msgs}")

            base = Base()
            base.ParseFromString(msg_buf)

            if base.template_id == 151:  # BestBidOffer
                msg = BestBidOffer()
                msg.ParseFromString(msg_buf)
                print(f"BestBidOffer:\nSymbol: {msg.symbol}, Bid Price: {msg.bid_price}, Ask Price: {msg.ask_price}")
            elif base.template_id == 150:  # LastTrade
                msg = LastTrade()
                msg.ParseFromString(msg_buf)
                print(f"LastTrade:\nSymbol: {msg.symbol}, Trade Price: {msg.trade_price}")
            else:
                print(f"Unhandled message type: {base.template_id}")

        except asyncio.TimeoutError:
            print("No message received, sending heartbeat.")
            await send_heartbeat(ws)
        except websockets.ConnectionClosed:
            print("WebSocket connection closed.")
            break


async def send_heartbeat(ws):
    """Send a heartbeat to keep the connection alive."""
    rq = RequestHeartbeat()
    rq.template_id = 18
    await ws.send(rq.SerializeToString())
    print("Sent heartbeat request")


async def main(uri, system_name, user_id, password, exchange, symbol):
    """Main function to connect and subscribe to Rithmic market data."""
    ssl_context = None
    if "wss://" in uri:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
        ssl_context.load_verify_locations(localhost_pem)

    ws = await connect_to_rithmic(uri, ssl_context)
    try:
        await rithmic_login(ws, system_name, user_id, password)
        await subscribe(ws, exchange, symbol)
        await consume(ws)
    finally:
        await ws.close()
        print("WebSocket connection closed.")


def run_rithmic(uri, system_name, user_id, password, exchange, symbol):
    """Run the Rithmic client as a function."""
    asyncio.run(main(uri, system_name, user_id, password, exchange, symbol))


# Example of how to call the function programmatically
if __name__ == "__main__":
    # Replace with your inputs
    URI = "wss://rituz00100.rithmic.com:443"
    SYSTEM_NAME = "Rithmic Test"
    USER_ID = "pulkitchadhaqwerty@gmail.com"
    PASSWORD = "UBiKMPuY"
    EXCHANGE = "CME"
    SYMBOL = "ESZ4"

    run_rithmic(URI, SYSTEM_NAME, USER_ID, PASSWORD, EXCHANGE, SYMBOL)
