# SampleMD.py
from response_market_data_update_pb2 import ResponseMarketDataUpdate
import asyncio
import pathlib
import ssl
import websockets
from request_login_pb2 import RequestLogin
from request_market_data_update_pb2 import RequestMarketDataUpdate
from base_pb2 import Base
from response_market_data_update_pb2 import ResponseMarketDataUpdate
from best_bid_offer_pb2 import BestBidOffer
from last_trade_pb2 import LastTrade
from request_heartbeat_pb2 import RequestHeartbeat  # Ensure correct import for heartbeat


async def send_heartbeat(ws):
    # Construct the heartbeat message
    heartbeat = RequestHeartbeat()
    heartbeat.template_id = 19  # Set the template ID if required (use the correct value per Rithmic's spec)

    # Send the serialized heartbeat message
    await ws.send(heartbeat.SerializeToString())
    print("Heartbeat sent")


# Function to connect to Rithmic
async def connect_to_rithmic(uri, ssl_context):
    ws = await websockets.connect(uri, ssl=ssl_context, ping_interval=3)
    print(f"Connected to {uri}")
    return ws

# Login function
async def rithmic_login(ws, system_name, infra_type, user_id, password):
    login_request = RequestLogin()
    login_request.template_id = 10
    login_request.template_version = "3.9"  # Set template version as required
    login_request.system_name = system_name
    login_request.infra_type = infra_type  # Use the enum, e.g., RequestLogin.SysInfraType.TICKER_PLANT
    login_request.user = user_id  # Correct field for username
    login_request.password = password  # Correct field for password
    login_request.app_name = "RithmicApp"  # Set an application name
    login_request.app_version = "1.0.0"  # Set an application version

    # Send the serialized login request
    await ws.send(login_request.SerializeToString())
    response = await ws.recv()
    print("Login response received:", response)

# Subscribe function
# SampleMD.py

from request_market_data_update_pb2 import RequestMarketDataUpdate

async def subscribe(ws, exchange, symbol):
    rq = RequestMarketDataUpdate()
    rq.template_id = 100
    rq.symbol = symbol
    rq.exchange = exchange
    rq.request = RequestMarketDataUpdate.Request.SUBSCRIBE
    rq.update_bits = RequestMarketDataUpdate.UpdateBits.LAST_TRADE | RequestMarketDataUpdate.UpdateBits.BBO  # Set update_bits as required

    # Send the serialized subscription request
    await ws.send(rq.SerializeToString())
    print(f"Subscribed to {symbol} on {exchange}")


# Unsubscribe function
async def unsubscribe(ws, exchange, symbol):
    rq = RequestMarketDataUpdate()
    rq.template_id = 100
    rq.symbol = symbol
    rq.exchange = exchange
    rq.request = RequestMarketDataUpdate.Request.UNSUBSCRIBE
    await ws.send(rq.SerializeToString())
    print(f"Unsubscribed from {symbol} on {exchange}")



def parse_market_data(data):
    update = ResponseMarketDataUpdate()
    update.ParseFromString(data)
    print(update)

# Consume function
async def consume(ws):
    max_num_msgs = 100  # Number of messages to process

    for _ in range(max_num_msgs):
        try:
            msg_buf = await asyncio.wait_for(ws.recv(), timeout=5)
            print("Received message")

            # Parse message base to get template_id
            base = Base()
            base.ParseFromString(msg_buf)

            # Route message based on template_id
            if base.template_id == 13:  # Logout response
                print("Received logout response")

            elif base.template_id == 19:  # Heartbeat response
                print("Received heartbeat response")

            elif base.template_id == 101:  # ResponseMarketDataUpdate
                msg = ResponseMarketDataUpdate()
                msg.ParseFromString(msg_buf)
                print("ResponseMarketDataUpdate:")
                print(f"  user_msg: {msg.user_msg}")
                print(f"  rp_code: {msg.rp_code}")

            elif base.template_id == 151:  # BestBidOffer
                msg = BestBidOffer()
                msg.ParseFromString(msg_buf)
                print("BestBidOffer:")
                print(f"  Symbol: {msg.symbol}")
                print(f"  Exchange: {msg.exchange}")
                print(f"  Bid Price: {msg.bid_price}")
                print(f"  Ask Price: {msg.ask_price}")

            elif base.template_id == 150:  # LastTrade
                msg = LastTrade()
                msg.ParseFromString(msg_buf)
                print("LastTrade:")
                print(f"  Symbol: {msg.symbol}")
                print(f"  Exchange: {msg.exchange}")
                print(f"  Trade Price: {msg.trade_price}")
                print(f"  Trade Size: {msg.trade_size}")

            else:
                print(f"Unknown message type with template_id: {base.template_id}")

        except asyncio.TimeoutError:
            if ws.open:
                print("Connection is open; sending heartbeat")
                await send_heartbeat(ws)
            else:
                print("Connection closed; exiting consume function")
                return

# Logout function
async def rithmic_logout(ws):
    logout_request = RequestLogin()  # Adjust with actual logout protobuf message if available
    await ws.send(logout_request.SerializeToString())
    print("Logged out from Rithmic")

# Disconnect function
async def disconnect_from_rithmic(ws):
    await ws.close()
    print("Disconnected from Rithmic")

# Main function to fetch market data
async def fetch_market_data(uri, system_name, user_id, password, exchange, symbol):
    ssl_context = None
    if "wss://" in uri:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
        ssl_context.load_verify_locations(localhost_pem)

    ws = await connect_to_rithmic(uri, ssl_context)
    await rithmic_login(ws, system_name, RequestLogin.SysInfraType.TICKER_PLANT, user_id, password)
    await subscribe(ws, exchange, symbol)
    await consume(ws)

    if ws.open:
        await unsubscribe(ws, exchange, symbol)
        await rithmic_logout(ws)
        await disconnect_from_rithmic(ws)
        print("Completed data fetch")


