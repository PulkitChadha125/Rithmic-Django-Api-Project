import asyncio
import websockets
import ssl

# Protobuf Imports
import base_pb2
import exchange_order_notification_pb2
import request_login_pb2
import response_login_pb2
import request_account_list_pb2
import response_account_list_pb2
import request_trade_routes_pb2
import response_new_order_pb2
import response_trade_routes_pb2
import request_new_order_pb2
import request_subscribe_for_order_updates_pb2
import rithmic_order_notification_pb2

# Globals
g_fcm_id = ""
g_ib_id = ""
g_account_id = ""
g_trade_route = ""

async def connect_to_rithmic(uri, ssl_context):
    ws = await websockets.connect(uri, ssl=ssl_context, ping_interval=3)
    print(f"Connected to WebSocket at {uri}")
    return ws

async def login(ws, user_id, password, system_name):
    rq = request_login_pb2.RequestLogin()
    rq.template_id = 10
    rq.user = user_id
    rq.password = password
    rq.system_name = system_name
    rq.app_name = "RithmicApp"
    rq.app_version = "1.0.0"
    rq.infra_type = request_login_pb2.RequestLogin.SysInfraType.ORDER_PLANT
    rq.template_version = "3.9"
    await ws.send(rq.SerializeToString())

    response = await ws.recv()
    rp = response_login_pb2.ResponseLogin()
    rp.ParseFromString(response)

    print("\n      ResponseLogin :\n      ===============")
    print(f"        template_id : {rp.template_id}")
    print(f"   template_version : {rp.template_version}")
    print(f"           user_msg : {rp.user_msg}")
    print(f"            rp code : {rp.rp_code}")
    print(f"             fcm_id : {rp.fcm_id}")
    print(f"             ib_id  : {rp.ib_id}")
    print(f"       country_code : {rp.country_code}")
    print(f"         state_code : {rp.state_code}")
    print(f" heartbeat_interval : {rp.heartbeat_interval}")
    print(f"     unique_user_id : {rp.unique_user_id}")

    if "0" in rp.rp_code:
        global g_fcm_id, g_ib_id
        g_fcm_id, g_ib_id = rp.fcm_id, rp.ib_id
        return True
    return False

async def fetch_account_info(ws):
    print("\nRetrieving account list...")
    rq = request_account_list_pb2.RequestAccountList()
    rq.template_id = 302
    rq.fcm_id = g_fcm_id
    rq.ib_id = g_ib_id
    rq.user_type = 3

    await ws.send(rq.SerializeToString())
    global g_account_id
    g_account_id = None

    while True:
        rp_buf = await ws.recv()
        rp = response_account_list_pb2.ResponseAccountList()
        rp.ParseFromString(rp_buf)

        print("\n ResponseAccountList :\n ====================")
        print(f"         template_id : {rp.template_id}")
        print(f"            user_msg : {rp.user_msg}")
        print(f"  rq_handler_rp_code : {rp.rq_handler_rp_code}")
        print(f"             rp code : {rp.rp_code}")
        print(f"              fcm_id : {rp.fcm_id}")
        print(f"              ib_id  : {rp.ib_id}")
        print(f"          account_id : {rp.account_id}")
        print(f"        account_name : {rp.account_name}")

        if not rp.rq_handler_rp_code:
            if "0" in rp.rp_code:
                if g_account_id:
                    return True
                else:
                    print("Failed to retrieve complete account list.")
                    return False

        if rp.rq_handler_rp_code and rp.rq_handler_rp_code[0] == "0" and rp.account_id:
            g_account_id = rp.account_id

async def fetch_trade_route_info(ws):
    print("\nRetrieving trade routes...")
    rq = request_trade_routes_pb2.RequestTradeRoutes()
    rq.template_id = 310
    rq.subscribe_for_updates = False
    await ws.send(rq.SerializeToString())

    global g_trade_route
    g_trade_route = None

    while True:
        rp_buf = await ws.recv()
        rp = response_trade_routes_pb2.ResponseTradeRoutes()
        rp.ParseFromString(rp_buf)

        print("\n ResponseTradeRoutes :\n =====================")
        print(f"         template_id : {rp.template_id}")
        print(f"            user_msg : {rp.user_msg}")
        print(f"  rq_handler_rp_code : {rp.rq_handler_rp_code}")
        print(f"             rp code : {rp.rp_code}")
        print(f"              fcm_id : {rp.fcm_id}")
        print(f"              ib_id  : {rp.ib_id}")
        print(f"            exchange : {rp.exchange}")
        print(f"         trade_route : {rp.trade_route}")
        print(f"              status : {rp.status}")
        print(f"          is_default : {rp.is_default}")

        if not rp.rq_handler_rp_code:
            if "0" in rp.rp_code:
                if g_trade_route:
                    return True
                else:
                    print("Failed to retrieve complete trade routes.")
                    return False

        if rp.rq_handler_rp_code and rp.rq_handler_rp_code[0] == "0" and rp.trade_route:
            g_trade_route = rp.trade_route


async def place_order(ws, symbol, side, quantity, exchange):
    global g_fcm_id, g_ib_id, g_account_id, g_trade_route

    # Debug output to verify field values
    print("\n--- Order Request Debug ---")
    print(f"Symbol: {symbol}")
    print(f"Side: {'BUY' if side == 'B' else 'SELL'}")
    print(f"Quantity: {quantity}")
    print(f"FCM ID: {g_fcm_id}")
    print(f"IB ID: {g_ib_id}")
    print(f"Account ID: {g_account_id}")
    print(f"Trade Route: {g_trade_route}")
    print(f"Exchange: {exchange}")

    # Building the order request
    rq = request_new_order_pb2.RequestNewOrder()
    rq.template_id = 312
    rq.fcm_id = g_fcm_id
    rq.ib_id = g_ib_id
    rq.account_id = g_account_id
    rq.symbol = symbol
    rq.quantity = int(quantity)  # Ensure quantity is an integer
    rq.transaction_type = (
        request_new_order_pb2.RequestNewOrder.BUY
        if side == "B" else
        request_new_order_pb2.RequestNewOrder.SELL
    )
    rq.exchange = exchange  # Set the exchange explicitly based on input
    rq.trade_route = g_trade_route
    rq.price_type = request_new_order_pb2.RequestNewOrder.MARKET  # Assuming market order for simplicity
    rq.duration = request_new_order_pb2.RequestNewOrder.DAY  # Assuming DAY as order duration
    rq.manual_or_auto = request_new_order_pb2.RequestNewOrder.MANUAL  # Set manual or auto explicitly

    await ws.send(rq.SerializeToString())
    print("Order request sent")

    # Receive and process response
    response_data = await ws.recv()
    rp = response_new_order_pb2.ResponseNewOrder()
    rp.ParseFromString(response_data)

    print("\nResponseNewOrder:")
    print(f"  template_id: {rp.template_id}")
    print(f"  user_msg: {rp.user_msg}")
    print(f"  user_tag: {rp.user_tag}")
    print(f"  rq_handler_rp_code: {rp.rq_handler_rp_code}")
    print(f"  rp_code: {rp.rp_code}")
    print(f"  basket_id: {rp.basket_id}")
    print(f"  ssboe: {rp.ssboe}")
    print(f"  usecs: {rp.usecs}")

    # Check for errors
    if rp.rp_code and rp.rp_code[0] != '0':
        print(f"Order failed with error: {rp.rp_code}")
    else:
        print("Order placed successfully")




async def subscribe_for_order_updates(ws):
    rq = request_subscribe_for_order_updates_pb2.RequestSubscribeForOrderUpdates()
    rq.template_id = 309
    await ws.send(rq.SerializeToString())
    print("Subscribed for order updates")

import asyncio
from websockets.exceptions import ConnectionClosedError

async def handle_order_updates(ws):
    try:
        while True:
            try:
                response_data = await asyncio.wait_for(ws.recv(), timeout=60)
                # Process the response_data here, e.g., parse it and check for order updates

            except asyncio.TimeoutError:
                print("No updates received within timeout period. Exiting update handler.")
                break  # Exit the loop if no data is received within the timeout

            except ConnectionClosedError:
                print("WebSocket connection closed unexpectedly. Exiting update handler.")
                break  # Exit on unexpected disconnection

    except ConnectionClosedError:
        print("WebSocket connection was closed. Reconnecting...")
        # Optionally: Add reconnection logic here

    finally:
        await ws.close()
        print("WebSocket connection closed.")


async def execute_order(uri, system_name, user_id, password, exchange, symbol, side, quantity):
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    ws = await connect_to_rithmic(uri, ssl_context)

    if await login(ws, user_id, password, system_name):
        print("Login successful")
        if await fetch_account_info(ws) and await fetch_trade_route_info(ws):
            await place_order(ws, symbol, side, quantity, exchange)  # Pass exchange here
            await handle_order_updates(ws)
        else:
            print("Account or trade route retrieval failed")
    else:
        print("Login failed")
    await ws.close()
