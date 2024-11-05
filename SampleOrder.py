import asyncio
import pathlib
import ssl
import websockets

import base_pb2
import request_account_list_pb2
import response_account_list_pb2
import request_heartbeat_pb2
import response_heartbeat_pb2
import request_login_pb2
import response_login_pb2
import request_login_info_pb2
import response_login_info_pb2
import request_logout_pb2
import response_logout_pb2
import request_trade_routes_pb2
import response_trade_routes_pb2
import request_subscribe_for_order_updates_pb2
import response_subscribe_for_order_updates_pb2
import request_new_order_pb2
import rithmic_order_notification_pb2
import exchange_order_notification_pb2

# Global variables for managing state
g_rcvd_account = False
g_fcm_id = ""
g_ib_id = ""
g_account_id = ""
g_symbol = ""
g_exchange = ""
g_rcvd_trade_route = False
g_trade_route = ""
g_order_is_complete = False
from rithmic_order_notification_pb2 import RithmicOrderNotification
from exchange_order_notification_pb2 import ExchangeOrderNotification




async def rithmic_order_notification_cb(msg_buf):
    notification = RithmicOrderNotification()
    notification.ParseFromString(msg_buf)
    print("Rithmic Order Notification:")
    print(f"  Template ID: {notification.template_id}")
    print(f"  Notify Type: {notification.notify_type}")
    print(f"  Status: {notification.status}")
    print(f"  Account ID: {notification.account_id}")
    print(f"  Symbol: {notification.symbol}")
    print(f"  Exchange: {notification.exchange}")
    # Add more fields as necessary for detailed logging

async def exchange_order_notification_cb(msg_buf):
    notification = ExchangeOrderNotification()
    notification.ParseFromString(msg_buf)
    print("Exchange Order Notification:")
    print(f"  Template ID: {notification.template_id}")
    print(f"  Notify Type: {notification.notify_type}")
    print(f"  Status: {notification.status}")
    print(f"  Account ID: {notification.account_id}")
    print(f"  Symbol: {notification.symbol}")
    print(f"  Exchange Order ID: {notification.exchange_order_id}")
    # Add more fields as necessary for detailed logging

async def connect_to_rithmic(uri, ssl_context):
    ws = await websockets.connect(uri, ssl=ssl_context, ping_interval=3)
    print(f"Connected to {uri}")
    return ws

async def rithmic_login(ws, system_name, infra_type, user_id, password):
    rq = request_login_pb2.RequestLogin()
    rq.template_id = 10
    rq.template_version = "5.27"  # Update this version as needed
    rq.user_msg.append("hello")
    rq.user = user_id
    rq.password = password
    rq.app_name = "SampleOrder.py"
    rq.app_version = "0.3.0.0"
    rq.system_name = system_name
    rq.infra_type = infra_type

    serialized = rq.SerializeToString()
    await ws.send(serialized)

    rp_buf = await ws.recv()
    rp = response_login_pb2.ResponseLogin()
    rp.ParseFromString(rp_buf[0:])

    print(f"")
    print(f"      ResponseLogin :")
    print(f"      ===============")
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
    print(f"")



async def list_accounts(ws, fcm_id, ib_id):
    global g_account_id, g_fcm_id, g_ib_id, g_rcvd_account
    rq = request_account_list_pb2.RequestAccountList()
    rq.template_id = 302
    rq.fcm_id = fcm_id
    rq.ib_id = ib_id
    serialized = rq.SerializeToString()
    await ws.send(serialized)
    rp_buf = await ws.recv()
    rp = response_account_list_pb2.ResponseAccountList()
    rp.ParseFromString(rp_buf)
    if rp.rq_handler_rp_code and rp.rq_handler_rp_code[0] == "0":
        g_fcm_id = rp.fcm_id
        g_ib_id = rp.ib_id
        g_account_id = rp.account_id
        g_rcvd_account = True
        print("Account information received:", g_account_id)

async def list_trade_routes(ws):
    global g_trade_route, g_rcvd_trade_route
    rq = request_trade_routes_pb2.RequestTradeRoutes()
    rq.template_id = 310
    serialized = rq.SerializeToString()
    await ws.send(serialized)
    rp_buf = await ws.recv()
    rp = response_trade_routes_pb2.ResponseTradeRoutes()
    rp.ParseFromString(rp_buf)
    if rp.rq_handler_rp_code and rp.rq_handler_rp_code[0] == "0":
        g_trade_route = rp.trade_route
        g_rcvd_trade_route = True
        print("Trade route received:", g_trade_route)

async def new_order(ws, exchange, symbol, side):
    rq = request_new_order_pb2.RequestNewOrder()
    rq.template_id = 312
    rq.fcm_id = g_fcm_id
    rq.ib_id = g_ib_id
    rq.account_id = g_account_id
    rq.exchange = exchange
    rq.symbol = symbol
    rq.quantity = 1
    rq.transaction_type = request_new_order_pb2.RequestNewOrder.TransactionType.BUY if side == "B" else request_new_order_pb2.RequestNewOrder.TransactionType.SELL
    rq.duration = request_new_order_pb2.RequestNewOrder.Duration.DAY
    rq.price_type = request_new_order_pb2.RequestNewOrder.PriceType.MARKET
    rq.trade_route = g_trade_route
    serialized = rq.SerializeToString()
    await ws.send(serialized)
    print("Order request sent")

async def rithmic_logout(ws):
    rq = request_logout_pb2.RequestLogout()
    rq.template_id = 12
    await ws.send(rq.SerializeToString())
    print("Logout request sent")

async def execute_order(uri, system_name, user_id, password, exchange, symbol, side):
    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
    ssl_context.load_verify_locations(localhost_pem)

    ws = await connect_to_rithmic(uri, ssl_context)
    print("Connected to WebSocket")

    # Perform login
    await rithmic_login(ws, system_name, request_login_pb2.RequestLogin.SysInfraType.ORDER_PLANT, user_id, password)
    print("Login completed")

    # Retrieve accounts
    await list_accounts(ws, g_fcm_id, g_ib_id)
    print(f"Account retrieved: {g_account_id}, Received Account: {g_rcvd_account}")

    # Retrieve trade routes
    await list_trade_routes(ws)
    print(f"Trade route retrieved: {g_trade_route}, Received Trade Route: {g_rcvd_trade_route}")

    # Check if account and trade route were received
    if g_rcvd_account and g_rcvd_trade_route:
        print("Proceeding with order placement")
        await new_order(ws, exchange, symbol, side)
        await consume(ws)
    else:
        print("Order execution aborted: Missing account or trade route information")

    if ws.open:
        await rithmic_logout(ws)
        await ws.close()


async def consume(ws):
    max_num_msgs = 20
    for _ in range(max_num_msgs):
        msg_buf = await ws.recv()
        base = base_pb2.Base()
        base.ParseFromString(msg_buf)
        if base.template_id == 351:
            await rithmic_order_notification_cb(msg_buf)
        elif base.template_id == 352:
            await exchange_order_notification_cb(msg_buf)

# Place function calls for order processing as per integration with main.py
