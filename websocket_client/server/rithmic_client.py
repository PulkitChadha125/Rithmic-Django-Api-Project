# websocket_client/server/rithmic_client.py

import asyncio
import pathlib
import ssl
import websockets
from rithmic_api.base_pb2 import Base
from rithmic_api.request_heartbeat_pb2 import RequestHeartbeat
from rithmic_api.request_login_pb2 import RequestLogin
from rithmic_api.request_market_data_update_pb2 import RequestMarketDataUpdate
from rithmic_api.response_login_pb2 import ResponseLogin
from rithmic_api.best_bid_offer_pb2 import BestBidOffer
from rithmic_api.last_trade_pb2 import LastTrade
from mqtt_publisher.mqtt_client import get_mqtt_client
from django.conf import settings
from datetime import datetime
import logging

logger = logging.getLogger("rithmic")


class RithmicClient:
    def __init__(self, uri, system_name, user_id, password):
        self.uri = uri
        self.system_name = system_name
        self.user_id = user_id
        self.password = password
        self.ws = None
        self.mqtt_client = get_mqtt_client()
        self.ssl_context = None

        if "wss://" in uri:
            self.ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            localhost_pem = pathlib.Path(__file__).with_name("rithmic_ssl_cert_auth_params")
            self.ssl_context.load_verify_locations(localhost_pem)

    async def connect(self):
        """Connect to the Rithmic WebSocket."""
        try:
            if self.ws is None or self.ws.closed:
                self.ws = await websockets.connect(self.uri, ssl=self.ssl_context, ping_interval=3)
                logger.info(f"Connected to {self.uri}")
            return self.ws
        except Exception as e:
            logger.error(f"Connection error: {e}")
            raise

    async def login(self):
        """Log in to the Rithmic system."""
        try:
            # Ensure we have an active connection
            if not self.ws or self.ws.closed:
                raise Exception("No active WebSocket connection")

            # Create login request
            rq = RequestLogin()
            rq.template_id = 10
            rq.template_version = "3.9"
            rq.user = self.user_id
            rq.password = self.password
            rq.app_name = "SampleMD.py"
            rq.app_version = "0.3.0.0"
            rq.system_name = self.system_name
            rq.infra_type = RequestLogin.SysInfraType.TICKER_PLANT

            # Log request (excluding password)
            logger.debug(
                f"Sending login request: user={self.user_id}, system={self.system_name}, template_id={rq.template_id}"
            )

            # Serialize and send request
            request_data = rq.SerializeToString()
            if not request_data:
                raise Exception("Failed to serialize login request")

            await self.ws.send(request_data)
            rp_buf = await self.ws.recv()

            # Log the raw response buffer
            logger.debug(f"Raw response buffer: {rp_buf}")

            rp = ResponseLogin()
            try:
                rp.ParseFromString(rp_buf)
            except Exception as parse_error:
                logger.error(f"Failed to parse login response: {parse_error}")
                logger.error(f"Response buffer length: {len(rp_buf)}")
                raise Exception(f"Failed to parse login response: {parse_error}")

            # Log the full response like in the original code
            logger.info(
                f"ResponseLogin:\nTemplate ID: {rp.template_id}\nTemplate Version: {rp.template_version}\n"
                f"User Msg: {rp.user_msg}\nRP Code: {rp.rp_code}\nFCM ID: {rp.fcm_id}\nIB ID: {rp.ib_id}"
            )

            # Get first values from repeated fields or use defaults
            rp_code = rp.rp_code[0] if rp.rp_code else "Unknown"
            user_msg = rp.user_msg[0] if rp.user_msg else "No message"

            # Log the response
            logger.info(
                f"ResponseLogin:\nTemplate ID: {rp.template_id}\nTemplate Version: {rp.template_version}\n"
                f"User Msg: {user_msg}\nRP Code: {rp_code}\nFCM ID: {rp.fcm_id}\nIB ID: {rp.ib_id}"
            )

            # Check response code (first value from the repeated field)
            if rp_code != "0":
                raise Exception(f"Login failed with code {rp_code}: {user_msg}")

            logger.info("Login successful")
            return rp

        except Exception as e:
            logger.error(f"Login error: {e}")
            raise

    async def subscribe(self, exchange, symbol):
        """Subscribe to market data."""
        try:
            rq = RequestMarketDataUpdate()
            rq.template_id = 100
            rq.symbol = symbol
            rq.exchange = exchange
            rq.request = RequestMarketDataUpdate.Request.SUBSCRIBE
            rq.update_bits = RequestMarketDataUpdate.UpdateBits.LAST_TRADE | RequestMarketDataUpdate.UpdateBits.BBO

            await self.ws.send(rq.SerializeToString())
            logger.info(f"Subscribed to market data for {symbol} on {exchange}")
        except Exception as e:
            logger.error(f"Subscription error: {e}")
            raise

    async def run(self, exchange, symbol, max_messages=100):
        """Main run loop for the client."""
        try:
            logger.info("Starting Rithmic client run...")

            logger.info("Attempting to connect...")
            await self.connect()
            logger.info("Connection established successfully")

            logger.info("Attempting login...")
            try:
                await self.login()
                logger.info("Login completed successfully")
            except Exception as login_error:
                logger.error(f"Login failed with error: {str(login_error)}")
                logger.error(f"Login error type: {type(login_error)}")
                raise

            logger.info("Attempting to subscribe...")
            await self.subscribe(exchange, symbol)

            message_count = 0
            while True:
                message = await self.receive_message()
                if message:
                    # self.mqtt_client.publish_message(settings.MQTT_CONFIG["TOPIC"], message)
                    message_count += 1

                    if message_count % 20 == 0:
                        logger.info(f"Processed {message_count}/{max_messages} messages")

        except Exception as e:
            logger.error(f"Error in run loop: {e}")
            raise
        finally:
            await self.close()

    async def receive_message(self):
        """Receive and process a single message."""
        try:
            msg_buf = await asyncio.wait_for(self.ws.recv(), timeout=5)

            base = Base()
            base.ParseFromString(msg_buf)

            if base.template_id == 151:  # BestBidOffer
                msg = BestBidOffer()
                msg.ParseFromString(msg_buf)
                json_data = {
                    "timestamp": datetime.now().strftime("%m-%d-%Y %H:%M:%S"),
                    "symbol": msg.symbol,
                    "ask_price": msg.ask_price,
                    "bid_price": msg.bid_price,
                }
                logger.debug(f"BestBidOffer: {json_data}")
                self.mqtt_client.publish_message(settings.MQTT_CONFIG["TOPIC"], json_data)
                # return json_data

            elif base.template_id == 150:  # LastTrade
                msg = LastTrade()
                msg.ParseFromString(msg_buf)
                json_data = {
                    "timestamp": datetime.now().strftime("%m-%d-%Y %H:%M:%S"),
                    "symbol": msg.symbol,
                    "trade_price": msg.trade_price,
                }
                logger.debug(f"LastTrade: {json_data}")
                self.mqtt_client.publish_message(settings.MQTT_CONFIG["TOPIC"], json_data)
                # return json_data

            elif base.template_id == 101:
                if "no data" in str(msg_buf):
                    logger.warning("No data available for symbol")
                    json_data = {
                        "timestamp": datetime.now().strftime("%m-%d-%Y %H:%M:%S"),
                        "error": "No data found for this symbol",
                    }
                    self.mqtt_client.publish_message(settings.MQTT_CONFIG["TOPIC"], json_data)
                    # return json_data

            elif base.template_id == 19:
                logger.debug("Received heartbeat response")
                return None

            # return None  # For unhandled message types

        except asyncio.TimeoutError:
            await self.send_heartbeat()
            return None
        except websockets.ConnectionClosed:
            logger.error("WebSocket connection closed")
            raise
        except Exception as e:
            logger.error(f"Error receiving message: {e}")
            raise

    async def send_heartbeat(self):
        """Send a heartbeat to keep the connection alive."""
        try:
            rq = RequestHeartbeat()
            rq.template_id = 18
            await self.ws.send(rq.SerializeToString())
            logger.debug("Sent heartbeat")
        except Exception as e:
            logger.error(f"Error sending heartbeat: {e}")
            raise

    async def close(self):
        """Close the WebSocket connection."""
        try:
            if self.ws and not self.ws.closed:
                await self.ws.close()
                logger.info("WebSocket connection closed")
        except Exception as e:
            logger.error(f"Error closing connection: {e}")
            raise


def run_rithmic(uri, system_name, user_id, password, exchange, symbol):
    """Run the Rithmic client as a function."""
    client = RithmicClient(uri, system_name, user_id, password)
    asyncio.run(client.run(exchange, symbol))
