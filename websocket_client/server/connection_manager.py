# websocket_client/server/connection_manager.py

import asyncio
import logging
import threading
from datetime import datetime, timedelta, timezone
from django.conf import settings
from mqtt_publisher.mqtt_client import get_mqtt_client
from .rithmic_client import RithmicClient

logger = logging.getLogger("rithmic")
mqtt_client = get_mqtt_client()


class Connection:
    def __init__(self, exchange, symbol):
        self.symbol = symbol
        self.exchange = exchange
        self.last_used = datetime.now(timezone.utc)
        self.subscribers = set()
        self.is_active = True
        self.client = None


class ConnectionManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ConnectionManager, cls).__new__(cls)
                cls._instance.connections = {}
                cls._instance.lock = asyncio.Lock()
            return cls._instance

    async def get_or_create_connection(self, exchange, symbol):
        """Get existing connection or create a new one."""
        connection_id = f"{exchange}_{symbol}"

        async with self.lock:
            # Check for existing connection
            if connection_id in self.connections:
                conn = self.connections[connection_id]
                await conn.client.subscribe(exchange, symbol)
                logger.info("Avoid new connection for existing!")
                return connection_id

            if len(self.connections):
                logger.info("Reuse existing connection!")
                conn = list(self.connections.values())[0]
                self.connections[connection_id] = conn
                conn.last_used = datetime.now(timezone.utc)
                await conn.client.subscribe(exchange, symbol)
                return connection_id

            # Create new connection
            try:
                logger.info(f"Creating new connection for {symbol} on {exchange}")

                # Create new Connection instance
                connection = Connection(exchange, symbol)

                # Create and initialize RithmicClient
                connection.client = RithmicClient(
                    uri=settings.WS_SERVER_CONFIG["URI"],
                    system_name=settings.WS_SERVER_CONFIG["SYSTEM_NAME"],
                    user_id=settings.WS_SERVER_CONFIG["USER_ID"],
                    password=settings.WS_SERVER_CONFIG["PASSWORD"],
                )

                # Connect and login
                await connection.client.connect()
                await connection.client.login()

                # Subscribe to market data
                await connection.client.subscribe(exchange, symbol)

                # Store the connection
                self.connections[connection_id] = connection

                # Start message handling task
                asyncio.create_task(self._handle_messages(connection_id))

                logger.info(f"Successfully created connection {connection_id}")
                return connection_id

            except Exception as e:
                logger.error(f"Failed to create connection: {e}")
                if connection_id in self.connections:
                    await self.remove_connection(connection_id)
                raise

    async def _handle_messages(self, connection_id):
        """Handle incoming messages for a connection."""
        connection = self.connections[connection_id]

        while connection.is_active:
            try:
                message = await connection.client.receive_message()
                if message:  # Only process if we got a valid message
                    # Publish to MQTT
                    # print(message)
                    mqtt_client.publish_message(settings.MQTT_CONFIG["TOPIC"], message)
                    connection.last_used = datetime.now(timezone.utc)

            except Exception as e:
                logger.error(f"Error handling messages for {connection_id}: {e}")
                if not connection.client.ws or connection.client.ws.closed:
                    logger.error("WebSocket connection lost, attempting reconnect...")
                    try:
                        await connection.client.connect()
                        await connection.client.login()
                        await connection.client.subscribe(connection.exchange, connection.symbol)
                    except Exception as reconnect_error:
                        logger.error(f"Reconnection failed: {reconnect_error}")
                        await asyncio.sleep(5)  # Wait before retry
                else:
                    await asyncio.sleep(1)  # Prevent tight loop on other errors

    async def remove_connection(self, connection_id):
        """Remove a connection and cleanup."""
        async with self.lock:
            for connection_id in self.connections.keys():
                connection = self.connections[connection_id]
                connection.is_active = False
                if connection.client:
                    await connection.client.close()
                logger.info(f"Removed connection {connection_id}")
            self.connections = {}
            self.client = None

    async def check_connections(self):
        """Check connection health and remove stale ones."""
        async with self.lock:
            now = datetime.now(timezone.utc)
            timeout = timedelta(seconds=settings.RITHMIC_SERVER["CONNECTION_TIMEOUT"])

            for conn_id, conn in list(self.connections.items()):
                try:
                    if now - conn.last_used > timeout:
                        logger.info(f"Connection {conn_id} timed out")
                        await self.remove_connection(conn_id)
                    elif conn.client and not conn.client.ws.closed:
                        await conn.client.send_heartbeat()
                except Exception as e:
                    logger.error(f"Error checking connection {conn_id}: {e}")
                    await self.remove_connection(conn_id)

    async def close_all(self):
        """Close all connections."""
        async with self.lock:
            for conn_id in list(self.connections.keys()):
                await self.remove_connection(conn_id)

    async def get_status(self):
        """Get status of all connections."""
        async with self.lock:
            return {
                "active_connections": len(self.connections),
                "connections": [
                    {
                        "id": conn_id,
                        "symbol": conn.symbol,
                        "exchange": conn.exchange,
                        "last_used": conn.last_used.isoformat(),
                        "subscribers": len(conn.subscribers),
                        "is_connected": conn.client and not conn.client.ws.closed if conn.client else False,
                    }
                    for conn_id, conn in self.connections.items()
                ],
            }
