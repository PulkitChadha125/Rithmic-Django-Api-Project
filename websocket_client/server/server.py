# websocket_client/server/server.py

import asyncio
import json
import logging
from aiohttp import web
from .connection_manager import ConnectionManager
from django.conf import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("rithmic")


class WebSocketServer:
    def __init__(self):
        self.app = web.Application()
        self.connection_manager = None
        self.setup_routes()
        self.app.on_startup.append(self.startup)
        self.app.on_shutdown.append(self.shutdown)
        logger.info("WebSocket server initialized")

    def setup_routes(self):
        self.app.router.add_post("/subscribe", self.handle_subscribe)
        self.app.router.add_post("/unsubscribe", self.handle_unsubscribe)
        self.app.router.add_get("/status", self.handle_status)
        self.app.router.add_get("/health", self.handle_health)
        logger.info("Routes configured")

    async def startup(self, app):
        """Initialize server components on startup."""
        logger.info("Starting WebSocket server components...")
        self.connection_manager = ConnectionManager()
        logger.info("WebSocket server components started")

    async def shutdown(self, app):
        """Cleanup on server shutdown."""
        logger.info("Shutting down WebSocket server...")
        if self.connection_manager:
            await self.connection_manager.close_all()
        logger.info("WebSocket server shutdown complete")

    async def handle_health(self, request):
        """Health check endpoint."""
        return web.json_response({"status": "healthy"})

    async def handle_subscribe(self, request):
        """Handle subscription requests from Django workers."""
        try:
            data = await request.json()
            symbol = data.get("symbol")
            exchange = data.get("exchange")

            if not symbol or not exchange:
                raise web.HTTPBadRequest(text="Symbol and exchange are required")

            logger.info(f"Handling subscribe request for {symbol} on {exchange}")
            connection_id = await self.connection_manager.get_or_create_connection(exchange, symbol)

            return web.json_response(
                {"status": "success", "connection_id": connection_id, "symbol": symbol, "exchange": exchange}
            )

        except Exception as e:
            logger.error(f"Error handling subscribe request: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def handle_unsubscribe(self, request):
        """Handle unsubscribe requests."""
        try:
            data = await request.json()
            connection_id = data.get("connection_id")

            if not connection_id:
                raise web.HTTPBadRequest(text="Connection ID is required")

            await self.connection_manager.remove_connection(connection_id)

            return web.json_response({"status": "success", "message": "Unsubscribed successfully"})

        except Exception as e:
            logger.error(f"Error handling unsubscribe request: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)

    async def handle_status(self, request):
        """Return server status and active connections."""
        try:
            status = await self.connection_manager.get_status()
            return web.json_response(status)
        except Exception as e:
            logger.error(f"Error handling status request: {e}")
            return web.json_response({"status": "error", "message": str(e)}, status=500)
