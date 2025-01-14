# websocket_client/apps.py

from django.apps import AppConfig
import threading
import asyncio
from aiohttp import web
import logging

logger = logging.getLogger("rithmic")


def run_server_in_thread(app, host="0.0.0.0", port=8080):
    """Run the WebSocket server in a separate thread."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Create a runner
    runner = web.AppRunner(app)

    try:
        # Start the server
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, host, port)
        loop.run_until_complete(site.start())
        logger.info(f"WebSocket server started successfully on {host}:{port}")
        loop.run_forever()
    except Exception as e:
        logger.error(f"Error running WebSocket server: {e}")
    finally:
        loop.run_until_complete(runner.cleanup())
        loop.close()


class WebsocketClientConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "websocket_client"
    server = None
    server_thread = None

    def ready(self):
        """
        Start the WebSocket server when Django starts.
        This method is called once when Django starts.
        """
        # Avoid running on Django auto-reload
        import sys

        # if "runserver" not in sys.argv:
        #     return

        from .server.server import WebSocketServer
        from django.conf import settings

        try:
            # Create WebSocket server instance
            self.server = WebSocketServer()

            # Start server in a separate thread
            self.server_thread = threading.Thread(
                target=run_server_in_thread,
                args=(
                    self.server.app,
                    settings.RITHMIC_SERVER["HOST"],
                    settings.RITHMIC_SERVER["PORT"],
                ),
                daemon=True,  # Make thread daemon so it stops when Django stops
            )
            self.server_thread.start()

            logger.info("WebSocket server started successfully")

        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
            raise
