# rithmicapp/views.py

from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import pathlib
from rithmic_api.SampleMD import run_rithmic  # Import your function
from rithmic_api.SampleOrder_requests import place_rithmic_order
import ssl
import aiohttp
import logging
from asgiref.sync import async_to_sync
from django.conf import settings
from mqtt_publisher.mqtt_client import get_mqtt_client
from .utils import run_in_background

logger = logging.getLogger("rithmic")


@run_in_background
def run_rithmic_background(*args):
    """
    Wrapper function to run run_rithmic in background
    """
    run_rithmic(*args)


class RithmicApiView(APIView):
    def get(self, request):
        return Response({"success": True}, status=status.HTTP_200_OK)

    def post(self, request):
        logger.info(f"Received data: {request.data}")
        uri = request.data.get("uri")
        system_name = request.data.get("system_name")
        user_id = request.data.get("user_id")
        password = request.data.get("password")
        exchange = request.data.get("exchange")
        symbol = request.data.get("symbol")
        mqtt_client = get_mqtt_client()
        mqtt_client.publish_message(
            settings.MQTT_CONFIG["TOPIC"], {"status": f"Publishing ask/bid prices for {symbol} on exchange {exchange}"}
        )

        # Validate parameters
        if not all([uri, system_name, user_id, password, exchange, symbol]):
            return Response({"error": "All parameters are required."}, status=status.HTTP_400_BAD_REQUEST)

        # Call the run_rithmic function
        try:
            settings.WS_SERVER_CONFIG.update(
                {
                    "URI": uri,
                    "SYSTEM_NAME": system_name,
                    "USER_ID": user_id,
                    "PASSWORD": password,
                }
            )
            response = async_to_sync(self._subscribe_market_data)(exchange, symbol)
            return Response(
                {
                    "message": "Successfully subscribed to market data.",
                    "connection_id": response.get("connection_id"),
                    "symbol": symbol,
                    "exchange": exchange,
                },
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    async def _subscribe_market_data(self, exchange, symbol):
        """Async method to subscribe to market data through the WebSocket server."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://{settings.RITHMIC_SERVER['HOST']}:{settings.RITHMIC_SERVER['PORT']}/subscribe",
                    json={"exchange": exchange, "symbol": symbol},
                ) as response:
                    if response.status != 200:
                        error_data = await response.json()
                        raise Exception(f"Failed to subscribe: {error_data.get('message', 'Unknown error')}")

                    return await response.json()
        except Exception as e:
            logger.error(f"Error in _subscribe_market_data: {e}")
            raise

    def put(self, request):
        logger.info(f"Received data: {request.data}")
        uri = request.data.get("uri")
        system_name = request.data.get("system_name")
        user_id = request.data.get("user_id")
        password = request.data.get("password")
        exchange = request.data.get("exchange")
        symbol = request.data.get("symbol")
        side = request.data.get("side")
        quantity = request.data.get("quantity")

        try:
            response = place_rithmic_order(uri, system_name, user_id, password, exchange, symbol, side, quantity)
            return Response({"RithmicOrderNotification": response}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def delete(self, request):
        """
        API endpoint to unsubscribe from market data.
        Expects connection_id in the request data.
        """
        logger.info(f"Received unsubscribe request: {request.data}")

        # Get connection ID from request
        connection_id = "All"

        # Validate parameters
        if not connection_id:
            return Response({"error": "connection_id is required."}, status=status.HTTP_400_BAD_REQUEST)

        try:
            # Call the unsubscribe endpoint of the WebSocket server
            response = async_to_sync(self._unsubscribe_market_data)(connection_id)

            return Response(
                {"message": "Successfully unsubscribed from market data.", "connection_id": connection_id},
                status=status.HTTP_200_OK,
            )
        except Exception as e:
            logger.error(f"Error in unsubscribe: {e}")
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    async def _unsubscribe_market_data(self, connection_id):
        """Async method to unsubscribe from market data through the WebSocket server."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"http://{settings.RITHMIC_SERVER['HOST']}:{settings.RITHMIC_SERVER['PORT']}/unsubscribe",
                    json={"connection_id": connection_id},
                ) as response:
                    if response.status != 200:
                        error_data = await response.json()
                        raise Exception(f"Failed to unsubscribe: {error_data.get('message', 'Unknown error')}")

                    return await response.json()
        except Exception as e:
            logger.error(f"Error in _unsubscribe_market_data: {e}")
            raise
