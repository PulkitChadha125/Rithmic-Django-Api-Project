from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
import pathlib
from rithmic_api.SampleMD import run_rithmic  # Import your function
from rithmic_api.SampleOrder_requests import place_rithmic_order
import ssl
import logging
from django.conf import settings
from mqtt_publisher.mqtt_client import get_mqtt_client
from .utils import run_in_background

logger = logging.getLogger(__name__)


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
            run_rithmic_background(uri, system_name, user_id, password, exchange, symbol)
            return Response({"message": "Rithmic function executed successfully."}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

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
