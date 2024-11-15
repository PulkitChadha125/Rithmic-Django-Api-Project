from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rithmic_api.SampleMD import run_rithmic  # Import your function
import logging

logger = logging.getLogger(__name__)
class RithmicApiView(APIView):
    def post(self, request):
        logger.info(f"Received data: {request.data}")
        uri = request.data.get('uri')
        system_name = request.data.get('system_name')
        user_id = request.data.get('user_id')
        password = request.data.get('password')
        exchange = request.data.get('exchange')
        symbol = request.data.get('symbol')

        # Validate parameters
        if not all([uri, system_name, user_id, password, exchange, symbol]):
            return Response({"error": "All parameters are required."}, status=status.HTTP_400_BAD_REQUEST)

        # Call the run_rithmic function
        try:
            run_rithmic(uri, system_name, user_id, password, exchange, symbol)
            return Response({"message": "Rithmic function executed successfully."}, status=status.HTTP_200_OK)
        except Exception as e:
            return Response({"error": str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

