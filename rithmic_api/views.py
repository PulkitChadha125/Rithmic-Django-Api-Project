from django.http import JsonResponse
from rithmic.main import execute_order  # Adjust import based on function location


def execute_order_view(request, user_id, password, quantity):
    # Add necessary logic to process request parameters
    uri = "wss://rituz00100.rithmic.com:443"
    system_name = "Rithmic Test"
    exchange = "CME"
    symbol = "ESZ4"
    side = "S"

    # Call execute_order from main.py with the provided arguments
    result = execute_order(uri, system_name, user_id, password, exchange, symbol, side, quantity)

    return JsonResponse({'status': 'Order Executed', 'result': result})
