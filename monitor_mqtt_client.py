import paho.mqtt.client as mqtt
import time
from datetime import datetime
import argparse
import sys


def on_connect(client, userdata, flags, rc):
    """Callback for when the client connects to the broker."""
    connection_codes = {
        0: "Connected successfully",
        1: "Incorrect protocol version",
        2: "Invalid client identifier",
        3: "Server unavailable",
        4: "Bad username or password",
        5: "Not authorized",
    }
    if rc == 0:
        print("Connected to MQTT broker!")
        client.subscribe(args.topic)
        print(f"Subscribed to topic: {args.topic}")
    else:
        print(f"Connection failed: {connection_codes.get(rc, f'Unknown error ({rc})')}")
        sys.exit(1)


def on_message(client, userdata, msg):
    """Callback for when a message is received."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        payload = msg.payload.decode("utf-8")
    except:
        payload = msg.payload

    print(f"\n[{timestamp}] {msg.topic}:")
    print(f"    {payload}")


def on_disconnect(client, userdata, rc):
    """Callback for when the client disconnects from the broker."""
    if rc != 0:
        print("Unexpected disconnection. Attempting to reconnect...")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MQTT Message Monitor")
    parser.add_argument("--host", default="103.98.212.140", help="MQTT broker host")
    parser.add_argument("--port", type=int, default=6473, help="MQTT broker port (default: 1883)")
    parser.add_argument("--username", default="Jatias", help="MQTT username")
    parser.add_argument("--password", default="Zirobase", help="MQTT password")
    parser.add_argument("--topic", default="dev-prices", help="MQTT topic to subscribe to (default: # [all topics])")
    parser.add_argument("--tls", action="store_true", help="Enable TLS/SSL connection")

    args = parser.parse_args()

    # Create MQTT client instance
    client = mqtt.Client()

    # Set username and password
    client.username_pw_set(args.username, args.password)

    # Enable TLS if requested
    if args.tls:
        client.tls_set()

    # Assign callbacks
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    try:
        print(f"Connecting to {args.host}:{args.port}")
        client.connect(args.host, args.port)

        print("Press Ctrl+C to exit")
        client.loop_forever()

    except KeyboardInterrupt:
        print("\nDisconnecting from broker...")
        client.disconnect()
        print("Disconnected")
    except Exception as e:
        print(f"Error: {e}")
