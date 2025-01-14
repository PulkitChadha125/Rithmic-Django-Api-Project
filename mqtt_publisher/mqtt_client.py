import paho.mqtt.client as mqtt
import json
import logging
import ssl
from django.conf import settings
from threading import Lock

logger = logging.getLogger("rithmic")


class SingletonMQTTClient:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance


class MQTTClient(SingletonMQTTClient):
    def __init__(self):
        # Only initialize if it hasn't been initialized
        if not hasattr(self, "client"):
            # Get configuration from Django settings
            self.config = settings.MQTT_CONFIG

            # Initialize MQTT client with clean session setting
            self.client = mqtt.Client(clean_session=self.config.get("CLEAN_SESSION", True))

            # Set up authentication
            if self.config.get("USERNAME") and self.config.get("PASSWORD"):
                self.client.username_pw_set(username=self.config["USERNAME"], password=self.config["PASSWORD"])

            # Set up TLS if enabled
            if self.config.get("USE_TLS", False):
                self.setup_tls()

            # Set up callbacks
            self.client.on_connect = self.on_connect
            self.client.on_publish = self.on_publish
            self.client.on_disconnect = self.on_disconnect

            self._is_connected = False
            self._last_error = None

    def setup_tls(self):
        """Configure TLS/SSL settings for the MQTT client"""
        try:
            # Configure TLS
            self.client.tls_set(
                ca_certs=self.config.get("TLS_CERT_PATH"),
                cert_reqs=ssl.CERT_REQUIRED,
                tls_version=ssl.PROTOCOL_TLS,
                ciphers=None,
            )

            # Disable hostname checking if needed
            self.client.tls_insecure_set(False)

        except Exception as e:
            logger.error(f"Error setting up TLS: {str(e)}")
            raise

    def on_connect(self, client, userdata, flags, rc):
        """Callback for when the client connects to the broker"""
        connect_codes = {
            0: "Connected successfully",
            1: "Connection refused - incorrect protocol version",
            2: "Connection refused - invalid client identifier",
            3: "Connection refused - server unavailable",
            4: "Connection refused - bad username or password",
            5: "Connection refused - not authorized",
        }

        if rc == 0:
            self._is_connected = True
            self._last_error = None
            logger.info("Connected to MQTT broker successfully")
        else:
            self._is_connected = False
            self._last_error = connect_codes.get(rc, f"Unknown error code: {rc}")
            logger.error(f"Failed to connect to MQTT broker: {self._last_error}")

    def on_publish(self, client, userdata, mid):
        pass
        # logger.info(f"Message {mid} published successfully")

    def on_disconnect(self, client, userdata, rc):
        self._is_connected = False
        if rc != 0:
            self._last_error = "Unexpected disconnection"
            logger.warning("Unexpected disconnection from MQTT broker")
        else:
            self._last_error = None
            logger.info("Disconnected from MQTT broker")

    def connect(self):
        """Connect to the MQTT broker"""
        try:
            self.client.connect(
                self.config["BROKER_URL"], self.config["BROKER_PORT"], keepalive=self.config["KEEP_ALIVE"]
            )
            self.client.loop_start()
        except Exception as e:
            self._last_error = str(e)
            logger.error(f"Error connecting to MQTT broker: {str(e)}")
            raise

    def disconnect(self):
        """Disconnect from the MQTT broker"""
        self.client.loop_stop()
        self.client.disconnect()
        self._is_connected = False

    def is_connected(self):
        """Check if client is currently connected"""
        return self._is_connected

    def get_last_error(self):
        """Get the last error message"""
        return self._last_error

    def publish_message(self, topic, payload):
        """
        Publish a JSON message to the specified topic

        Args:
            topic (str): The MQTT topic to publish to
            payload (dict): The message payload as a Python dictionary

        Returns:
            tuple: (success: bool, error_message: str or None)
        """
        if not self.is_connected():
            return False, "MQTT client is not connected"

        try:
            # Convert payload to JSON string
            json_payload = json.dumps(payload)

            # Publish the message
            for tp in topic:
                result = self.client.publish(tp, json_payload, qos=self.config["QOS_LEVEL"])

                # Check if the message was queued
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    error_msg = mqtt.error_string(result.rc)
                    logger.error(f"Failed to publish message: {error_msg}")
                    return False, error_msg

                # Wait for the message to be published
                result.wait_for_publish()
            return True, None

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error publishing message: {error_msg}")
            return False, error_msg


_mqtt_client = None


def get_mqtt_client():
    """
    Get the singleton instance of the MQTT client
    """
    global _mqtt_client
    if _mqtt_client is None:
        _mqtt_client = MQTTClient()
    return _mqtt_client
