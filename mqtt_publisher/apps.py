from django.apps import AppConfig
import logging

logger = logging.getLogger(__name__)


class MqttPublisherConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "mqtt_publisher"

    def ready(self):
        # Import here to avoid circular imports
        from .mqtt_client import get_mqtt_client

        # Initialize the MQTT client when Django starts
        try:
            client = get_mqtt_client()
            client.connect()
            logger.info("MQTT client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MQTT client: {str(e)}")
