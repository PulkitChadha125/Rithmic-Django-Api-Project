from threading import Thread
from django.db import connections
from functools import wraps
import logging

logger = logging.getLogger("rithmic")


def run_in_background(func):
    """
    Decorator to run a function in a background thread.
    Handles database connection cleanup properly.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        def clean_db_wrapper():
            try:
                func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Background thread error: {str(e)}")
            finally:
                # Close database connections after thread completes
                connections.close_all()

        thread = Thread(target=clean_db_wrapper)
        thread.daemon = True  # Thread will be terminated when main program exits
        thread.start()
        return thread

    return wrapper
