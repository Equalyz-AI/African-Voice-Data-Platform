import logging
from logging.config import dictConfig

def setup_logging():
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
            },
        },
        "handlers": {
            "file": {
                "class": "logging.FileHandler",
                "filename": "app.log",
                "formatter": "default"
            },
            "console": {
                "class": "logging.StreamHandler",
                "formatter": "default"
            },
        },
        "loggers": {
            "govllminer": {
                "handlers": ["file", "console"],
                "level": "INFO",
                "propagate": False,
            },
            "uvicorn": {
                "level": "WARNING",
            },
            "watchfiles": {
                "level": "WARNING",
            },
        },
    }
    
    dictConfig(LOGGING_CONFIG)
    
    # Explicitly set these to avoid conflicts
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.error").setLevel(logging.WARNING)
    logging.getLogger("watchfiles").setLevel(logging.WARNING)
