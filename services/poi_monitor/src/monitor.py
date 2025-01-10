import os
import time
import logging
from logging.config import dictConfig
from dotenv import load_dotenv
from .database import Database
from .notification import SlackNotifier
from .analyzer import PoiAnalyzer

# Load environment variables
load_dotenv()

# Configure logging
logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'json': {
            '()': 'pythonjsonlogger.jsonlogger.JsonFormatter',
            'format': '%(asctime)s %(levelname)s %(name)s %(message)s'
        }
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'json'
        }
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO'
    }
}

dictConfig(logging_config)
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting POI Monitor service...")
    
    # Initialize components
    try:
        db = Database()
        notifier = SlackNotifier()
        analyzer = PoiAnalyzer(db, notifier)
        logger.info("Successfully initialized all components")
    except Exception as e:
        logger.error(f"Failed to initialize components: {str(e)}")
        raise
    
    try:
        while True:
            logger.info("Running POI check iteration")
            analyzer.process_new_submissions()
            time.sleep(int(os.getenv('CHECK_INTERVAL', 300)))  # Default 5 minutes
            
    except KeyboardInterrupt:
        logger.info("Shutting down POI Monitor service...")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
