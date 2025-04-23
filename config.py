import os
from dotenv import load_dotenv
import logging

load_dotenv()

SPACEX_API_URL = 'https://api.spacexdata.com'

DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'pass': os.getenv('DB_PASS'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'name': os.getenv('DB_NAME'),
}

logging.basicConfig(
    level=logging.DEBUG,
    format= '%(asctime)s - %(levelname)s - %(message)s',
    handlers= [
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]

)
logger = logging.getLogger()