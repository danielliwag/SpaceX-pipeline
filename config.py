import os
from dotenv import load_dotenv
import logging

load_dotenv()







logging.basicConfig(
    level=logging.INFO,
    format= '%(asctime)s - %(levelname)s - %(message)s',
    handlers= [
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler()
    ]

)
logger = logging.getLogger()