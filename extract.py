import requests
from config import logger


def extract_data(url):
    logger.info('Extracting data from API')
    try:
        res = requests.get(url)
        data = res.json()
        logger.info('Data ingestion successfully completed')
        return data
    except Exception as e:
        logger.error(f'Problem occured during data ingestion: {e}')