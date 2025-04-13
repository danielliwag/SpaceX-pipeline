import requests
from config import logger

url = 'https://api.spacexdata.com/v5/launches'


def extract_data(url):
    logger.info('Extracting data from API')
    try:
        response = requests.get(url)
        data = response.json()
        return data
    except Exception as e:
        logger.error(f'Problem occured during data ingestion: {e}')