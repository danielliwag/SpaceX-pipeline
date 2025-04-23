from config import DB_CONFIG, logger, SPACEX_API_URL
from extract import extract_data
from transform import transform_data
from load import load_data


def run_etl_job():
    logger.info('ETL Job Starting')
    try:
        data = extract_data(f'{SPACEX_API_URL}/v5/launches')
        cleaned_data = transform_data(data)
        load_data(cleaned_data, DB_CONFIG)
        

    except Exception as e:
        logger.error(f'Error occured during ETL job - {e}')

    logger.info('ETL Job finished')

if __name__ == '__main__':
    run_etl_job()