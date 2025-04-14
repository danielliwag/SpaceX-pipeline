from config import db_config, logger, url, rocketurl, launchpadurl
from extract import extract_data
from transform import transform_data, fetch_name_detail
from load import load_data


def run_etl_job():
    logger.info('ETL Job Starting')
    try:
        data = extract_data(url)
        dataframe = transform_data(data)
        cleaned_dataframe = fetch_name_detail(dataframe, 'name', 'rocket', rocketurl)
        cleaned_dataframe = fetch_name_detail(cleaned_dataframe, 'name', 'launchpad', launchpadurl)
        print(cleaned_dataframe['failures'])
        load_data(cleaned_dataframe, db_config)
        

    except Exception as e:
        logger.error(f'Error occured during ETL job - {e}')


if __name__ == '__main__':
    run_etl_job()