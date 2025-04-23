from sqlalchemy import create_engine
import json
from config import logger

def load_data(dataframe, DB_CONFIG):
    logger.info('Loading the dataframe to the database.')
    if not dataframe.empty:
        try:
            # for col in dataframe.columns:
            #     if dataframe[col].apply(lambda x: isinstance(x, (dict, list))).any():
            #         dataframe[col] = dataframe[col].apply(json.dumps)

            engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}")
            dataframe.to_sql('spacex_launches', engine, if_exists ='replace', index= False)
            logger.info('Data is successfully loaded into the database.')
        except Exception as e:
            logger.error(f'Problem occured during data loading. {e}')
    else:
        logger.warning('No data to load.')
    