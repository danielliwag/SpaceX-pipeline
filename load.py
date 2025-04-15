from sqlalchemy import create_engine
import json
from config import logger

def load_data(dataframe, db_config):
    logger.info('Loading the dataframe to the database.')
    if not dataframe.empty:
        try:
            for col in dataframe.columns:
                if dataframe[col].apply(lambda x: isinstance(x, (dict, list))).any():
                    dataframe[col] = dataframe[col].apply(json.dumps)

            engine = create_engine(f"postgresql://{db_config['user']}:{db_config['pass']}@{db_config['host']}:{db_config['port']}/{db_config['name']}")
            dataframe.to_sql('spacex_launches', engine, if_exists ='replace', index= False)
        except Exception as e:
            logger.error(f'Problem occured during data loading. {e}')
    else:
        logger.warning('No data to load.')
    logger.info('Data is successfully loaded into the database.')