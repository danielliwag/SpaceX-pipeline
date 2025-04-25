from sqlalchemy import create_engine
import psycopg2
from config import logger

def load_data(dataframe, DB_CONFIG):
    logger.info('Loading the dataframe to the database.')

    if dataframe.empty:
        logger.warning('No data to load.')
        return

    table_name = 'spacex_launches'

    try:
        # Step 1: Fetch existing IDs
        conn = psycopg2.connect(
            dbname=DB_CONFIG['name'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['pass'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        cur = conn.cursor()
        cur.execute(f"SELECT id FROM {table_name}")
        existing_ids = {row[0] for row in cur.fetchall()}
        cur.close()
        conn.close()

        # Step 2: Filter new rows
        new_df = dataframe[~dataframe['id'].isin(existing_ids)]
        logger.info(f'{len(new_df)} new rows found to insert.')

        if new_df.empty:
            logger.info('No new data to insert.')
            return

        # Step 3: Insert new data
        engine = create_engine(
            f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
        )
        new_df.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f'{len(new_df)} new rows successfully inserted into the database.')

    except Exception as e:
        logger.error(f'Problem occurred during data loading: {e}')
