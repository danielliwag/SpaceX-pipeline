# from sqlalchemy import create_engine
# import psycopg2
# from config import logger

# def load_data(dataframe, DB_CONFIG):
#     logger.info('Loading the dataframe to the database.')

#     if dataframe.empty:
#         logger.warning('No data to load.')
#         return

#     table_name = 'spacex_launches'

#     try:
#         # Step 1: Fetch existing IDs
#         conn = psycopg2.connect(
#             dbname=DB_CONFIG['name'],
#             user=DB_CONFIG['user'],
#             password=DB_CONFIG['pass'],
#             host=DB_CONFIG['host'],
#             port=DB_CONFIG['port']
#         )
#         cur = conn.cursor()
#         cur.execute(f"SELECT id FROM {table_name}")
#         existing_ids = {row[0] for row in cur.fetchall()}
#         cur.close()
#         conn.close()

#         # Step 2: Filter new rows
#         new_df = dataframe[~dataframe['id'].isin(existing_ids)]
#         logger.info(f'{len(new_df)} new rows found to insert.')

#         if new_df.empty:
#             logger.info('No new data to insert.')
#             return

#         # Step 3: Insert new data
#         engine = create_engine(
#             f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['name']}"
#         )
#         new_df.to_sql(table_name, engine, if_exists='append', index=False)
#         logger.info(f'{len(new_df)} new rows successfully inserted into the database.')

#     except Exception as e:
#         logger.error(f'Problem occurred during data loading: {e}')

import psycopg2
from psycopg2.extras import execute_values
from config import logger

def load_data(dataframe, DB_CONFIG):
    logger.info('Starting UPSERT of new data to the database.')

    if dataframe.empty:
        logger.warning('No data to load.')
        return
    
    table_name = 'spacex_launches'
    
    # Use all columns in the dataframe
    insert_columns = list(dataframe.columns)
    insert_values = [tuple(row) for row in dataframe[insert_columns].values]
    
    quoted_columns = ', '.join(f'"{col}"' for col in insert_columns)

    # Prepare SQL for UPSERT
    insert_query = f"""
        INSERT INTO {table_name} ({quoted_columns})
        VALUES %s
        ON CONFLICT (id) DO NOTHING;
    """
    
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['name'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['pass'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port']
        )
        cur = conn.cursor()
        execute_values(cur, insert_query, insert_values)
        conn.commit()
        logger.info(f"{len(insert_values)} rows attempted to insert with UPSERT.")
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"UPSERT failed: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()
