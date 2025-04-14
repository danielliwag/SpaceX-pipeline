from config import logger
import pandas as pd
from extract import extract_data
import requests


def transform_data(dataframe):
    try:
        if dataframe:
            df = pd.json_normalize(dataframe)
            df['name'] = df['name'].str.strip()
            df['date_utc'] = pd.to_datetime(df['date_utc'])
            df['success'] = df['success'].astype(bool)
            df.columns = df.columns.str.replace('.', '_', regex=False)
            return df
    
        else:
            logger.warning('No data to transform.')

    except Exception as e:
        logger.error(f'Problem during data transformation: {e}')


def fetch_name_detail(df, name, column_id, url):
    name_ids = df[f'{column_id}'].unique()
    
    name_id_lookup = {}
    for nid in name_ids:
        try:
            nid = nid.strip()
            res = requests.get(f'{url}/{nid}')
            res.raise_for_status()
            name_id_lookup[nid] = res.json().get(name, None)
        except Exception as e:
            logger.warning(f'Launchpad ID failed: {nid} - {e}')
            name_id_lookup[nid] = None

    df[f'{column_id}_name'] = df[f'{column_id}'].map(name_id_lookup)
    return df