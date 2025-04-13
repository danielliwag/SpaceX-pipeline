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
            return df
    
        else:
            logger.error('The dataframe is empty')

    except Exception as e:
        logger.error('No data to transform.')


url = 'https://api.spacexdata.com/v5/launches'
data = transform_data(extract_data(url))


def fetch_name_detail(df, name, column_id, url):
    name_ids = df[f'{column_id}'].unique()
    
    name_id_lookup = {}
    for nid in name_ids:
        try:
            nid = nid.strip()
            res = requests.get(f'{url}/{nid}')
            print(res.json())
            res.raise_for_status()
            name_id_lookup[nid] = res.json().get(name, None)
        except Exception as e:
            logger.warning(f'Launchpad ID failed: {nid} - {e}')
            name_id_lookup[nid] = None

    df[f'{column_id}_name'] = df[f'{column_id}'].map(name_id_lookup)
    return df


trans_data = fetch_name_detail(data, 'name', 'rocket', 'https://api.spacexdata.com/v4/rockets')
print(trans_data['rocket_name'].value_counts())