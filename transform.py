import requests
import pandas as pd
import json
from config import logger, SPACEX_API_URL


def fetch_lookup_maps():
    try:
        rockets = requests.get(f"{SPACEX_API_URL}/v4/rockets").json()
        launchpads = requests.get(f"{SPACEX_API_URL}/v4/launchpads").json()

        rocket_map = {r['id']: r['name'] for r in rockets}
        launchpad_map = {l['id']: l['full_name'] for l in launchpads}

        return rocket_map, launchpad_map

    except Exception as e:
        logger.error(f"Error fetching rocket/launchpad lookups: {e}")
        return {}, {}


def transform_data(raw_data):
    if not raw_data:
        logger.warning("No data received for transformation.")
        return pd.DataFrame()

    try:
        df = pd.json_normalize(raw_data)

        logger.info("Cleaning and transforming launch data.")

        df['name'] = df['name'].str.strip()
        df['date_utc'] = pd.to_datetime(df['date_utc'], errors='coerce')
        df['success'] = df['success'].astype('boolean')

        # Fetch lookup maps
        rocket_map, launchpad_map = fetch_lookup_maps()

        df['rocket_name'] = df['rocket'].map(rocket_map)
        df['launchpad_location'] = df['launchpad'].map(launchpad_map)

        # Flatten nested json
        list_columns = ['flickr_images', 'capsules', 'payloads', 'ships', 'crew', 'failures']
        for col in list_columns:
            full_col = 'links.flickr.original' if col == 'flickr_images' else col
            if full_col in df.columns:
                def safe_join(val):
                    if isinstance(val, list):
                        if all(isinstance(item, dict) for item in val):
                            return ', '.join(str(item.get('id', item)) for item in val)
                        return ', '.join(str(item) for item in val)
                    return ''
                
                df[col] = df[full_col].apply(safe_join)
                if col != full_col:
                    df.drop(columns=[full_col], inplace=True)

        def extract_core_ids(core_list):
            if isinstance(core_list, list):
                return ', '.join(str(c.get('core')) for c in core_list if isinstance(c, dict) and c.get('core'))
            return ''

        df['core_ids'] = df['cores'].apply(extract_core_ids)

        # Drop unnecessary columns
        drop_cols = ['rocket', 'launchpad', 'cores']
        df.drop(columns=[col for col in drop_cols if col in df.columns], inplace=True)

        # Convert all remaining dict-type columns to JSON strings
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x)

        # Convert lists of dicts to JSON strings
        for col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) and any(isinstance(i, dict) for i in x) else x)
        # Replace NaN with None so psycopg2 maps it to SQL NULL
        df = df.where(pd.notnull(df), None)

        # Ensure booleans stay booleans by removing NaN
        bool_columns = ['fairings.reused', 'fairings.recovery_attempt', 'fairings.recovered']
        for col in bool_columns:
            if col in df.columns:
                df[col] = df[col].where(pd.notnull(df[col]), None)

        logger.info("Transformation complete.")
        return df

    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        return pd.DataFrame()