from config import logger
import pandas as pd
import requests


def transform_data(dataframe):
    try:
        if dataframe:
            df = pd.json_normalize(dataframe)
            df['launch_id'] = df['id']
            df['name'] = df['name'].str.strip()
            df['date_utc'] = pd.to_datetime(df['date_utc'])
            df['success'] = df['success'].astype(bool)
            df['links.flickr.original'] = df['links.flickr.original'].apply(lambda x: ', '.join(x))
            df['capsules'] = df['capsules'].apply(lambda x: ', '.join(x))
            df['payloads'] = df['payloads'].apply(lambda x: ', '.join(x))

            cores_df = pd.json_normalize(dataframe, record_path='cores', meta= ['id'], sep='_')
            cores_df['launch_id'] = cores_df['id']
            cores_df.drop(columns=['id'], inplace= True)

            failures_df = pd.json_normalize(dataframe, record_path='failures', meta=['id'], sep='_')
            failures_df.rename(columns={'id': 'launch_id'}, inplace= True)

            crew_records = []
            for launch in dataframe:
                if launch.get("crew"):
                    for crew_id in launch["crew"]:
                        crew_records.append({
                            "launch_id": launch["id"],
                            "crew_id": crew_id
                        })
            crew_df = pd.DataFrame(crew_records)
            
            merged_df = pd.merge(df, cores_df, on='launch_id', how='left')
            merged_df = pd.merge(merged_df, failures_df, on='launch_id', how='left')
            merged_df = pd.merge(merged_df, crew_df, on='launch_id', how='left')
            return merged_df
    
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