from config import logger
import pandas as pd
from extract import extract_data

def transform_data(dataframe):
    try:
        if dataframe:
            df = pd.json_normalize(dataframe)

            return df
    
        else:
            logger.error('The dataframe is empty')

    except Exception as e:
        logger.error('No data to transform.')


url = 'https://api.spacexdata.com/v5/launches'
data = transform_data(extract_data(url))
print(data['success'])