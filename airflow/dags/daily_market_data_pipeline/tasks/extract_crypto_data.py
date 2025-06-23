import logging
from datetime import datetime,time
from airflow.operators.python import PythonOperator
import os
import yfinance as yf
import requests
import json
import pandas as pd
from dotenv import load_dotenv


load_dotenv()

#Configure logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)


#fetch ALPHA VANTAGE API KEY from .env
alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY")

#Check if API_KEY is available for ALPHA VANTAGE
if not alpha_vantage_api_key:
    logger.error("Alpha Vantage API key is not set!")
    raise ValueError("Alpha Vantage API key is not set!")


CRYPTO=['BTC/USD', 'ETH/USD']
DATA_DIR='/opt/airflow/dags/raw_data/CRYPTO'


os.makedirs(DATA_DIR,exist_ok=True)

def extract_cypto_data(**context):
    url='https://www.alphavantage.co/query'
    for pair in CRYPTO:
        from_symbol,to_symbol=pair.split('/')
        params={
            "function":"DIGITAL_CURRENCY_DAILY",
            "symbol":from_symbol,
            'market':to_symbol,
            "apikey":alpha_vantage_api_key
        }
        response=requests.get(url,params=params)
        if response.status_code==200:
            data=response.json()

            # Add this to check what's actually being returned
            if "Time Series (Digital Currency Daily)" not in data:
                logger.warning(f"Empty or missing time series for {pair}. Response: {data}")
                logger.debug(json.dumps(data, indent=2))
                continue

            time_series=data.get('Time Series (Digital Currency Daily)',{})
            df=pd.DataFrame.from_dict(time_series,orient='index')

            df.index=pd.to_datetime(df.index)

            file_path=f'{DATA_DIR}/{from_symbol}_{to_symbol}.csv'
            df.to_csv(file_path)
            
            #file_path=f'{DATA_DIR}/{from_symbol}_{to_symbol}.parquet'
            #df.to_parquet(file_path,engine='pyarrow',index=True)

            logger.info(f'Data fetched successfully for {pair}')
        else:
            logger.error((f'Failed to fetch data for {pair}'))



extract_cypto_data_task=PythonOperator(
    task_id='extract_cypto_data',
    python_callable=extract_cypto_data
)
