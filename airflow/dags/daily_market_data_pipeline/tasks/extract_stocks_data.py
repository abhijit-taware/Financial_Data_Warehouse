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


STOCKS=['AAPL','MSFT']
DATA_DIR='/opt/airflow/dags/raw_data/stocks'


os.makedirs(DATA_DIR,exist_ok=True)



def extract_stocks_data(**context):
    #GEt market status from upstream task 
    task_instance=context["task_instance"]
    market_status=task_instance.xcom_pull(task_ids="check_market_status",key="market_status")

    logging.info(f"Market Status recieved: {market_status}")

    if market_status['NYSE']["is_open"]:

        url='https://www.alphavantage.co/query'
        for symbol in STOCKS:
            params={
                "function":"TIME_SERIES_DAILY",
                "symbol":symbol,
                "apikey":alpha_vantage_api_key
            }
            response=requests.get(url,params=params)
            if response.status_code==200:
                data=response.json()

                # Add this to check what's actually being returned
                if "Time Series (Daily)" not in data:
                    logger.warning(f"Empty or missing time series for {symbol}. Response: {data}")
                    logger.debug(json.dumps(data, indent=2))
                    continue

                time_series=data.get('Time Series (Daily)',{})
                df=pd.DataFrame.from_dict(time_series,orient='index')

                df.index=pd.to_datetime(df.index)
                
                file_path=f'{DATA_DIR}/{symbol}.csv'
                df.to_csv(file_path)

                #file_path=f'{DATA_DIR}/{symbol}.parquet'
                #df.to_parquet(file_path,engine='pyarrow',index=True)

                logger.info(f'Data fetched successfully for {symbol}')
            else:
                logger.error((f'Failed to fetch data for {symbol}'))
    else:
        logger.error('Market is closed.')

def extract_stocks_data_yahoo(**context):
    for symbol in STOCKS:
        df=yf.download(symbol,period='1d',interval='1d')
        df.to_csv(f'{DATA_DIR}/yahoo/{symbol}.csv')


extract_stocks_data_task=PythonOperator(
    task_id='extract_stocks_data',
    python_callable=extract_stocks_data
)

extract_stocks_data_yahoo_task=PythonOperator(
    task_id='extract_stocks_data_yahoo',
    python_callable=extract_stocks_data_yahoo
)