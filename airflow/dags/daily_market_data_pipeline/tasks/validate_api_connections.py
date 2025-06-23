import requests
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import logging

#configure logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

load_dotenv()

#fetch ALPHA VANTAGE API KEY from .env
alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY")

#Check if API_KEY is available for ALPHA VANTAGE
if not alpha_vantage_api_key:
    logger.error("Alpha Vantage API key is not set!")
    raise ValueError("Alpha Vantage API key is not set!")

#API endpoints configuration
API_ENDPOINTS = {
    'alpha_vantage': {
        "url":'https://www.alphavantage.co/query',
        "params":{
            "function":"TIME_SERIES_DAILY",
            "symbol":"AAPL",
            "apikey":alpha_vantage_api_key
        }
    },
    'binance': {
        "url":'https://api.binance.com/api/v3/ticker/price',
        "params":{"symbol":"BTCUSDT"}
    }
}

def check_api_endpoints(**context):

    api_status={}

    for api_name,config in API_ENDPOINTS.items():
        try:
            url=config["url"]
            params=config["params"]


            '''params={
                "function":"TIME_SERIES_DAILY",
                "symbol":"AAPL",
                "apikey":alpha_vantage_api_key
            }'''
            #Get the response for configured api along with params
            response=requests.get(url,params=params)
            if response.status_code!=200:
                logger.error(f"{api_name} API failed with status {response.status_code}")
                raise Exception(f"{api_name} API failed with status {response.status_code}")
            
            data=response.json()

            if api_name=="alpha_vantage" and "Time Series (Daily)" not in data:
                logger.error(f"{api_name} response invalid: {data}")
                raise Exception(f"{api_name} response invalid: {data}")
            
            if api_name=="binance" and  "price" not in data:
                logger.error(f"{api_name} response invalid: {data}")
                raise Exception(f"{api_name} response invalid: {data}")
            
            api_status[api_name]={"is_valid":True}
            logging.info(f"{api_name} validated successfully.")

        except Exception as e:
            logger.error(f"Error checking {api_name} status: {str(e)}")
            api_status[api_name] = {"is_valid":False,'error':str(e)}

    #Store result in xcom for downstream tasks
    context["task_instance"].xcom_push(key="api_status",value=api_status)
    return api_status


validate_api_task = PythonOperator(
    task_id="check_api_endpoints",
    python_callable=check_api_endpoints,
)
                        

   



'''

def check_api_endpoints():
    alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY")
    if not alpha_vantage_api_key:
        logger.error("Alpha Vantage API key is not set!")
        raise ValueError("Alpha Vantage API key is not set!")

    params={
        "function":"TIME_SERIES_DAILY",
        "symbol":"AAPL",
        "apikey":alpha_vantage_api_key
    }
    response=requests.get(API_ENDPOINTS["stock_api"],params=params)
    if response.status_code!=200:
        logger.error(f"Alpha Vantage API failed with status {response.status_code}")
        raise Exception(f"Alpha Vantage API failed with status {response.status_code}")
    data=response.json()
    if "Time Series (Daily)" not in data:
        logger.error(f"Alpha Vantage response invalid: {data}")
        raise Exception(f"Alpha Vantage response invalid: {data}")
    

    #check binance api
    crypt_params={"symbol":"BTCUSDT"}

    crypt_response=requests.get(API_ENDPOINTS["crypto_api"],params=crypt_params)
    if crypt_response.status_code!=200:
        logger.error(f"Binance API failed with status {crypt_response.status_code}")
        raise Exception(f"Binance API failed with status {crypt_response.status_code}")
    data=crypt_response.json()
    if "price" not in data:
        logger.error(f"Binance response invalid: missing price: {data}")
        raise Exception(f"Binance response invalid: missing price: {data}")
'''