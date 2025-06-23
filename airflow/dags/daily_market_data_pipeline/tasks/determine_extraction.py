from airflow.operators.python import PythonOperator
import logging
from datetime import datetime,time


#configure logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

def determine_extraction_flow(**context):

    #GEt market status from upstream task 
    task_instance=context["task_instance"]
    market_status=task_instance.xcom_pull(task_ids="check_market_status",key="market_status")

    logging.info(f"Market Status recieved: {market_status}")

    #Crypto always runs(no market dependency)
    tasks_to_run=['extract_crypto_data']

    #stock extraction only if market is open
    if market_status['NYSE']["is_open"]:
        tasks_to_run.append("extract_stocks_data")
        logging.info("Market is open- will extract both stocks and crypto data")
    else:
        logging.info("Market is closed - will extract only crypto data")

    logging.info(f"Tasks to execute: {tasks_to_run}")

    return tasks_to_run

determine_extraction_flow_task=PythonOperator(
    task_id='determine_extraction_flow',
        python_callable=determine_extraction_flow,
)