from datetime import datetime,timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from daily_market_data_pipeline.tasks.check_market_hours import check_market_task
from daily_market_data_pipeline.tasks.validate_api_connections import validate_api_task
from daily_market_data_pipeline.tasks.determine_extraction import determine_extraction_flow_task
from daily_market_data_pipeline.tasks.extract_stocks_data import extract_stocks_data_task
from daily_market_data_pipeline.tasks.extract_crypto_data import extract_cypto_data_task
from daily_market_data_pipeline.tasks.stream_data_using_kafka import stream_stocks_data_task,stream_crypto_data_task
from daily_market_data_pipeline.tasks.transform_crypto_data_spark import transform_crypto_data_spark_task
from daily_market_data_pipeline.tasks.create_spark_default_conn import set_spark_default_connection_task

#default arguments foor the DAG
default_args={
    'owner':'FTDWH',
    'depends_on_past':False,
    'start_date':days_ago(1),
    'email_on_failure':True,
    'email_on_retry':False,
    'retries':2,
    'retry_delay':timedelta(minutes=5),
    'catchup':False
}

#Create the DAG
with DAG (
    dag_id='daily_market_data_pipeline',
    default_args=default_args,
    description='Check market hours and trigger appropriate data pipeline tasks',
    schedule_interval='*/15 * * * *', #Run every 15 minutes
    max_active_runs=1,
    tags=['finance','market-hours','data-warehouse']
) as dag:
    check_market_task.dag=dag
    validate_api_task.dag=dag
    determine_extraction_flow_task.dag=dag
    extract_stocks_data_task.dag=dag
    extract_cypto_data_task.dag=dag
    stream_stocks_data_task.dag=dag
    stream_crypto_data_task.dag=dag
    transform_crypto_data_spark_task.dag=dag
    set_spark_default_connection_task.dag=dag

check_market_task >> validate_api_task >> set_spark_default_connection_task >>determine_extraction_flow_task >> [extract_stocks_data_task,extract_cypto_data_task]

extract_stocks_data_task >> stream_stocks_data_task

extract_cypto_data_task >> stream_crypto_data_task >> transform_crypto_data_spark_task