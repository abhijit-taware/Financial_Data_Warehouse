import json
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer
import logging
import os
import pandas as pd
import time
from airflow.operators.python import PythonOperator


logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)


DATA_DIR_stocks='/opt/airflow/dags/raw_data/stocks'
KAFKA_TOPIC_stock='stock_data'
DATA_DIR_crypto='/opt/airflow/dags/raw_data/CRYPTO'
KAFKA_TOPIC_crypto = 'crypto_data'
KAFKA_BROKER = 'kafka:9092'


def create_kafka_topic(topic_name,num_partitions=1,replication_factor=1,broker='kafka:9092'):
    admin_client=AdminClient({'bootstrap.servers':broker})
    topic_metadata=admin_client.list_topics(timeout=5)

    if topic_name not in topic_metadata.topics:
        new_topic=NewTopic(topic=topic_name,num_partitions=num_partitions,replication_factor=replication_factor)
        fs=admin_client.create_topics([new_topic])
        for topic,f in fs.items():
            try:
                f.result()
                logger.info(f'Topic {topic} created')
            except Exception as e:
                logger.error(f'Failed to create topic {topic}: {e}')
    else:
        logger.info(f'Topic {topic_name} already exists.')


def stream_stock_data_to_kafka(**context):

    create_kafka_topic(KAFKA_TOPIC_stock, broker=KAFKA_BROKER)

    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    def delivery_report_stocks(err,msg):
        if err:
            logger.error(f"Delivery Failed: {err}")
        else:
            logger.info(f'Message delivered to {msg.topic()}')

    file_list_stocks=[
        os.path.splitext(f)[0]
        for f in os.listdir(DATA_DIR_stocks) 
            if os.path.isfile(os.path.join(DATA_DIR_stocks,f))
    ]


    for symbol in file_list_stocks:
        file_path=f'{DATA_DIR_stocks}/{symbol}.csv'
        if os.path.exists(file_path):
            logging.info(f'Streaming file : {file_path}')
            #df=pd.read_parquet(file_path,engine='pyarrow')
            df=pd.read_csv(file_path,header=None, names=["date", "open", "high", "low", "close", "volume"],skiprows=1)
            records=df.to_dict(orient='records')
            for record in records:
                record['symbol']=symbol
                try:
                    producer.produce(KAFKA_TOPIC_stock,json.dumps(record).encode('utf-8'),callback=delivery_report_stocks)
                    producer.poll(0)
                except BufferError:
                    logger.warning("Buffer full, waiting to flush")
                    producer.flush()
                    time.sleep(1)
            producer.flush()



def stream_crypto_data_to_kafka(**context):

    create_kafka_topic(KAFKA_TOPIC_crypto, broker=KAFKA_BROKER)

    producer = Producer({'bootstrap.servers': 'kafka:9092'})
    def delivery_report_crypto(err,msg):
        if err:
            logger.error(f"Delivery Failed: {err}")
        else:
            logger.info(f'Message delivered to {msg.topic()}')

    file_list_crypto=[
        os.path.splitext(f)[0]
        for f in os.listdir(DATA_DIR_crypto) 
            if os.path.isfile(os.path.join(DATA_DIR_crypto,f))
    ]
    
    for symbol in file_list_crypto:
        file_path=f'{DATA_DIR_crypto}/{symbol}.csv'
        if os.path.exists(file_path):
            logging.info(f'Streaming file : {file_path}')
            #df=pd.read_parquet(file_path,engine='pyarrow')
            df=pd.read_csv(file_path,header=None, names=["date", "open", "high", "low", "close", "volume"],skiprows=1)
            records=df.to_dict(orient='records')
            for record in records:
                record['symbol']=symbol
                try:
                    logging.info(f"Producing record: {record}")
                    producer.produce(KAFKA_TOPIC_crypto,json.dumps(record).encode('utf-8'),callback=delivery_report_crypto)
                    producer.poll(0)
                except BufferError:
                    logger.warning("Buffer full, waiting to flush")
                    producer.flush()
                    time.sleep(1)
            producer.flush()

stream_stocks_data_task=PythonOperator(
    task_id='stream_stocks_data_kafka',
    python_callable=stream_stock_data_to_kafka
)

stream_crypto_data_task=PythonOperator(
    task_id='stream_crypto_data_kafka',
    python_callable=stream_crypto_data_to_kafka
)