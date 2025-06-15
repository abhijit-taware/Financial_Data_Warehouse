import logging
from airflow.models import Connection
from airflow import settings
from airflow.operators.python import PythonOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def set_spark_default_connection(**kwargs):
    conn = Connection(
    conn_id='spark_default',
    conn_type='spark',
    host='spark://spark-master:7077'
    )

    session = settings.Session()
    if not session.query(Connection).filter_by(conn_id='spark_default').first():
        session.add(conn)
        session.commit()
        logger.info("Connection spark_default added.")
    else:
        logger.info("Connection spark_default already exists.")

set_spark_default_connection_task=PythonOperator(
    task_id='set_spark_default_connection',
    python_callable=set_spark_default_connection
)


