#from airflow.providers.papermill.operators.papermill import PapermillOperator
import logging
#from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dotenv import load_dotenv
import os 

load_dotenv()
sql_password=os.getenv("MSSQL_SA_PASSWORD")
sql_port=os.getenv("MSSQL_PORT")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'kafka:9092'

transform_crypto_data_spark_task=SparkSubmitOperator(
    task_id='transform_crypto_data_script',
    application='/opt/jupyter/notebooks/transform_crypto_data.py',
    name='transform_crypto_data',
    conn_id='spark_default',
    spark_binary='/opt/spark/bin/spark-submit',  # Path to the spark-submit binary
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.1,com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre8",
    repositories="https://repo1.maven.org/maven2",
    application_args=[
        '--spark_master','{{ params.spark_master }}',
        #'--executor-memory','{{ params.executor_memory }}',
        #'--driver-memory','{{ params.driver_memory }}',
        #'--total-executor-cores','{{ params.total_executor_cores }}',
        '--kafka_servers','{{ params.kafka_servers }}',
        '--kafka_topic','crypto_data',
        '--output_path','/opt/airflow/data/processed/crypto/',
        '--checkpoint_path','/opt/airflow/data/checkpoints/crypto/',
        '--execution_date','{{ ds }}',
        '--timeout_minutes', '10'
        # '--sql_server','{{params.sql_server}}',
        # '--sql_database','{{params.sql_database}}',
        # '--sql_port','{{params.sql_port}}',
        # '--sql_username','{{params.sql_username}}',
        # '--sql_password','{{params.sql_password}}',
        # '--enable_sql_write'
    ],
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        '--executor-memory': '{{ params.executor_memory }}',
        '--driver-memory': '{{ params.driver_memory }}',
        '--total-executor-cores': '{{ params.total_executor_cores }}',
        "spark.sql.adaptive.enabled": "false", 
        "spark.hadoop.fs.permissions.umask-mode": "022",
        "spark.sql.streaming.stopGracefullyOnShutdown": "true"
    },
    params={
        'spark_master': 'spark://spark-master:7077',
        'executor_memory': '1g',
        'driver_memory': '1g',
        'total_executor_cores': 1,
        'kafka_servers': KAFKA_BROKER
        # 'sql_server': 'host.docker.internal',
        # 'sql_database': 'Financial_DWH',
        # 'sql_port': sql_port,
        # 'sql_username':'sa',
        # 'sql_password':sql_password
    }
)


