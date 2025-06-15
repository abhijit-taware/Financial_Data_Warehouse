#from airflow.providers.papermill.operators.papermill import PapermillOperator
import logging
#from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BROKER = 'kafka:9092'

transform_crypto_data_spark_task=SparkSubmitOperator(
    task_id='transform_crypto_data_script',
    application='/opt/jupyter/notebooks/transform_crypto_data.py',
    name='transform_crypto_data',
    conn_id='spark_default',
    spark_binary='/opt/spark/bin/spark-submit',  # Path to the spark-submit binary
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-core_2.12:2.4.0",
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
        '--execution_date','{{ ds }}'
    ],
    conf={
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        '--executor-memory': '{{ params.executor_memory }}',
        '--driver-memory': '{{ params.driver_memory }}',
        '--total-executor-cores': '{{ params.total_executor_cores }}',
    },
    params={
        'spark_master': 'spark://spark-master:7077',
        'executor_memory': '2g',
        'driver_memory': '1g',
        'total_executor_cores': 2,
        'kafka_servers': KAFKA_BROKER
    }
)


'''transform_crypto_data_spark_task = BashOperator(
    task_id='transform_crypto_data_script',
    bash_command="""
    /opt/spark/bin/spark-submit \
        --spark_master '{{ params.spark_master }}' \
        --executor-memory '{{ params.executor_memory }}' \
        --driver-memory '{{ params.driver_memory }}' \
        --total-executor-cores '{{ params.total_executor_cores }}' \
        /opt/jupyter/notebooks/transform_crypto_data.py \
        --kafka_servers '{{ params.kafka_servers }}' \
        --kafka_topic 'crypto_data' \
        --output_path '/opt/airflow/data/processed/crypto/' \
        --checkpoint_path '/opt/airflow/data/checkpoints/crypto/' \
        --execution_date '{{ ds }}'
    """,
    params={
        'spark_master': 'spark://67b6206694bc:7077',
        'executor_memory': '2g',
        'driver_memory': '1g',
        'total_executor_cores': '2',
        'kafka_servers': KAFKA_BROKER
    }
)'''


'''
transform_crypto_data_spark_task = BashOperator(
    task_id='transform_crypto_data_script',
    bash_command="""
    python /opt/jupyter/notebooks/transform_crypto_data.py \
        --kafka_servers '{{ params.kafka_servers }}' \
        --kafka_topic 'crypto_data' \
        --output_path '/opt/airflow/data/processed/crypto/' \
        --checkpoint_path '/opt/airflow/data/checkpoints/crypto/' \
        --execution_date '{{ ds }}' \
        --executor_memory '2g' \
        --driver_memory '1g' \
        --total_executor_cores '2' \
        --spark_master 'spark://0c7afcf933a9:7077'
    """,
    params={
        'kafka_servers': KAFKA_BROKER  # Ensure this is defined earlier in your DAG file
    }
)
'''

#TO RUN the dag using .ipynb (pytin notebook)
'''
transform_crypto_data_spark_task = PapermillOperator(
    task_id='transform_crypto_data_notebook',
    input_nb='/opt/jupyter/notebooks/transform_crypto_data.ipynb',
    output_nb='/opt/jupyter/notebooks/output/transform_crypto_data_{{ ds }}.ipynb',
    parameters={
        'kafka_servers': KAFKA_BROKER,
        'kafka_topic': 'crypto_data',
        'output_path': '/opt/airflow/data/processed/crypto/',
        'checkpoint_path': '/opt/airflow/data/checkpoints/crypto/',
        'execution_date': '{{ ds }}',
        'executor_memory': '2g',
        'driver_memory': '1g',
        'total_executor_cores': 2,
        'spark_master':'spark://fced0d436c3f:7077'
    }
)
'''


