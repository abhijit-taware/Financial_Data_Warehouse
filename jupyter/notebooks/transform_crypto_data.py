# %%
#Spark Master
#spark_master='spark://41cf611846e0:7077'

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging
import sys
import argparse
from delta import configure_spark_with_delta_pip
from functools import partial 
from pyspark.sql import Window

# %%
#Configure Logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

# %%
#def create_spark_session(spark_master,executor_memory, driver_memory, total_executor_cores):
def create_spark_session(spark_master):
    spark=SparkSession.builder\
            .appName("CryptoDataTransformation")\
            .master(spark_master)\
            .config("spark.ui.port", "4040")\
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")\
            #.config("spark.executor.memory", executor_memory) \
            #.config("spark.driver.memory", driver_memory) \
            #.config("spark.executor.cores", total_executor_cores) \

    return configure_spark_with_delta_pip(spark).getOrCreate()


# %%
def define_crypto_schema():
    '''Define schema for crypto data'''
    return StructType([
        StructField("symbol", StringType(), True),
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", DoubleType(), True),
    ])

# %%
def read_kafka_crypto_stream(spark,kafka_server,kafka_topic):
    """Read crypto data from kafka"""
    return spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", kafka_server)\
    .option("subscribe", kafka_topic)\
    .option("startingOffsets", "latest")\
    .option("failOnDataLoss", "false")\
    .load()

# %%
def parse_crypto_json_data(df):
    """Parse JSON crypto data from Kafka"""
    crypto_schema=define_crypto_schema()

    json_df=df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df=json_df.select(from_json(col("json_str"), crypto_schema).alias("data")).select("data.*")

    #return df.select(col("symbol"),col("date"),col("open"),col("high"),col("low"),col("close"),col("volume"))
    #logger.info(parsed_df)
    return parsed_df

# %%
def apply_crypto_transformations(df):
    """Apply business logic transformations to crypto data"""
    logger.info("Applying transformations to crypto data")
    return df\
        .withColumn("processed_timestamp",current_timestamp())\
        .withColumn("processing_date",current_date())\
        .withColumn("volume_rank",dense_rank().over(Window.orderBy(col("volume"))))
        

# %%
def apply_crypto_quality_checks(df):
    """Apply  data quality checks to crypto data"""
    logger.info("Applying data quality checks to crypto data")
    return df\
        .withColumn("data_quality_flag",struct(
            when(col("symbol").isNull(), "missing_symbol")
        )).withColumn("data_quality_status",when(col("data_quality_flag").isNull(), "Passed").otherwise("Failed"))

# %%
def write_to_delta_with_partitions(df,output_path,partition_cols=None):
    """Write DataFrame to Delta lake with Partitioning"""
    logger.info(f"Writing data to Delta lake at {output_path} with partitions {partition_cols}")
    write=df.write\
        .format("delta")\
        .mode("append")\
        .option("overwriteSchema", "true")
    if partition_cols:
        write=write.partitionBy(*partition_cols)
    
    return write.save(output_path)

# %%
def process_crypto_batch(df,epoch_id,output_path):
    """Process each batch of crypto data"""
    try:
        logger.info(f"Processing crypto batch {epoch_id}")

        if df.count() == 0:
            logger.info(f"No data in batch {epoch_id}, skipping")
            return
        
        #Appply transformations and quality checks
        transformed_df=apply_crypto_transformations(df)
        quality_checked_df=apply_crypto_quality_checks(transformed_df)

        #Seperate data by quality
        clean_data=quality_checked_df.filter(col("data_quality_status") == "Passed")
        failed_data=quality_checked_df.filter(col("data_quality_status") == "Failed")

        #write clean data 
        if clean_data.count()>0:
            write_to_delta_with_partitions(clean_data,output_path["clean_data"],["symbol"])
        
        if failed_data.count()>0:
            write_to_delta_with_partitions(failed_data,output_path["failed_data"],["symbol"])

        logger.info(f"Batch {epoch_id} processed: {clean_data.count()} clean , {failed_data.count()} failed")    
    except Exception as e :
        logger.error(f"Error processing crypto batch {epoch_id}: {str(e)}")

def foreach_batch_function(df, epoch_id, output_path):
    """Function to be used with foreachBatch"""
    logger.info(f'Count of recorded received in df : {df.count()}')
    if df.count()>0:
        process_crypto_batch(df, epoch_id, output_path)
        logger.info(f"Batch {epoch_id} processed successfully")
    else:
        logger.info(f"No data in batch {epoch_id}, skipping processing")
 

# %%
def main():
    parser =argparse.ArgumentParser(description='Transform crypto_data from kafka')
    parser.add_argument('--kafka_servers',required=True,help='kafka bootstrap servers')
    parser.add_argument('--kafka_topic',required=True,help='kafka topic name')
    parser.add_argument('--output_path',required=True,help='output path for processed data')
    parser.add_argument('--checkpoint_path',required=True,help='checkpoint path for streaming')
    parser.add_argument('--execution_date',required=True,help='Airflow execution date')
    #parser.add_argument('--executor_memory',default='2g',help='Executor memory')
    #parser.add_argument('--driver_memory',default='1g',help='Driver memory')
    #parser.add_argument('--total_executor_cores',default=2,help='Total executor cores')
    parser.add_argument('--spark_master',default='',help='Spark master URL')

    args=parser.parse_args()

    #Create Spark Session

    #spark=create_spark_session(args.spark_master,args.executor_memory, args.driver_memory, args.total_executor_cores)
    spark=create_spark_session(args.spark_master)
    spark.sparkContext.setLogLevel("WARN")

    #define output paths
    output_path={
        "clean_data":f"{args.output_path}/clean_data",
        "failed_data":f"{args.output_path}/failed_data"
    }

    try:
        #Read kafka data stream
        kafka_df=read_kafka_crypto_stream(spark,args.kafka_servers,args.kafka_topic)

        logger.info(f"Kafka stream started from topic {args.kafka_topic} on servers {args.kafka_servers}")
        
        #logger.info(kafka_df)

        #Parse JSON data
        parsed_df=parse_crypto_json_data(kafka_df)

        #Start streaming qury with foreachBatch
        query=parsed_df.writeStream\
            .foreachBatch(partial(foreach_batch_function,output_path=output_path))\
            .option("checkpointLocation", args.checkpoint_path)\
            .trigger(processingTime='30 seconds')\
            .start()
        
        logger.info(f"Crypto transformation job started successfully")
        
        query.awaitTermination()

    except Exception as e:
        logger.error(f"Error in crypto transformation job: {str(e)}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
        main()


