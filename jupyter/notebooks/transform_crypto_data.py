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
import os
from transformations_ohlcv import transformation_ohlcv

# %%
#Configure Logging
logging.basicConfig(level=logging.INFO)
logger=logging.getLogger(__name__)

# %%
def create_spark_session(spark_master):
    spark=SparkSession.builder\
            .appName("CryptoDataTransformation")\
            .master(spark_master)\
            .config("spark.ui.port", "4040")\
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.1") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

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
    .option("startingOffsets", "earliest")\
    .option("failOnDataLoss", "false")\
    .load()

# %%
def parse_crypto_json_data(df):
    """Parse JSON crypto data from Kafka"""
    crypto_schema=define_crypto_schema()

    json_df=df.selectExpr("CAST(value AS STRING) as json_str")

    parsed_df=json_df.select(from_json(col("json_str"), crypto_schema).alias("data")).select("data.*")

    return parsed_df

# %%
def apply_crypto_transformations(df):
    """Apply business logic transformations to crypto data - FIXED VERSION"""
    logger.info("Applying transformations to crypto data")
    

    df= transformation_ohlcv(df)

    return df\
        .withColumn("processed_timestamp",current_timestamp())\
        .withColumn("processing_date",current_date())\
        .withColumn("price_range", col("high") - col("low"))\
        .withColumn("is_volume_high", when(col("volume") > 1000000, True).otherwise(False))
    
# %%
def apply_crypto_quality_checks(df):
    """Apply data quality checks to crypto data"""
    logger.info("Applying data quality checks to crypto data")
    return df\
        .withColumn("data_quality_flag",
            when(col("symbol").isNull(), "missing_symbol")
            .when(col("open").isNull() | col("high").isNull() | col("low").isNull() | col("close").isNull(), "missing_price_data")
            .when(col("volume").isNull(), "missing_volume")
            .when(col("high") < col("low"), "invalid_high_low")
            .otherwise(None)
        )\
        .withColumn("data_quality_status",
            when(col("data_quality_flag").isNull(), "Passed").otherwise("Failed")
        )

# %%
def setup_delta_write_path(output_path):
    try:
        os.makedirs(output_path, exist_ok=True)
        os.chmod(output_path, 0o777)  # Set permissions to allow writing

        logger.info(f"Delta write path {output_path} is set up successfully")
        return True
    except PermissionError:
        logger.error(f"Permission denied for Delta write path {output_path}. Please check permissions.")
        return False
    except Exception as e:
        logger.error(f"Error setting up Delta write path {output_path}: {str(e)}")
        return False

def write_to_delta_with_partitions(df, output_path, partition_cols=None):
    """Write DataFrame to Delta lake with Partitioning - IMPROVED VERSION"""
    if not setup_delta_write_path(output_path):
        logger.error(f"Failed to set up Delta write path {output_path}. Exiting write operation.")
        return
    
    logger.info(f"Writing data to Delta lake at {output_path} with partitions {partition_cols}")
    
    try:
        # Try Delta write first
        write = df.write\
            .format("delta")\
            .mode("append")\
            .option("overwriteSchema", "true")\
            .option("mergeSchema", "true")
        
        if partition_cols:
            write = write.partitionBy(*partition_cols)
        
        write.save(output_path)
        logger.info(f"Successfully wrote to Delta Lake: {output_path}")
        
    except Exception as delta_error:
        logger.error(f"Delta write failed: {str(delta_error)}")
        
        # Fallback to Parquet
        try:
            logger.info("Falling back to Parquet format...")
            parquet_path = output_path + "_parquet_fallback"
            setup_delta_write_path(parquet_path)
            
            write = df.write\
                .format("parquet")\
                .mode("append")
            
            if partition_cols:
                write = write.partitionBy(*partition_cols)
            
            write.save(parquet_path)
            logger.info(f"Successfully wrote to Parquet: {parquet_path}")
            
        except Exception as parquet_error:
            logger.error(f"Both Delta and Parquet writes failed: {str(parquet_error)}")
            raise

# %%
def process_crypto_batch(df, epoch_id, output_path):
    """Process each batch of crypto data - IMPROVED VERSION"""
    try:
        logger.info(f"Processing crypto batch {epoch_id}")

        if df.count() == 0:
            logger.info(f"No data in batch {epoch_id}, skipping")
            return
        
        # Apply transformations and quality checks
        transformed_df = apply_crypto_transformations(df)
        quality_checked_df = apply_crypto_quality_checks(transformed_df)

        # Separate data by quality
        clean_data = quality_checked_df.filter(col("data_quality_status") == "Passed")
        failed_data = quality_checked_df.filter(col("data_quality_status") == "Failed")

        # Write clean data 
        if clean_data.count() > 0:
            logger.info(f"Writing {clean_data.count()} clean records")
            write_to_delta_with_partitions(clean_data, output_path["clean_data"], ["symbol"])
        
        if failed_data.count() > 0:
            logger.info(f"Writing {failed_data.count()} failed records")
            write_to_delta_with_partitions(failed_data, output_path["failed_data"], ["symbol"])

        logger.info(f"Batch {epoch_id} processed: {clean_data.count()} clean, {failed_data.count()} failed")    
        
    except Exception as e:
        logger.error(f"Error processing crypto batch {epoch_id}: {str(e)}")
        import traceback
        traceback.print_exc()

def foreach_batch_function(df, epoch_id, output_path):
    """Function to be used with foreachBatch"""
    logger.info(f'Count of records received in df: {df.count()}')
    if df.count() > 0:
        process_crypto_batch(df, epoch_id, output_path)
        logger.info(f"Batch {epoch_id} processed successfully")
    else:
        logger.info(f"No data in batch {epoch_id}, skipping processing")

# %%
def main():
    parser = argparse.ArgumentParser(description='Transform crypto_data from kafka')
    parser.add_argument('--kafka_servers', required=True, help='kafka bootstrap servers')
    parser.add_argument('--kafka_topic', required=True, help='kafka topic name')
    parser.add_argument('--output_path', required=True, help='output path for processed data')
    parser.add_argument('--checkpoint_path', required=True, help='checkpoint path for streaming')
    parser.add_argument('--execution_date', required=True, help='Airflow execution date')
    parser.add_argument('--spark_master', default='', help='Spark master URL')
    parser.add_argument('--timeout_minutes', type=int, default=10, help='Timeout in minutes')

    args = parser.parse_args()

    # Create Spark Session
    spark = create_spark_session(args.spark_master)
    spark.sparkContext.setLogLevel("WARN")

    # Use args.output_path instead of hardcoded path
    output_path = args.output_path
    checkpoint_path = args.checkpoint_path
    
    logger.info(f"Using output_path: {output_path}")
    logger.info(f"Using checkpoint_path: {checkpoint_path}")
    logger.info(f"Timeout set to: {args.timeout_minutes} minutes")

    # Define output paths
    output_path = {
        "clean_data": f"{args.output_path}/clean_data",
        "failed_data": f"{args.output_path}/failed_data"
    }

    query = None
    try:
        # Read kafka data stream
        kafka_df = read_kafka_crypto_stream(spark, args.kafka_servers, args.kafka_topic)

        logger.info(f"Kafka stream started from topic {args.kafka_topic} on servers {args.kafka_servers}")
        
        # Parse JSON data
        parsed_df = parse_crypto_json_data(kafka_df)

        # Start streaming query with foreachBatch
        query = parsed_df.writeStream\
            .foreachBatch(partial(foreach_batch_function, output_path=output_path))\
            .option("checkpointLocation", args.checkpoint_path)\
            .trigger(processingTime='30 seconds')\
            .start()
        
        logger.info(f"Crypto transformation job started successfully")
        
        #query.awaitTermination()
        # ADD TIMEOUT LOGIC HERE
        import time
        timeout_seconds = args.timeout_minutes * 60
        start_time = time.time()
        
        # Wait with timeout
        while query.isActive and (time.time() - start_time) < timeout_seconds:
            time.sleep(10)  # Check every 10 seconds
            elapsed = time.time() - start_time
            logger.info(f"Streaming running... Elapsed: {elapsed:.0f}s / {timeout_seconds}s")
        
        if query.isActive:
            logger.info(f"Timeout reached ({args.timeout_minutes} minutes). Stopping streaming query gracefully...")
            query.stop()
            logger.info("Streaming query stopped successfully")
        else:
            logger.info("Streaming query completed naturally")
            
        # Wait a bit more for graceful shutdown
        time.sleep(5)

    except Exception as e:
        logger.error(f"Error in crypto transformation job: {str(e)}")
        if query and query.isActive:
            logger.info("Stopping query due to error...")
            query.stop()
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()