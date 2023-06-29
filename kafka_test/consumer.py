import re
import pandas as pd
from datetime import datetime, timedelta
from hdfs import InsecureClient
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import SparkSession
#from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import sum as spark_sum
from pyspark.sql.functions import col, date_format, count, desc, max, regexp_extract
from pyspark.sql.types import *

# Kafka consumer settings
kafka_bootstrap_servers = 'localhost:9092'
kafka_topic = 'log_lines_topic'
kafka_group_id = 'spark-streaming-consumer-group'

# Create a SparkSession
spark = SparkSession.builder.appName("KafkaConsumerSparkStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Define the Kafka source options
kafka_options = {
    "kafka.bootstrap.servers": kafka_bootstrap_servers,
    "subscribe": kafka_topic,
    "startingOffsets": "latest"
}

# Read the Kafka stream using spark.readStream
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Convert the binary message value to string
df = df.withColumn("value", col("value").cast("string"))

host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
ts_pattern = r"\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\]"
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
status_pattern = r'\s(\d{3})\s'
content_size_pattern = r'\s(\d+)$'

df = df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))

# Handling nulls in HTTP status
df = df[df['status'].isNotNull()] 

# Handling nulls in HTTP content size
df = df.na.fill({'content_size': 0})

# Process the Kafka messages and write to console and text file
query = df.writeStream.outputMode("append").format("console").start()

# Set the current time as the start time
start_time = datetime.now()

while query.isActive:
    # Check if no messages have been received for 10 seconds
    elapsed_time = datetime.now() - start_time
    if elapsed_time > timedelta(seconds=10):
        query.stop()

save_hdfs = False
if save_hdfs:
    hdfs_path = "http://localhost:9870/temp"
    #hdfs_path = "./test2/"
    parquet_query = df.writeStream.format("parquet").outputMode("append").option("checkpointLocation", '/tmp/output/ch').option("path", hdfs_path).start()
    parquet_query.awaitTermination()

save_local = True
if save_local:
    local_path = "./test/"
    csv_query = repartitioned_df.writeStream \
        .format("csv") \
        .option("header", "true") \
        .outputMode("append") \
        .option("checkpointLocation", "./tmp") \
        .option("path", local_path) \
        .start()
    csv_query.awaitTermination()