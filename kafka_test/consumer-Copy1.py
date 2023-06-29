from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import *
from datetime import datetime, timedelta
from hdfs import InsecureClient
# other needed packages
from spark_tranformation import *


# Kafka consumer settings
# the following are just examples 
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
    "startingOffsets": "latest",
}

# Read the Kafka stream using spark.readStream
df = spark.readStream.format("kafka").options(**kafka_options).load()

# Convert the binary message value to string
df = df.withColumn("value", col("value").cast("string"))

# Extract the desired fields from the log line
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

# Handling timestamp using udf
udf_parse_time = udf(parse_clf_time)

parsed_df = df.select('*', udf_parse_time(df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')

# Process the Kafka messages and write to console and text file
query = parsed_df.writeStream.outputMode("append").format("console").start()

# Set the current time as the start time
start_time = datetime.now()

while query.isActive:
    # Check if no messages have been received for 10 seconds
    elapsed_time = datetime.now() - start_time
    if elapsed_time > timedelta(seconds=10):
        query.stop()

        
# Write the DataFrame to Parquet files
# hdfs_path = "hdfs://localhost:9870/temp"
# 
# parquet_query = parsed_df.writeStream \
#     .format("parquet") \
#     .outputMode("append") \
#     .option("checkpointLocation", "/tmp/output/ch") \
#     .option("path", hdfs_path) \
#     .start()
# 
# parquet_query.awaitTermination()

local_path = "./test/"

csv_query = parsed_df.writeStream \
    .format("csv") \
    .option("header", "true") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/output/ch") \
    .option("path", local_path) \
    .start()

csv_query.awaitTermination()