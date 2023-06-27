from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_extract
from pyspark.sql.types import *
from datetime import datetime, timedelta
from hdfs import InsecureClient
# other needed packages


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
df = df.withColumn(
    "ip_address",
    regexp_extract(col("value"), r'^([\d.]+)', 1)
).withColumn(
    "timestamp",
    regexp_extract(col("value"), r'\[([^:]+)', 1)
).withColumn(
    "year",
    regexp_extract(col("timestamp"), r'(\d{4})', 1)
…
…
….

# Select the desired columns
parsed_df = df.select("ip_address", "year", "month", "day", "hour", "minute", "method", "endpoint", "http_version", "response_code", "bytes")

# Process the Kafka messages and write to console and text file
query = parsed_df.writeStream.outputMode("append").format("console").start()

# Set the current time as the start time
start_time = datetime.now()

while query.isActive:
    # Check if no messages have been received for 10 seconds
    elapsed_time = datetime.now() - start_time
    if elapsed_time > timedelta(seconds=10):
        query.stop()

local_path = '/tmp/output/logs/' # an example

w_query = parsed_df.writeStream.format("parquet").outputMode("append").option("checkpointLocation", '/tmp/output/ch').option("path", local_path).start()

query.awaitTermination()