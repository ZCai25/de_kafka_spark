from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract
from pyspark.sql.functions import udf
from spark_tranformation import * # function and regex patterns 

# Create a SparkSession
spark = SparkSession.builder.appName('LogAnalytics').getOrCreate()

# Configure the Kafka source
kafka_params = {
    'kafka.bootstrap.servers': 'localhost:9092',
    'subscribe': 'log_topic',
    'startingOffsets': 'earliest'
}

# Read from the Kafka topic and convert to RDD
logs_df = spark.read.format('kafka').options(**kafka_params).load()
log_rdd = logs_df.selectExpr('CAST(value AS STRING)').rdd

# Perform your log analytics on the RDD
# Example: Display the first 10 rows of the RDD
# for row in log_rdd.take(10):
#     print(row)

# Perform transformations of the logs_df
logs_df = logs_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))



# Handling nulls in HTTP status
logs_df = logs_df[logs_df['status'].isNotNull()] 

# Handling nulls in HTTP content size
logs_df = logs_df.na.fill({'content_size': 0})

# Handling Temporal Fields (Timestamp)
# def parse_clf_time(text):
#     """ Convert Common Log time format into a Python datetime object
#     Args:
#         text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
#     Returns:
#         a string suitable for passing to CAST('timestamp')
#     """
#     # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
#     return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
#       int(text[7:11]),
#       month_map[text[3:6]],
#       int(text[0:2]),
#       int(text[12:14]),
#       int(text[15:17]),
#       int(text[18:20])
#     )
udf_parse_time = udf(parse_clf_time)

logs_df = logs_df.select('*', udf_parse_time(logs_df['timestamp']).cast('timestamp').alias('time')).drop('timestamp')


# Save the logs_df_format DataFrame as a CSV file
logs_df.write.csv('logs_df_format.csv', header=True)
logs_df.write.parquet('logs_df.parquet')

# Stop the SparkSession

spark.stop()
