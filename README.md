# de_hw_kafka_spark
data engineering class projects for log analytics using kafka and spark

# run kafka producer in terminal 
- python producer.py
# run spark streaming script to consume the log files 
- python spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 streaming_app.py
