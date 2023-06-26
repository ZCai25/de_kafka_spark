from kafka import KafkaProducer

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers='localhost:9092')

# Read the log file and send each line as a Kafka message
with open('42MBSmallServerLog.log', 'r') as file:
    for line in file:
        producer.send('log_topic', line.encode('utf-8'))

# Close the Kafka producer
producer.close()
