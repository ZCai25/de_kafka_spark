from confluent_kafka import Producer
# other needed packages

# Specify the Kafka broker and topic to produce to 
# the following are examples
brokers = 'localhost:9092'
topic = 'log_lines_topic'

dataset = '42MBSmallServerLog.log'


# Set up the Kafka producer
producer = Producer({
    'bootstrap.servers': brokers,  # Kafka broker address
    'api.version.request': True
})

def callback(err, event):
    if err:
        print(f'Produce to topic {event.topic()} failed for event: {event.key()}')
    else:
        val = event.value().decode('utf8')
        print(f'{val} sent to partition {event.partition()}.')

if __name__ == '__main__':
    batch_size = 1000  # Adjust this value based on requirements

    # Open the log file
    with open(dataset, 'r') as file:
        batch = []
        line_number = 1 # start index with line number 1
        for line in file:
            # Add each line to the current batch
            batch.append(line)
            line_number +=1

            # Check if the batch size is reached
            if len(batch) >= batch_size:
                # Produce the batch to Kafka
                producer.produce(topic, key=str(line_number),value=''.join(batch).encode('utf-8'))
                producer.flush()

                # Clear the batch
                batch = []
         
        
            # Check if there are any remaining lines in the last batch
            if batch:
                # Produce the remaining lines to Kafka
                producer.poll(1)
                producer.produce(topic, key= str(line_number),value=''.join(batch).encode('utf-8'),callback=callback)
                producer.flush()

    # Flush any remaining messages in the producer queue
    producer.flush()
 
    producer.produce(topic, key= "Stop",value="Bye....",callback=callback)

print('job completed')