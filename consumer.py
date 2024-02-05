from confluent_kafka import Consumer, KafkaError

def create_kafka_consumer():
    # Define Kafka consumer configuration
    consumer_config = {
        'bootstrap.servers': 'localhost:9092',  # Kafka bootstrap servers
        'group.id': 'your_consumer_group',  # Consumer group ID
        'auto.offset.reset': 'earliest',  # Configuration determines what to do when there is no initial
    }

    # Create a Kafka consumer instance
    consumer = Consumer(consumer_config)

    return consumer

def subscribe_to_topic(consumer, topic):
    # Subscribe to the Kafka topic
    consumer.subscribe([topic])

def consume_messages(consumer):
    # Poll for messages
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # End of partition event
                continue
            else:
                print(msg.error())
                break

        # Print the received message value
        print('Received message: {}'.format(msg.value().decode('utf-8')))

def main():
    kafka_topic = 'bigdata'  # Kafka topic name
    consumer = create_kafka_consumer()
    subscribe_to_topic(consumer, kafka_topic)
    consume_messages(consumer)

if __name__ == "__main__":
    main()