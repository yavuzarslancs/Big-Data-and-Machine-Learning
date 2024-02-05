import pandas as pd
from confluent_kafka import Producer
import time

def read_csv_into_dataframe(file_path):
    # Read the CSV file into a Pandas DataFrame
    df = pd.read_csv(file_path)
    return df

def create_kafka_producer(bootstrap_servers):
    # Kafka producer configuration
    producer_config = {
        'bootstrap.servers': bootstrap_servers,
    }

    # Create a Kafka producer instance
    producer = Producer(producer_config)
    return producer

def produce_messages_to_kafka(producer, kafka_topic, dataframe, max_duration_seconds=10, batch_sleep_seconds=1):
    # Record the start time
    start_time = time.time()

    # Run the producer for a maximum duration
    while time.time() - start_time < max_duration_seconds:
        # Convert each row to JSON and push to Kafka
        for _, row in dataframe.iterrows():
            message_value = row.to_json()
            producer.produce(kafka_topic, value=message_value)

        # Wait for any outstanding messages to be delivered and delivery reports to be received
        producer.flush()

        # Print a message indicating that messages have been sent
        print(f"Data pushed to Kafka topic '{kafka_topic}' successfully.")

        # Sleep for the specified seconds before sending the next batch of messages
        time.sleep(batch_sleep_seconds)

if __name__ == "__main__":
    # Rest of your code here...

    # Set your Kafka configurations
    bootstrap_servers = 'localhost:9092'
    kafka_topic_name = 'bigdata'
    max_duration_seconds = 1000

    # Read CSV into DataFrame
    df_password = read_csv_into_dataframe('Advertising.csv')

    # Create Kafka producer
    kafka_producer = create_kafka_producer(bootstrap_servers)

    # Produce messages to Kafka
    produce_messages_to_kafka(kafka_producer, kafka_topic_name, df_password, max_duration_seconds=max_duration_seconds, batch_sleep_seconds=1)