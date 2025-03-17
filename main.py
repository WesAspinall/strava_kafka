from kafka import KafkaProducer, KafkaConsumer
import time

TOPIC_NAME = "hello-world-topic"

def produce_message():
    """
    Produce a single 'Hello World' message to a Kafka topic.
    """
    # Create producer instance
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092'
    )

    # Send message to Kafka
    producer.send(TOPIC_NAME, b'Hello World from Kafka!')
    producer.flush()  # Ensure all messages are sent
    print(f"Produced message to topic '{TOPIC_NAME}'")

def consume_message():
    """
    Consume messages from the given Kafka topic and print them.
    """
    consumer = KafkaConsumer(
        TOPIC_NAME,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        group_id='hello-world-consumer-group'
    )

    print(f"Consuming messages from topic '{TOPIC_NAME}'...")
    for message in consumer:
        print(f"Received message: {message.value.decode('utf-8')}")
        break  # Exit after first message for simplicity

if __name__ == "__main__":
    # 1. Produce the message
    produce_message()

    # 2. Wait briefly for the broker to process the message
    time.sleep(2)

    # 3. Consume the message
    consume_message()
