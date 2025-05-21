import sys
from confluent_kafka import Producer
from globals import client_id, topic, bootstrap_servers

conf = {'bootstrap.servers': bootstrap_servers, 'client.id': client_id}
producer = Producer(conf)

def produce_to_kafka(image_id):
    try:
        producer.produce(topic, key=image_id, value=image_id)
        producer.flush()
        print(f"Produced image ID: {image_id}")
    except Exception as e:
        print("Kafka produce error:", e)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 producer.py <image_id>")
        sys.exit(1)
    produce_to_kafka(sys.argv[1])

