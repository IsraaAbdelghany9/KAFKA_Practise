from confluent_kafka import Consumer, KafkaError, KafkaException
import requests, random, sys, cv2, numpy as np
from globals import client_id, topic, group_id_1, bootstrap_servers
import os

conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id_1,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)

def detect_object():
    return random.choice(['car', 'house', 'person'])

def process_message(msg):
    image_id = msg.value().decode()

    try:
        # Get image from server
        res = requests.get(f"http://127.0.0.1:5000/images/{image_id}.jpg")
        if res.status_code != 200:
            print(f"Image {image_id} not found")
            return

        # Convert bytes to image
        image_array = np.frombuffer(res.content, np.uint8)
        img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)

        # Convert to grayscale
        gray_img = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Overwrite the original image
        img_path = f"images/{image_id}.jpg"
        cv2.imwrite(img_path, gray_img)

        # Send label
        label = detect_object()
        response = requests.put(f"http://127.0.0.1:5000/object/{image_id}", json={"object": label, "filename": f"{image_id}.jpg"})
        print(f"Processed {image_id} as {label}, status: {response.status_code}")

    except Exception as e:
        print("Error:", e)

def consume_loop():
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(msg.error())
            else:
                process_message(msg)
    finally:
        consumer.close()

consume_loop()
