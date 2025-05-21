from confluent_kafka import Consumer,KafkaException,KafkaError
import sys
from globals import client_id, topic
from globals import group_id_2 as group_id

conf = {'bootstrap.servers':'35.226.132.78:9094,35.202.119.108:9094,34.56.208.130:9094','group.id':group_id,'auto.offset.reset':'smallest'}

consumer = Consumer(conf)

def msg_process(msg):
    print(msg.value())
    

running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        consumer.close()

def shutdown():
    running = False
    
basic_consume_loop(consumer,topics=[topic])