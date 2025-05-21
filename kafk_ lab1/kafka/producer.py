from confluent_kafka import Producer
from globals import client_id, topic

conf = {'bootstrap.servers':'35.226.132.78:9094,35.202.119.108:9094,34.56.208.130:9094','client.id':client_id}

producer = Producer(conf)

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))


for i in range(10):
    producer.produce(topic,key="key"+str(i),value="hello world " + str(i),callback=acked)

producer.flush()

