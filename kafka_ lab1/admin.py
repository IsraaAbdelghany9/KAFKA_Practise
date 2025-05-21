from confluent_kafka.admin import AdminClient, NewTopic
from globals import topic

conf = {'bootstrap.servers': '35.226.132.78:9094,35.202.119.108:9094,34.56.208.130:9094'}
admin = AdminClient(conf)

new_topic = NewTopic(topic=topic, num_partitions=3, replication_factor=1)

# Create the topic
fs = admin.create_topics([new_topic])

# Wait for topic creation to complete
for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
