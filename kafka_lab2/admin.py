from confluent_kafka.admin import AdminClient, NewTopic
from globals import topic, bootstrap_servers

conf = {'bootstrap.servers': bootstrap_servers}
admin = AdminClient(conf)

new_topic = NewTopic(topic=topic, num_partitions=3, replication_factor=1)

fs = admin.create_topics([new_topic])
for topic, f in fs.items():
    try:
        f.result()
        print(f"Topic '{topic}' created")
    except Exception as e:
        print(f"Failed to create topic {topic}: {e}")
