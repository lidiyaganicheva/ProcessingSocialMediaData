from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import config
import time

def create_topic_if_not_exists(topic_name, partitions, replication_factor):
    admin_client = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        print(f"Created topic '{topic_name}' with {partitions} partitions")
    else:
        print(f"Topic '{topic_name}' already exists")

if __name__ == "__main__":
    # Create main topic
    create_topic_if_not_exists(config.KAFKA_GENERATOR_TOPIC, config.NUM_PARTITIONS, config.REPLICATION_FACTOR)

    # Create keywords topic
    create_topic_if_not_exists('reddit-keywords', config.NUM_PARTITIONS, config.REPLICATION_FACTOR)

    time.sleep(5) 