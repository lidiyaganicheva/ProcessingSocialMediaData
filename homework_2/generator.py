import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time
import logging
import sys
import config
import random

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)

KAFKA_TOPIC = config.KAFKA_TOPIC
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
DATASET_PATH = config.DATASET_PATH
NUM_PARTITIONS = config.NUM_PARTITIONS
REPLICATION_FACTOR = config.REPLICATION_FACTOR

def create_topic_if_not_exists(topic_name, partitions, replication_factor):
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logging.info(f"Created topic '{topic_name}' with {partitions} partitions")
    else:
        print(f"Topic '{topic_name}' already exists")

def load_data(file_path):
    """Loads the dataset and yields one comment at a time."""
    chunksize = 1000
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        for _, row in chunk.iterrows():
            yield row.to_dict()

def send_comments(producer, topic, comment_generator):
    partition_counter = 0
    partition_counts = {i: 0 for i in range(NUM_PARTITIONS)}

    for comment in comment_generator:
        try:
            message = json.dumps(comment)
            comment_hash = hash(message)
            partition = abs(comment_hash) % NUM_PARTITIONS
            key = str(partition)
            partition_counter += 1
            partition_counts[partition] += 1

            future = producer.send(topic, key=key, value=message)
            future.get(timeout=10)

            if partition_counter % 1000 == 0:
                print(f"\nMessages sent so far: {partition_counter}")
                print("Messages per partition:")
                for p, count in partition_counts.items():
                    print(f"  Partition {p}: {count}")

            time.sleep(0.01)
        except Exception as e:
            print(f"Error sending message: {str(e)}")
            continue

    print("\nFinal message distribution:")
    for p, count in partition_counts.items():
        print(f"  Partition {p}: {count} messages")

if __name__ == "__main__":
    create_topic_if_not_exists(KAFKA_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x.encode('utf-8') if isinstance(x, str) else json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8') if x else None
    )
    comment_gen = load_data(DATASET_PATH)
    send_comments(producer, KAFKA_TOPIC, comment_gen)
    producer.flush()

