import pandas as pd
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import json
import time
import logging
import sys
import config
from multiprocessing import Process

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(stream=sys.stdout)]
)

KAFKA_TOPIC = config.KAFKA_TOPIC
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
DATASET_PATH = config.DATASET_PATH
NUM_PARTITIONS = config.NUM_PARTITIONS
NUM_PRODUCERS = config.NUM_PRODUCERS
REPLICATION_FACTOR = config.REPLICATION_FACTOR

def create_topic_if_not_exists(topic_name, partitions, replication_factor):
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    existing_topics = admin_client.list_topics()

    if topic_name not in existing_topics:
        topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logging.info(f"Created topic '{topic_name}' with {partitions} partitions")
    else:
        logging.info(f"Topic '{topic_name}' already exists")

def load_data(file_path, producer_id, num_producers):
    """Load only the portion of data assigned to this producer."""
    chunksize = 1000
    for chunk in pd.read_csv(file_path, chunksize=chunksize, skiprows=lambda i: i > 0 and (i - 1) % num_producers != producer_id):
        for _, row in chunk.iterrows():
            yield row.to_dict()

def send_comments(producer_id):
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: x.encode('utf-8') if isinstance(x, str) else json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8') if x else None
    )

    partition_counter = 0
    partition_counts = {i: 0 for i in range(NUM_PARTITIONS)}
    comment_gen = load_data(DATASET_PATH, producer_id, NUM_PRODUCERS)

    for comment in comment_gen:
        try:
            message = json.dumps(comment)
            # Round-robin partitioning
            partition = partition_counter % NUM_PARTITIONS
            key = hash(message)
            partition_counter += 1
            partition_counts[partition] += 1

            producer.send(KAFKA_TOPIC, key=key, value=message)

            if partition_counter % 1000 == 0:
                logging.info(f"Producer {producer_id} - Sent {partition_counter} messages")
                for p, count in partition_counts.items():
                    logging.info(f"  Partition {p}: {count} messages")

            time.sleep(0.01)
        except Exception as e:
            logging.error(f"Producer {producer_id} error: {e}")
            continue

    producer.flush()
    logging.info(f"Producer {producer_id} - Final distribution:")
    for p, count in partition_counts.items():
        logging.info(f"  Partition {p}: {count} messages")

if __name__ == "__main__":
    create_topic_if_not_exists(KAFKA_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)

    processes = []
    for i in range(NUM_PRODUCERS):
        p = Process(target=send_comments, args=(i,))
        p.start()
        processes.append(p)

    for p in processes:
        p.join()
