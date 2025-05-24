import os

KAFKA_TOPIC = "reddit-comments"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092', 'localhost:39092', 'localhost:49092']
DATASET_PATH = "entertainment_comicbooks.csv"
NUM_PARTITIONS = 10
NUM_PRODUCERS = 2
NUM_CONSUMERS = 10
REPLICATION_FACTOR = 2
PROCESSED_FILENAME = "processed_data.csv"