import os

KAFKA_TOPIC = "reddit-comments"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092', 'localhost:39092', 'localhost:49092']
DATASET_PATH = "entertainment_comicbooks.csv"
NUM_PARTITIONS = 3
NUM_REPLICAS = 1
PROCESSED_FILENAME = "processed_data.csv"