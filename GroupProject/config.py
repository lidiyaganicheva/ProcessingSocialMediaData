import os

KAFKA_GENERATOR_TOPIC = "reddit-comments"
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092', 'localhost:39092', 'localhost:49092']
DATASET_PATH = "entertainment_comicbooks.csv"
NUM_PARTITIONS = 5
NUM_PRODUCERS = 1
NUM_CONSUMERS = 5

REPLICATION_FACTOR = 2

KAFKA_LANGUAGE_TOPIC = "reddit-comments"
KAFKA_SENTIMENT_TOPIC = "sentiment-detected"
