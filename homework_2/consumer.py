import time
import csv
import os
from kafka import KafkaConsumer
import json
from datetime import datetime
from homework_2 import config

TOPIC_NAME = config.KAFKA_TOPIC
CSV_FILE = config.PROCESSED_FILENAME
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS

# Initialize Kafka consumer
consumer = KafkaConsumer(
    TOPIC_NAME,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    group_id='reddit_processor_group',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

if not os.path.exists(CSV_FILE):
    with open(CSV_FILE, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['timestamp', 'iso_datetime'])

print("Kafka consumer started. Waiting for messages...")

try:
    for message in consumer:
        comment = message.value

        # Imitate processing by sleeping for 1 sec
        time.sleep(1)

        # Get the timestamp - hardcoded as 5 to match current datasets, where column '5' represents timestamp of comment
        created_utc = comment.get('5')

        if created_utc:
            iso_time = datetime.fromtimestamp(created_utc).isoformat()
            with open(CSV_FILE, mode='a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([created_utc, iso_time])
            print(f"Processed comment for {iso_time} timestamp")
        else:
            print("Comment is missing '5' field")

except KeyboardInterrupt:
    print("Consumer stopped.")
