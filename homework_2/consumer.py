import time
import csv
import os
import argparse
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.structs import TopicPartition
import json
from datetime import datetime
import config
from concurrent.futures import ThreadPoolExecutor

TOPIC_NAME = config.KAFKA_TOPIC
CSV_FILE = config.PROCESSED_FILENAME
KAFKA_BOOTSTRAP_SERVERS = config.KAFKA_BOOTSTRAP_SERVERS
NUM_PARTITIONS = config.NUM_PARTITIONS
REPLICATION_FACTOR = config.REPLICATION_FACTOR
NUM_CONSUMERS = config.NUM_CONSUMERS

def verify_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    existing_topics = admin_client.list_topics()
    if TOPIC_NAME not in existing_topics:
        print(f"Topic {TOPIC_NAME} does not exist. Creating it...")
        topic = NewTopic(
            name=TOPIC_NAME,
            num_partitions=NUM_PARTITIONS,
            replication_factor=REPLICATION_FACTOR
        )
        admin_client.create_topics([topic])
        print(f"Created topic {TOPIC_NAME} with {NUM_PARTITIONS} partitions")
    else:
        print(f"Topic {TOPIC_NAME} exists")
        configs = admin_client.describe_topics([TOPIC_NAME])
        for cfg in configs:
            print(f"Topic {cfg['topic']} has {len(cfg['partitions'])} partitions")
            for p in cfg['partitions']:
                print(f"  Partition {p['partition']} leader: {p['leader']}")

def process_messages(consumer_id):
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    topic_config = admin_client.describe_topics([TOPIC_NAME])[0]
    total_partitions = len(topic_config['partitions'])
    print(f"Topic {TOPIC_NAME} has {total_partitions} partitions")

    partitions_per_consumer = total_partitions // NUM_CONSUMERS
    start_partition = consumer_id * partitions_per_consumer
    end_partition = start_partition + partitions_per_consumer
    assigned_partitions = list(range(start_partition, end_partition))
    if not assigned_partitions:
        assigned_partitions = [consumer_id % total_partitions]
    print(f"Consumer {consumer_id} will handle partitions: {assigned_partitions}")
    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='reddit_processor_group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else None,
        key_deserializer=lambda m: m.decode('utf-8') if m else None,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000
    )
    if assigned_partitions:
        consumer.assign([TopicPartition(TOPIC_NAME, p) for p in assigned_partitions])
        print(f"Consumer {consumer_id} assigned to partitions: {assigned_partitions}")
    else:
        print(f"Consumer {consumer_id} has no partitions to process, exiting...")
        return
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        key_serializer=lambda x: str(x).encode('utf-8') if x else None
    )
    print(f"Consumer {consumer_id} started. Waiting for messages...")
    try:
        print(f"Consumer {consumer_id} group: reddit_processor_group")
        print(f"Consumer {consumer_id} bootstrap servers:", KAFKA_BOOTSTRAP_SERVERS)
        print(f"Consumer {consumer_id} subscribing to topic:", TOPIC_NAME)
        print(f"Consumer {consumer_id} topics:", consumer.topics())
        print(f"Consumer {consumer_id} subscription:", consumer.subscription())
        print(f"Consumer {consumer_id} partitions for topic:", consumer.partitions_for_topic(TOPIC_NAME))
        time.sleep(5)
        print(f"Consumer {consumer_id} assigned to partitions: {consumer.assignment()}")
        print(f"Consumer {consumer_id} assignment after sleep:", consumer.assignment())
        for partition in assigned_partitions:
            tp = TopicPartition(TOPIC_NAME, partition)
            end_offset = consumer.end_offsets([tp])[tp]
            print(f"Consumer {consumer_id} partition {partition} end offset: {end_offset}")
        for message in consumer:
            try:
                if not message or not message.value:
                    print(f"Consumer {consumer_id}: Received empty message")
                    continue
                comment = message.value
                if isinstance(comment, str):
                    comment = json.loads(comment)
                time.sleep(1)
                created_utc = comment.get('5')
                if created_utc:
                    iso_time = datetime.fromtimestamp(created_utc).isoformat()
                    with open(CSV_FILE, mode='a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([created_utc, iso_time])
                    processed_message = {
                        'id': comment.get('id', 'unknown'),
                        'timestamp': created_utc,
                        'processing_time': time.time(),
                        'processed_by': f'consumer_{consumer_id}'
                    }
                    producer.send('processed-messages', value=processed_message)
                    producer.flush()
                    print(f"Consumer {consumer_id} processed comment for {iso_time} timestamp from partition {message.partition}")
                else:
                    print(f"Consumer {consumer_id}: Comment is missing '5' field")
            except Exception as e:
                print(f"Error processing message in consumer {consumer_id}: {str(e)}")
                continue
    except KeyboardInterrupt:
        print(f"Consumer {consumer_id} stopped.")
    except Exception as e:
        print(f"Unexpected error in consumer {consumer_id}: {str(e)}")
    finally:
        try:
            consumer.close()
            producer.close()
        except Exception as e:
            print(f"Error closing consumer {consumer_id}: {str(e)}")

def main():
    if not os.path.exists(CSV_FILE):
        with open(CSV_FILE, mode='w', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(['timestamp', 'iso_datetime'])
    parser = argparse.ArgumentParser(description='Run Kafka consumers')
    parser.add_argument('--num-consumers', type=int, default=1,
                        help='Number of consumers to run (default: 1)')
    args = parser.parse_args()
    print(f"Starting {NUM_CONSUMERS} consumers...")
    verify_topic()
    with ThreadPoolExecutor(max_workers=NUM_CONSUMERS) as executor:
        futures = [executor.submit(process_messages, i) for i in range(NUM_CONSUMERS)]
        try:
            for future in futures:
                future.result()
        except KeyboardInterrupt:
            print("\nShutting down all consumers...")
            executor.shutdown(wait=False)
        except Exception as e:
            print(f"Error in main thread: {str(e)}")
            executor.shutdown(wait=False)

if __name__ == "__main__":
    main()
