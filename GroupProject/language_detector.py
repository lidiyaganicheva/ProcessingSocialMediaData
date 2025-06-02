import json
from kafka import KafkaConsumer, KafkaProducer
from langdetect import detect, DetectorFactory
from generator import create_topic_if_not_exists
from config import (KAFKA_LANGUAGE_TOPIC, KAFKA_GENERATOR_TOPIC,
                    KAFKA_BOOTSTRAP_SERVERS,
                    NUM_PARTITIONS, REPLICATION_FACTOR)

def main():

    # create topic for language detection
    create_topic_if_not_exists(KAFKA_LANGUAGE_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)

    # Make language detection deterministic
    DetectorFactory.seed = 0

    # Consumer (read from reddit-comments)
    consumer = KafkaConsumer(
        KAFKA_GENERATOR_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='language-detector-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    # Producer (write to reddit-comments-with-lang)
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    print(f" Language detection started. Consuming from '{KAFKA_GENERATOR_TOPIC}' → Producing to '{KAFKA_LANGUAGE_TOPIC}'.")

    try:
        for message in consumer:
            comment = message.value
            body = (
                    # since 1 is column for the text in our csv file
                    comment.get('1') or ''
            )
            # Detect language
            try:
                lang = detect(body) if body.strip() else 'unknown'
            except Exception:
                lang = 'unknown'

            # Enrich message
            enriched_comment = comment.copy()
            enriched_comment['language'] = lang

            # Produce to output topic
            producer.send(KAFKA_LANGUAGE_TOPIC, enriched_comment)
            producer.flush()

            # 2 is hardcoded column number from our input data - related comment id
            print(f" Processed comment id={comment.get('2')} → language={lang}")

    except KeyboardInterrupt:
        print("\n Stopped by user.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()