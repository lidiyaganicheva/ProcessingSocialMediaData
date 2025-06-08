import json
from kafka import KafkaConsumer, KafkaProducer
import logging
import threading
import nltk
import config
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from generator import create_topic_if_not_exists
from config import (KAFKA_LANGUAGE_TOPIC, KAFKA_SENTIMENT_TOPIC,
                    KAFKA_BOOTSTRAP_SERVERS, NUM_CONSUMERS,
                    NUM_PARTITIONS, REPLICATION_FACTOR)

logging.basicConfig(
    level=config.LOGGING_LEVEL,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

try:
    nltk.data.find('sentiment/vader_lexicon.zip')
except LookupError:
    nltk.download('vader_lexicon')

analyzer = SentimentIntensityAnalyzer()


def analyze_sentiment(text):
    score = analyzer.polarity_scores(text)['compound']
    return "Positive" if score >= 0 else "Negative"


def consumer_worker(consumer_id):
    logging.info(f"Consumer-{consumer_id} started.")

    consumer = KafkaConsumer(
        KAFKA_LANGUAGE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='sentiment-service-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    for msg in consumer:
        data = msg.value
        try:
            text = data.get("1", "")
            if not isinstance(text, str):
                raise ValueError("Field '1' is not a string")

            sentiment = analyze_sentiment(text)
            data["sentiment"] = sentiment

            producer.send(KAFKA_SENTIMENT_TOPIC, value=data)
            logging.info(f"Consumer-{consumer_id}: Sentiment = {sentiment}")

        except Exception as e:
            logging.error(f"Consumer-{consumer_id}: Error processing message: {e}")


def main():
    threads = []
    for i in range(NUM_CONSUMERS):
        t = threading.Thread(target=consumer_worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    create_topic_if_not_exists(KAFKA_SENTIMENT_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)
    main()
