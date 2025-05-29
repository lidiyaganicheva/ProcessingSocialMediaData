import json
from kafka import KafkaConsumer, KafkaProducer
import logging
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

from GroupProject.config import NUM_PARTITIONS
from GroupProject.generator import REPLICATION_FACTOR
from generator import create_topic_if_not_exists

from config import KAFKA_LANGUAGE_TOPIC, KAFKA_SENTIMENT_TOPIC, KAFKA_BOOTSTRAP_SERVERS

logging.basicConfig(
    level=logging.INFO,
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

def main():
    consumer = KafkaConsumer(
        KAFKA_LANGUAGE_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    create_topic_if_not_exists(KAFKA_SENTIMENT_TOPIC, NUM_PARTITIONS, REPLICATION_FACTOR)

    logging.info("Sentiment microservice started...")
    for msg in consumer:
        data = msg.value

        try:
            # Get message text
            text = data.get("1", "")
            if not isinstance(text, str):
                raise ValueError("Message '1' field is not a string.")

            sentiment = analyze_sentiment(text)

            # Add sentiment to the message
            data["sentiment"] = sentiment

            producer.send(KAFKA_SENTIMENT_TOPIC, value=data)
            logging.info(f"Processed: {text[:50]}... → Sentiment: {sentiment}")

        except Exception as e:
            logging.error(f"Error processing message: {e}")
            continue

if __name__ == "__main__":
    main()