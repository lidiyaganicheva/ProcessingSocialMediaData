
import json
from kafka import KafkaConsumer, KafkaProducer
import logging
import threading
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from datetime import datetime

from generator import create_topic_if_not_exists
from config import (KAFKA_KEYWORDS_TOPIC,
                    KAFKA_BOOTSTRAP_SERVERS, NUM_CONSUMERS,
                    NUM_PARTITIONS, REPLICATION_FACTOR)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def main():
    consumer = KafkaConsumer(
        KAFKA_KEYWORDS_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id='report-service-group',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    for message in consumer:
        print("processing message simpos")
        data = message.value
        try:
            with open("report.json", 'r') as f:
                report = json.load(f)
        except Exception as e:
            print(e)
            report = {
                "languages": {},
                "sentiments": {},
                "keywords": {},
                "top_keywords": {},
                "timestamp": datetime.now().isoformat()
            }
        print(report)
        report["languages"][data["language"]] = report.get("languages", {}).get(data["language"], 0) + 1
        report["sentiments"][data["sentiment"]] = report.get("sentiments", {}).get(data["sentiment"], 0) + 1
        for keyword in data.get("keywords", []):
            report["keywords"][keyword] = report["keywords"].get(keyword, 0) + 1
        
        all_keywords = report.pop("keywords", {}).items()
        sorted_keywords = sorted(all_keywords, key=lambda item: item[1], reverse=True)  # list of tuples
        top_10_keywords = sorted_keywords[:10]
        report["top_keywords"] = {k: v for k, v in top_10_keywords}
        report["keywords"] = {k: v for k, v in sorted_keywords}  


        with open("report.json", 'w') as f:
            d = json.dumps(report, indent=4)
            f.write(d)

if __name__== "__main__":
    main()