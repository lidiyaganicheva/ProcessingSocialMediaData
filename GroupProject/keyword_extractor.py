import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.probability import FreqDist
import json
from kafka import KafkaConsumer, KafkaProducer
import config
import logging
import sys

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(stream=sys.stdout)
    ]
)

nltk.download('punkt')
nltk.download('stopwords')
nltk.download('averaged_perceptron_tagger')

class KeywordExtractor:
    def __init__(self):
        self.stop_words = set(stopwords.words('english'))
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.consumer = KafkaConsumer(
            config.KAFKA_SENTIMENT_TOPIC,
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='keyword-extractor-group',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"Initialized KeywordExtractor with topic: {config.KAFKA_SENTIMENT_TOPIC}")

    def extract_keywords(self, text, num_keywords=5):
        if not text or not isinstance(text, str):
            logging.warning(f"Invalid text for keyword extraction: {text}")
            return []
        tokens = word_tokenize(text.lower())
        filtered_tokens = [word for word in tokens if word.isalpha() and word not in self.stop_words]
        if not filtered_tokens:
            logging.warning("No valid tokens found after filtering")
            return []
        fdist = FreqDist(filtered_tokens)
        keywords = [word for word, freq in fdist.most_common(num_keywords)]
        return keywords

    def process_messages(self):
        logging.info("Starting keyword extraction service...")
        message_count = 0
        try:
            for message in self.consumer:
                message_count += 1
                try:
                    logging.debug(f"Processing message {message_count}")
                    sentiment_data = message.value

                    comment_text = sentiment_data.get("1", "")
                    if not comment_text:
                        logging.warning(f"No text found in message {message_count}")
                        continue
                        
                    keywords = self.extract_keywords(comment_text)
                    if not keywords:
                        logging.warning(f"No keywords extracted from message {message_count}")
                        continue

                    result = sentiment_data.copy()
                    result['keywords'] = keywords
                    
                    future = self.producer.send(config.KAFKA_KEYWORDS_TOPIC, value=result)
                    future.get(timeout=10)
                    
                    logging.info(f"Processed message {message_count} - ID: {result.get('Unnamed: 0')} - Keywords: {keywords}")
                    
                except Exception as e:
                    logging.error(f"Error processing message {message_count}: {str(e)}")
                    logging.error(f"Message that caused error: {message.value}")
                    continue
                    
        except KeyboardInterrupt:
            logging.info(f"Shutting down keyword extraction service after processing {message_count} messages...")
        finally:
            self.consumer.close()
            self.producer.close()

if __name__ == "__main__":
    extractor = KeywordExtractor()
    extractor.process_messages()
