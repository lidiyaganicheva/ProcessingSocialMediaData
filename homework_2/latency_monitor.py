import json
import time
from datetime import datetime
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import numpy as np
import config


class LatencyMonitor:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'processed-messages',
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            group_id='latency_monitor_group',
            auto_offset_reset='earliest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.latency_data = []
        self.report_interval = 60
        self.last_report_time = time.time()

    def calculate_latency(self, message):
        """
        Calculate processing latency for a message.
        """
        try:
            current_time = time.time()
            original_timestamp = message.value.get('timestamp')
            processing_time = message.value.get('processing_time')
            if original_timestamp is None or processing_time is None:
                print(f"Missing timestamp data in message: {message.value}")
                return None
            total_latency = (processing_time - original_timestamp) * 1000
            self.latency_data.append({
                'timestamp': datetime.fromtimestamp(current_time),
                'message_id': message.value.get('id', 'unknown'),
                'latency_ms': float(total_latency),
                'original_time': datetime.fromtimestamp(original_timestamp),
                'processing_time': datetime.fromtimestamp(processing_time),
                'processed_by': message.value.get('processed_by', 'unknown'),
                'partition': int(message.partition)
            })
            return total_latency
        except Exception as e:
            print(f"Error calculating latency: {str(e)}")
            print(f"Message value: {message.value}")
            return None

    def generate_report(self):
        if not self.latency_data:
            return
        df = pd.DataFrame(self.latency_data)
        report = {
            'timestamp': datetime.now().isoformat(),
            'total_messages': int(len(df)),
            'max_latency_ms': float(df['latency_ms'].max()),
            'min_latency_ms': float(df['latency_ms'].min()),
            'avg_latency_ms': float(df['latency_ms'].mean()),
            'p95_latency_ms': float(df['latency_ms'].quantile(0.95)),
            'p99_latency_ms': float(df['latency_ms'].quantile(0.99))
        }
        consumer_stats = df.groupby('processed_by')['latency_ms'].agg(['count', 'mean', 'min', 'max'])
        consumer_stats = consumer_stats.astype(float)
        df.to_csv('latency_report.csv', mode='a', header=False, index=False)
        self.producer.send('latency-reports', value=report)
        self.producer.flush()
        print("\n=== Latency Report ===")
        print(f"Total Messages: {report['total_messages']}")
        print(f"Max Latency: {report['max_latency_ms']:.2f} ms")
        print(f"Min Latency: {report['min_latency_ms']:.2f} ms")
        print(f"Average Latency: {report['avg_latency_ms']:.2f} ms")
        print(f"95th Percentile: {report['p95_latency_ms']:.2f} ms")
        print(f"99th Percentile: {report['p99_latency_ms']:.2f} ms")
        print("\nPer-Consumer Statistics:")
        print(consumer_stats)
        print("====================\n")
        self.latency_data = []

    def run(self):
        print("Starting latency monitoring service...")
        print(f"Monitoring topic: processed-messages")
        print(f"Bootstrap servers: {config.KAFKA_BOOTSTRAP_SERVERS}")
        try:
            for message in self.consumer:
                try:
                    latency = self.calculate_latency(message)
                    if latency is not None:
                        current_time = time.time()
                        if current_time - self.last_report_time >= self.report_interval:
                            self.generate_report()
                            self.last_report_time = current_time
                except Exception as e:
                    print(f"Error processing message: {str(e)}")
                    continue
        except KeyboardInterrupt:
            print("\nStopping latency monitoring service...")
            self.generate_report()
        finally:
            self.consumer.close()
            self.producer.close()


if __name__ == "__main__":
    monitor = LatencyM
