import config
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import importlib

def reset_topic():
    # Force reload config
    importlib.reload(config)
    
    # Print current configuration
    print(f"\nCurrent configuration:")
    print(f"NUM_PARTITIONS = {config.NUM_PARTITIONS}")
    print(f"REPLICATION_FACTOR = {config.REPLICATION_FACTOR}")
    
    admin_client = KafkaAdminClient(bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS)
    
    # First, try to delete all consumer groups
    try:
        print("Attempting to delete consumer groups...")
        consumer_groups = admin_client.list_consumer_groups()
        for group in consumer_groups:
            try:
                admin_client.delete_consumer_groups([group[0]])
                print(f"Deleted consumer group: {group[0]}")
            except Exception as e:
                print(f"Error deleting consumer group {group[0]}: {str(e)}")
    except Exception as e:
        print(f"Error listing consumer groups: {str(e)}")
    
    # Wait a bit for consumer groups to be deleted
    time.sleep(5)
    
    # Delete topic if it exists
    try:
        # First, check if topic exists
        existing_topics = admin_client.list_topics()
        if config.KAFKA_TOPIC in existing_topics:
            print(f"Topic {config.KAFKA_TOPIC} exists, attempting to delete...")
            
            # Try to delete topic
            try:
                admin_client.delete_topics([config.KAFKA_TOPIC])
                print(f"Sent delete request for topic {config.KAFKA_TOPIC}")
            except Exception as e:
                print(f"Error sending delete request: {str(e)}")
                return
            
            # Wait for topic deletion to complete with retries
            max_retries = 10  # Increased retries
            for i in range(max_retries):
                print(f"Waiting for topic deletion to complete... (attempt {i+1}/{max_retries})")
                time.sleep(10)  # Increased wait time
                
                # Check if topic still exists
                existing_topics = admin_client.list_topics()
                if config.KAFKA_TOPIC not in existing_topics:
                    print(f"Verified topic {config.KAFKA_TOPIC} is deleted")
                    break
                elif i == max_retries - 1:
                    print(f"Error: Topic {config.KAFKA_TOPIC} still exists after {max_retries} attempts")
                    print("Please ensure no consumers or producers are connected to the topic")
                    return
        else:
            print(f"Topic {config.KAFKA_TOPIC} does not exist, proceeding with creation")
        
    except Exception as e:
        print(f"Error during topic deletion: {str(e)}")
        return
    
    # Create topic with proper configuration
    try:
        topic = NewTopic(
            name=config.KAFKA_TOPIC,
            num_partitions=config.NUM_PARTITIONS,
            replication_factor=config.REPLICATION_FACTOR,
            topic_configs={
                'retention.ms': '86400000',  # 24 hours
                'cleanup.policy': 'delete',
                'delete.retention.ms': '86400000',
                'segment.ms': '3600000',  # 1 hour
                'segment.bytes': '1073741824',  # 1GB
                'min.insync.replicas': '1',
                'compression.type': 'producer'
            }
        )
        admin_client.create_topics([topic])
        print(f"Created topic {config.KAFKA_TOPIC} with {config.NUM_PARTITIONS} partitions")
        
        # Verify topic is created
        time.sleep(5)  # Wait for topic creation
        existing_topics = admin_client.list_topics()
        if config.KAFKA_TOPIC not in existing_topics:
            print(f"Error: Topic {config.KAFKA_TOPIC} was not created")
            return
        print(f"Verified topic {config.KAFKA_TOPIC} is created")
        
    except TopicAlreadyExistsError:
        print("Topic already exists")
    except Exception as e:
        print(f"Error creating topic: {str(e)}")

if __name__ == "__main__":
    reset_topic() 