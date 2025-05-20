from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError
import config
import time
import importlib

def reset_topic():
    # Force reload config
    importlib.reload(config)
    
    # Print current configuration
    print(f"\nCurrent configuration:")
    print(f"NUM_PARTITIONS = {config.NUM_PARTITIONS}")
    print(f"REPLICATION_FACTOR = {config.REPLICATION_FACTOR}")
    
    # Create admin client
    admin_client = KafkaAdminClient(
        bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
        request_timeout_ms=30000
    )
    
    try:
        # Delete existing topic
        print(f"\nDeleting topic {config.KAFKA_TOPIC}...")
        try:
            admin_client.delete_topics([config.KAFKA_TOPIC])
            print("Topic deleted successfully")
        except UnknownTopicOrPartitionError:
            print("Topic did not exist, proceeding with creation")
        except Exception as e:
            print(f"Error deleting topic: {str(e)}")
        
        # Wait longer for deletion to complete
        print("Waiting for topic operations to complete...")
        time.sleep(10)
        
        # Create new topic with specified partitions
        print(f"\nCreating topic {config.KAFKA_TOPIC} with {config.NUM_PARTITIONS} partitions...")
        topic = NewTopic(
            name=config.KAFKA_TOPIC,
            num_partitions=config.NUM_PARTITIONS,
            replication_factor=config.REPLICATION_FACTOR
        )
        
        # Print topic configuration before creation
        print(f"Topic configuration to be created:")
        print(f"  Name: {topic.name}")
        print(f"  Partitions: {topic.num_partitions}")
        print(f"  Replication Factor: {topic.replication_factor}")
        
        admin_client.create_topics([topic])
        print("Topic created successfully")
        
        # Wait for topic to be fully created
        print("\nWaiting for topic to be fully created...")
        time.sleep(5)
        
        # Verify topic configuration
        print("\nVerifying topic configuration...")
        max_retries = 3
        for attempt in range(max_retries):
            try:
                topic_config = admin_client.describe_topics([config.KAFKA_TOPIC])
                for topic_info in topic_config:
                    print(f"Topic: {topic_info['topic']}")
                    print(f"Number of partitions: {len(topic_info['partitions'])}")
                    for partition in topic_info['partitions']:
                        print(f"  Partition {partition['partition']}: Leader {partition['leader']}")
                
                # Verify partition count matches config
                if len(topic_info['partitions']) != config.NUM_PARTITIONS:
                    print(f"\nWarning: Topic has {len(topic_info['partitions'])} partitions, expected {config.NUM_PARTITIONS}")
                    if attempt < max_retries - 1:
                        print("Retrying topic creation...")
                        admin_client.delete_topics([config.KAFKA_TOPIC])
                        time.sleep(5)
                        admin_client.create_topics([topic])
                        time.sleep(5)
                        continue
                    else:
                        print("Failed to create topic with correct number of partitions after all attempts")
                else:
                    print("\nTopic configuration verified successfully")
                    break
                    
            except Exception as e:
                print(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt < max_retries - 1:
                    print("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    print("Failed to verify topic configuration after all attempts")
        
    except TopicAlreadyExistsError:
        print("Topic already exists")
    except Exception as e:
        print(f"Error creating topic: {str(e)}")
    finally:
        admin_client.close()

if __name__ == "__main__":
    reset_topic() 