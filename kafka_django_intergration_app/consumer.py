import json
import threading
import time
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.admin import AdminClient, NewTopic

class KafkaMessageConsumer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='django_kafka_topic', group_id='django-consumer-group-1'):
        admin_client = AdminClient({'bootstrap.servers': bootstrap_servers})
        topic_metadata = admin_client.list_topics(timeout=10)
        
        if topic not in topic_metadata.topics:
            print(f"Topic '{topic}' does not exist. Creating it...")
            new_topic = NewTopic(topic, num_partitions=3, replication_factor=1)
            admin_client.create_topics([new_topic])
            print(f"Topic '{topic}' created.")
        else:
            print(f"Topic '{topic}' already exists.")

        # Configuration for the Kafka consumer
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest'
        }
        
        self.consumer = Consumer(self.conf)
        self.topic = topic
        self.stop_event = threading.Event()
    
    def consume(self):
        try:
            # Subscribe to the topic
            self.consumer.subscribe([self.topic])
            
            while not self.stop_event.is_set():
                # Poll for messages
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    # Handle errors
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print('Reached end of partition')
                    elif msg.error():
                        print(f'Error: {msg.error()}')
                    continue
                
                # Process the message
                try:
                    message_value = msg.value().decode('utf-8')
                    parsed_message = json.loads(message_value)
                    print(f'Received message: {parsed_message}')
                except Exception as e:
                    print(f'Error processing message: {e}')
        
        except Exception as e:
            print(f'Error in consumer: {e}')
        finally:
            # Close the consumer
            self.consumer.close()
    
    def start(self):
        # Start consumer in a separate thread
        self.consumer_thread = threading.Thread(target=self.consume)
        self.consumer_thread.start()
    
    def stop(self):
        # Stop the consumer
        self.stop_event.set()
        self.consumer_thread.join()