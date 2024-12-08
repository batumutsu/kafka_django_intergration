from django.http import JsonResponse
import time
import json
from confluent_kafka import Producer

class KafkaMessageProducer:
    def __init__(self, bootstrap_servers='localhost:9092', topic='django_kafka_topic'):
        # Configuration for the Kafka producer
        self.conf = {
            'bootstrap.servers': bootstrap_servers,
        }
        self.producer = Producer(self.conf)
        self.topic = topic
    
    def delivery_report(self, err, msg):
        """
        Callback triggered by produce() for checking delivery reports
        """
        if err is not None:
            print(f'Message delivery failed1')
        else:
            print(f'Message delivered to {msg.topic()}')

    def send_message(self, message):
        try:
            # Convert message to JSON string
            msg_string = json.dumps(message).encode('utf-8')
            
            # Produce the message to Kafka
            self.producer.produce(
                self.topic, 
                value=msg_string, 
                callback=self.delivery_report
            )
            
            # Wait for any outstanding messages to be delivered
            self.producer.poll(0)
            
            return True
        except Exception as e:
            print(f'Error sending message: {e}')
            return False
        
    def __del__(self):
        # Flush any messages in the producer's queue
        self.producer.flush()