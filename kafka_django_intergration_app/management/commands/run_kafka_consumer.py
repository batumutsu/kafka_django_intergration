from django.core.management.base import BaseCommand
from kafka_django_intergration_app.consumer import KafkaMessageConsumer
import time

class Command(BaseCommand):
    help = 'Runs Kafka Consumer'

    def handle(self, *args, **options):
        consumer = KafkaMessageConsumer()
        consumer.start()
        
        try:
            # Keep the thread running
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            consumer.stop()
            self.stdout.write(self.style.SUCCESS('Kafka Consumer stopped'))