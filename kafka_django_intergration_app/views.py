from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt
from .producer import KafkaMessageProducer
import json
import time

@csrf_exempt
def send_message(request):
    if request.method == 'POST':
        data = json.loads(request.body)
        message = {
            'timestamp': time.time(),
            'data': data.get('data', 'No data provided')
        }
        
        producer = KafkaMessageProducer()
        result = producer.send_message(message)
        
        return JsonResponse({
            'status': 'success' if result else 'failed',
            'message': message
        })
    return JsonResponse({'error': 'Invalid request method'}, status=400)