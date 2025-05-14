from kafka import KafkaProducer
import json
import time
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

def get_kafka_producer():
    return KafkaProducer(
        bootstrap_servers='localhost:9092', # Servidor de inicio da comunicação, para dizer qual broker vai contactar
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 8, 1),
        security_protocol='PLAINTEXT'
    )

if __name__ == '__main__':
    try:
        producer = get_kafka_producer()
        print("Conectado ao Kafka")
        
        for i in range(1, 101):
            message = {
                'id': i,
                'timestamp': datetime.now().isoformat(),
                'content': f"Mensagem de teste {i}"
            }
            producer.send('mensagens', value=message)
            print(f"Enviado: {message}")
            time.sleep(1)
            
    except Exception as e:
        print(f"Erro: {e}")
        raise