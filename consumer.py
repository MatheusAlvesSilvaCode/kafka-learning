from kafka import KafkaConsumer
import json

def get_kafka_consumer():
    return KafkaConsumer(
        'mensagens',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        security_protocol='PLAINTEXT'
    )

if __name__ == '__main__':
    print("🛎️ Consumer pronto (Ctrl+C para parar)")
    for message in get_kafka_consumer():
        print(f"Recebido: {message.value}")