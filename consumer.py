from kafka import KafkaConsumer
import json

def safe_deserializer(m):
    try:
        return json.loads(m.decode('utf-8'))
    except json.JSONDecodeError:
        return {"raw_data": m.decode('utf-8')}  # Fallback para texto puro

consumer = KafkaConsumer(
    'mensagem',
    bootstrap_servers='localhost:9092',
    value_deserializer=safe_deserializer,
    auto_offset_reset='earliest'
)

print("🛎️ Consumer pronto (Ctrl+C para parar)")
for msg in consumer:
    print(f"📩 Recebido: {msg.value}")