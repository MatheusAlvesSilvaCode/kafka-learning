from kafka import KafkaProducer
import json
import time
import logging

# Ativa logs detalhados do Kafka
logging.basicConfig(level=logging.INFO)

try:
    print("⏳ Iniciando producer...")
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        api_version=(2, 8, 1)  # Versão explícita do protocolo
    )
    print("✅ Conectado ao Kafka")

    contador = 0
    while True:
        mensagem = {"id": contador, "timestamp": time.time()}
        print(f"🔄 Preparando: {mensagem}")
        
        future = producer.send('mensagem', mensagem)
        future.get(timeout=10)  # Timeout para debug
        
        print(f"📤 Enviado: {mensagem}")
        contador += 1
        time.sleep(1)

except Exception as e:
    print(f"❌ Erro grave: {str(e)}")
    raise