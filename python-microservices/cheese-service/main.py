from kafka import KafkaProducer, KafkaConsumer
import json
import random

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

consumer = KafkaConsumer(
    'pizza-with-sauce',
    bootstrap_servers='localhost:9092',
    group_id='queijo-service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'
)

def add_cheese(order_id, pizza):
    pizza['cheese'] = calc_cheese()
    print(f"🧀 Pedido {order_id} recebeu queijo: {pizza['cheese']}")
    producer.send('pizza-with-cheese', key=order_id.encode('utf-8'), value=pizza)
    producer.flush()

def calc_cheese():
    cheeses = ['extra', 'none', 'three cheese', 'goat cheese']
    return random.choice(cheeses)

def start_service():
    print("🔁 Serviço de queijo ativo... (esperando mensagens)")
    for msg in consumer:
        pizza = msg.value
        order_id = msg.key.decode('utf-8') if msg.key else 'sem-id'
        add_cheese(order_id, pizza)

if __name__ == '__main__':
    start_service()
