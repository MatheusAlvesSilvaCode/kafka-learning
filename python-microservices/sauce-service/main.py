from kafka import KafkaProducer, KafkaConsumer
import json
import random

# Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Consumer
consumer = KafkaConsumer(
    'pizza',
    bootstrap_servers='localhost:9092',
    group_id='molho-service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'
)

def add_sauce(order_id, pizza):
    pizza['sauce'] = calc_sauce()
    print(f"🍅 Pedido {order_id} recebeu molho: {pizza['sauce']}")
    producer.send('pizza-with-sauce', key=order_id.encode('utf-8'), value=pizza)
    producer.flush()

def calc_sauce():
    sauces = ['regular', 'light', 'extra', 'none', 'alfredo', 'regular', 'light', 'extra', 'alfredo']
    return random.choice(sauces)

def start_service():
    print("🔁 Serviço de molho ativo...")
    for msg in consumer:
        pizza = msg.value
        order_id = msg.key.decode('utf-8') if msg.key else 'sem-id'
        add_sauce(order_id, pizza)

if __name__ == '__main__':
    start_service()
