from kafka import KafkaProducer, KafkaConsumer
import json
import random

# Criar Producer Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Criar Consumer Kafka
consumer = KafkaConsumer(
    'pizza-with-cheese',
    bootstrap_servers='localhost:9092',
    group_id='carne-service',
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest'
)

# Função para adicionar carnes
def add_meats(order_id, pizza):
    pizza['meats'] = calc_meats()
    print(f" 🍖 Pedido {order_id} recebeu carnes: {pizza['meats']}")
    producer.send('pizza-with-meats', key=order_id.encode('utf-8'), value=pizza)
    producer.flush()

# Função auxiliar para sortear carnes
def calc_meats():
    i = random.randint(0, 4)
    meats = ['pepperoni', 'sausage', 'ham', 'anchovies', 'salami', 'bacon']
    if i == 0:
        return 'none'
    return ' & '.join(random.sample(meats, i))

# Loop principal de consumo e envio
def start_service():
    print("🧠 Serviço de carnes rodando...")
    for msg in consumer:
        pizza = msg.value
        order_id = msg.key.decode('utf-8') if msg.key else 'sem-id'
        add_meats(order_id, pizza)

if __name__ == '__main__':
    start_service()
