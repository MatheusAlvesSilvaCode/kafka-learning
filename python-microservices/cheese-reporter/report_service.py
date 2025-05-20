import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Configurações (substitua conforme seu config.properties)
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'  # ou seu servidor Kafka
CONSUMER_GROUP = 'cheese-report-group'      # substitua pelo seu grupo
AUTO_OFFSET_RESET = 'earliest'             # ou 'latest' conforme necessidade

cheeses = {}
cheese_topic = 'pizza-with-cheese'

def start_consumer():
    cheese_consumer = KafkaConsumer (
        cheese_topic,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=CONSUMER_GROUP,
        auto_offset_reset=AUTO_OFFSET_RESET,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    
    try:
        for message in cheese_consumer:  # KafkaConsumer é um iterável
            try:
                pizza = message.value
                add_cheese_count(pizza['cheese'])
            except Exception as e:
                print(f'Erro ao processar mensagem: {e}')
    except KeyboardInterrupt:
        print("Interrompendo consumidor...")
    finally:
        cheese_consumer.close()

def add_cheese_count(cheese):
    cheeses[cheese] = cheeses.get(cheese, 0) + 1

def generate_report():
    return json.dumps(cheeses, indent=4)

if __name__ == '__main__':
    start_consumer()