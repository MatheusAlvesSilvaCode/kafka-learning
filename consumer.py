from kafka import KafkaConsumer # Importando o Kafkaconsumer
import json # Importando json para leitura das chaves.

def safe_deserializer(m): # Criando função, chamada safe_deserializer que recebe o parâmetro 'm'
    try: # tente
        return json.loads(m.decode('utf-8')) #Tenta tranformar o texto em json em um python, primeiro ele codifica com o decode depois tranforma com o json loads
    except json.JSONDecodeError: # Se der eurro, não for um json.
        return {"raw_data": m.decode('utf-8')}  # Ele guarda, mas só como texto puro

consumer = KafkaConsumer( #Cria um consumidor Kafka, que vai escutar o tópico que vc criou, com o comando no kafka
    'mensagem', # Nome do tópico que ele vai escutar.
    bootstrap_servers='localhost:9092', # Aqui onde está o Kafka
    value_deserializer=safe_deserializer, # Como ele vai entender os dados que chegam
    auto_offset_reset='earliest' # começa do inicio as mensagens, se for a primeira.
)

print("🛎️ Consumer pronto (Ctrl+C para parar)") # Um aviso, que se chegar aqui passou pela função e está sendo escutado corretamente.
for msg in consumer: # Para cada msg 'mensagem' em consumer, faça:
    print(f"📩 Recebido: {msg.value}") # Mostre a mensagem recebida na tela. 