from kafka import KafkaProducer # Importa o KafkoProducer.
import json # Importa o json
import time # Importa o time, para podermos converter.
import logging # Para ver o que está acontecendo nos bastidores.


logging.basicConfig(level=logging.INFO) # Ativa logs detalhados do Kafka

try: # Tente:
    print("Iniciando producer...") # Printa na tela, quando estiver iniciando o o producer, se der erro vai para o except.
    producer = KafkaProducer( 
        bootstrap_servers='localhost:9092', # Onde está o kafka.
        value_serializer=lambda v: json.dumps(v).encode('utf-8'), # Transforma em dicionário de texto.
        api_version=(2, 8, 1)  # Qual versão especifica do kafka usar.
    )
    print("Conectado ao Kafka") # Caso dê certo irá passar por aqui e printar na tela

    contador = 0 # Contadorpara o while
    while True: # Em quanto isso for verdadeiro, faça:
        mensagem = {"id": contador, "timestamp": time.time()} # Cria um dicionário com número de tela e horário atual.
        print(f"Preparando: {mensagem}") # Mostra a mensagem criada.
        
        future = producer.send('mensagem', mensagem) # Aqui envia a mensagem para o tópico mensagem do Kafka
        future.get(timeout=10)  # Espera até ela ser enviada, para debug.
        
        print(f"Enviado: {mensagem}") # mostra a mensagem
        contador += 1 # incrementa na variavél contador
        time.sleep(1) # Espera o time de 1 segundo

except Exception as e: # Excetpion é 'e' que já por sí só é o erro, quando printamos ele printamos o erro que ele traz.
    print(f"Erro grave: {str(e)}") # Se der algo errado, mostre um erro e pare o programa.
    raise # repassa o erro.