from flask import Flask, jsonify
from threading import Thread
import uuid
import json
from kafka import KafkaProducer
import random

app = Flask(__name__)

# Criar produtor Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Endpoint para gerar pedidos
@app.route('/order/<int:qtd>', methods=['POST'])
def fazer_pedido(qtd):
    for _ in range(qtd):
        pizza = {
            "order_id": str(uuid.uuid4()),
            "sauce": "",
            "cheese": "",
            "meats": "",
            "veggies": ""
        }
        print(f"🍕 Pedido criado: {pizza['order_id']}")
        producer.send("pizza-with-sauce", key=pizza['order_id'].encode('utf-8'), value=pizza)
        producer.flush()
    return jsonify({"status": f"{qtd} pedidos enviados!"})

# Apenas roda a API
if __name__ == '__main__':
    app.run(debug=True)
