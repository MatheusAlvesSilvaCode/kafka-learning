from django.views.decorators.csrf import csrf_exempt
from django.http import JsonResponse
from kafka import KafkaProducer
import json
import uuid

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@csrf_exempt  # ⬅️ Adicione isso aqui!
def fazer_pedido(request, qtd):
    for _ in range(qtd):
        pizza = {
            "order_id": str(uuid.uuid4()),
            "sauce": "molho de tomate",
            "cheese": "",
            "meats": "",
            "veggies": ""
        }
        print(f"🍕 Pedido criado: {pizza['order_id']}")
        producer.send("pizza-with-sauce", key=pizza['order_id'].encode('utf-8'), value=pizza)
        producer.flush()

    return JsonResponse({"status": f"{qtd} pedidos enviados!"})
