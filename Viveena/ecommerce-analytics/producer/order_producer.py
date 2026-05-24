import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer



producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

CUSTOMERS = list(range(1, 11))
PRODUCTS = [101, 102, 103, 104, 105]

print("Producing orders...")

while True:

    order = {
        "order_id": random.randint(10000, 99999),
        "customer_id": random.choice(CUSTOMERS),
        "product_id": random.choice(PRODUCTS),
        "quantity": random.randint(1, 5),
        "order_time": datetime.now().isoformat()
    }

    producer.send("orders", value=order)
    producer.flush()

    print(order)

    time.sleep(2)


