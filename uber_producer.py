import json
import time
import random
from kafka import KafkaProducer

# Configuración del productor de Kafka conectándose al puerto 9092
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

topic_name = 'movilidad_streaming'

# Lista de bases de despacho (dispatching bases) de Uber en Nueva York
bases = ['B02512', 'B02617', 'B02682', 'B02764', 'B02598']

print("Iniciando productor de eventos de Uber hacia Kafka...")

try:
    while True:
        # Generar datos simulados de movilidad urbana
        evento = {
            "dispatching_base_num": random.choice(bases),
            "lat": round(random.uniform(40.5, 40.9), 4),
            "lon": round(random.uniform(-74.1, -73.7), 4),
            "timestamp": int(time.time() * 1000)
        }
        
        # Enviar al topic de Kafka
        producer.send(topic_name, value=evento)
        print(f"Enviado a Kafka: {evento}")
        
        # Intervalo de envío de 1 segundo
        time.sleep(1)

except KeyboardInterrupt:
    print("\nProductor detenido por el usuario.")
    producer.close()
