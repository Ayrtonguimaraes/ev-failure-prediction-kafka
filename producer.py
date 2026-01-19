import time
import json
import random
from datetime import datetime
from confluent_kafka import Producer

# Configurações do Producer
config = {
    'bootstrap.servers': 'localhost:9092', # Localização do nosso Broker Kafka
    'client.id': 'ev-sensor-001'           # Identificação deste simulador
}

# Inicializa o objeto do Producer
producer = Producer(config)

# Função de retorno para confirmar se a mensagem chegou ao Kafka
def delivery_report(err, msg):
    if err is not None:
        print(f"Erro ao entregar mensagem: {err}")
    else:
        print(f"Mensagem enviada para o tópico {msg.topic()} [Partição: {msg.partition()}]")

# Função que simula os sensores do veículo
def gerar_telemetria():
    return {
        "car_id": "EV-XYZ-123",
        "bateria": round(random.uniform(10.0, 100.0), 2),
        "temperatura": round(random.uniform(20.0, 80.0), 2),
        "velocidade": round(random.uniform(0.0, 120.0), 2),
        "lat": -23.5505,
        "long": -46.6333,
        "ts_envio": time.time()  # O "carimbo" de tempo para cálculo de latência
    }

try:
    print("Iniciando simulação de telemetria... Pressione Ctrl+C para parar.")
    while True:
        dados = gerar_telemetria()
        
        # Transforma o dicionário Python em uma string JSON
        payload = json.dumps(dados)
        
        # Envia para o tópico 'telemetria_ev'
        producer.produce(
            'telemetria_ev', 
            value=payload, 
            callback=delivery_report
        )
        
        # O flush() garante que as mensagens saiam do buffer e vão para o Kafka
        producer.flush()
        
        time.sleep(1) # Envia uma leitura por segundo
except KeyboardInterrupt:
    print("Simulação encerrada.")