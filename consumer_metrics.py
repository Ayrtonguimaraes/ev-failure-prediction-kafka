import json
import time
from confluent_kafka import Consumer
from prometheus_client import start_http_server, Summary

# Configuração Prometheus
LATENCY_METRIC = Summary('ev_latency_seconds', 'Tempo de viagem da mensagem (Caminho A)')

# Configuração Kafka - Grupo específico de métricas
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-metrics-group', # GRUPO A
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
consumer.subscribe(['telemetria_ev'])

start_http_server(8000)
print("Monitor de Métricas ativo na porta 8000...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        dados = json.loads(msg.value().decode('utf-8'))
        latencia = time.time() - dados['ts_envio']
        LATENCY_METRIC.observe(latencia)
        
except KeyboardInterrupt:
    consumer.close()