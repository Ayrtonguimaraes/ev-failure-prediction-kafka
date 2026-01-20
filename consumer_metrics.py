import json
import time
import sys
import csv
import os
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Summary, Counter, Gauge

# --- Definição de Métricas ---
LATENCY_METRIC = Summary('ev_latency_seconds', 'Latência End-to-End')
MESSAGES_PROCESSED = Counter('ev_messages_total', 'Total de mensagens processadas')
LAST_MSG_AGE = Gauge('ev_last_msg_age_seconds', 'Idade da última mensagem')

# --- Configuração do Arquivo CSV (Tese) ---
# Cria um nome único baseado no tempo para não sobrescrever testes anteriores
LOG_FILE = f"resultados_tese_{int(time.time())}.csv"

# Cria o arquivo e escreve o cabeçalho
print(f"--- Salvando dados brutos em: {LOG_FILE} ---")
with open(LOG_FILE, mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp_leitura", "vehicle_id", "latencia_segundos"])

# --- Configuração Kafka ---
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-metrics-group', 
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True
}

def main():
    start_http_server(8000)
    print("--- Monitor de Métricas Ativo (Porta 8000) ---")

    consumer = Consumer(kafka_config)
    consumer.subscribe(['telemetria_ev'])

    try:
        # A CORREÇÃO ESTÁ AQUI:
        # O arquivo abre aqui e mantemos ele aberto durante todo o loop
        with open(LOG_FILE, mode='a', newline='') as f:
            writer = csv.writer(f)
            
            while True:
                msg = consumer.poll(1.0)

                if msg is None: continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(f"Erro Kafka: {msg.error()}")
                        continue
                
                try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    
                    ts_now = time.time()
                    ts_envio = payload['ts_envio']
                    latencia = ts_now - ts_envio
                    
                    # 1. Atualiza Prometheus
                    LATENCY_METRIC.observe(latencia)
                    MESSAGES_PROCESSED.inc()
                    LAST_MSG_AGE.set(latencia)
                    
                    # 2. Salva no CSV
                    writer.writerow([ts_now, payload['vehicle_id'], latencia])
                    
                    # IMPORTANTE: Força a gravação no disco imediatamente
                    # (Evita perder dados se der Ctrl+C)
                    f.flush() 
                    
                    # 3. Debug Visual
                    print(f"[<] {payload['vehicle_id']} | Lat: {latencia:.4f}s")
                    
                except KeyError:
                    print("Erro: JSON incompleto.")
                except Exception as e:
                    print(f"Erro processamento: {e}")
                    
    except KeyboardInterrupt:
        print("\nEncerrando e fechando arquivo...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()