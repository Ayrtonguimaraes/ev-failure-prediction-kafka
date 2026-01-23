import json
import time
import csv
import sys
import os
from confluent_kafka import Consumer, KafkaError, TopicPartition
from prometheus_client import start_http_server, Summary, Counter, Gauge

# --- Métricas Prometheus ---
LATENCY_METRIC = Summary('ev_latency_seconds', 'Latência End-to-End')
CONSUMER_LAG = Gauge('ev_consumer_lag', 'Consumer Lag')
MSG_RATE = Gauge('ev_throughput_mps', 'Throughput')

LOG_FILE = "metrics_speed_layer.csv"

kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-metrics-SPEED-V4', 
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
}

def get_lag(consumer, topic, partition):
    try:
        tp = TopicPartition(topic, partition)
        low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
        pos = consumer.position([tp])
        if pos and pos[0].offset != KafkaError.OFFSET_INVALID:
            return high - pos[0].offset
    except Exception:
        return 0
    return 0

def main():
    # Inicia servidor Prometheus
    try:
        start_http_server(8000)
        print("--- Metrics Monitor Ativo (Porta 8000) ---")
    except Exception as e:
        print(f"Aviso Prometheus: {e}")

    consumer = Consumer(kafka_config)
    consumer.subscribe(['telemetria_ev'])

    # Abre com buffer=1 (line buffering) para garantir escrita
    # Se o arquivo já existir, 'w' vai sobrescrever.
    csv_file = open(LOG_FILE, mode='w', newline='', buffering=1)
    writer = csv.writer(csv_file)
    
    # Cabeçalho para Análise (Lazidis et al.)
    writer.writerow(["ts_consumo", "lag", "throughput", "avg_latencia", "max_latencia"])
    csv_file.flush()
    os.fsync(csv_file.fileno())

    msg_count_window = 0
    latencias_window = [] # Buffer para calcular estatísticas do segundo
    last_tick = time.time()

    print("--- Aguardando mensagens... ---")

    try:
        while True:
            msg = consumer.poll(0.5)

            if msg is None: continue
            if msg.error(): continue

            try:
                payload = json.loads(msg.value())
                ts_now = time.time()
                ts_envio = float(payload['ts_envio'])
                
                latencia = ts_now - ts_envio
                
                # Prometheus
                LATENCY_METRIC.observe(latencia)
                
                # Acumuladores para o CSV
                msg_count_window += 1
                latencias_window.append(latencia)

                # Monitoramento Periódico (A cada 1.0s)
                if (ts_now - last_tick) >= 1.0:
                    # 1. Coleta Lag
                    lag = get_lag(consumer, msg.topic(), msg.partition())
                    
                    # 2. Atualiza Prometheus Gauges
                    CONSUMER_LAG.set(lag)
                    MSG_RATE.set(msg_count_window)
                    
                    # 3. Cálculos Estatísticos
                    if latencias_window:
                        avg_lat = sum(latencias_window) / len(latencias_window)
                        max_lat = max(latencias_window)
                    else:
                        avg_lat = 0.0
                        max_lat = 0.0
                    
                    # 4. Escreve no CSV
                    writer.writerow([
                        f"{ts_now:.2f}", 
                        lag, 
                        msg_count_window, 
                        f"{avg_lat:.4f}", 
                        f"{max_lat:.4f}"
                    ])
                    
                    # 5. GARANTIA DE DISCO (FLUSH)
                    csv_file.flush() 
                    os.fsync(csv_file.fileno())
                    
                    # 6. Reset Window
                    msg_count_window = 0
                    latencias_window = [] 
                    last_tick = ts_now
                    
                    # Log Visual apenas se houver problema
                    if lag > 2000:
                        print(f"[ALERTA] Lag Alto: {lag}")

            except Exception:
                pass

    except KeyboardInterrupt:
        print("\nSalvando e fechando Metrics...")
    finally:
        csv_file.close()
        consumer.close()

if __name__ == '__main__':
    main()