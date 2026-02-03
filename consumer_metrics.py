"""Speed Layer Consumer - Métricas em tempo real via Prometheus."""
import sys
import time
import os
from confluent_kafka import Consumer, KafkaError, TopicPartition
from prometheus_client import start_http_server, Summary, Counter, Gauge

try:
    import orjson
    JSON_LOADS = orjson.loads
except ImportError:
    import json
    JSON_LOADS = json.loads

# --- Métricas Prometheus ---
LATENCY_METRIC = Summary('ev_latency_seconds', 'Latência End-to-End')
LATENCY_AVG = Gauge('ev_latency_avg_seconds', 'Latência Média (janela 1s)')
LATENCY_MAX = Gauge('ev_latency_max_seconds', 'Latência Máxima (janela 1s)')
CONSUMER_LAG = Gauge('ev_consumer_lag', 'Consumer Lag')
MSG_RATE = Gauge('ev_throughput_mps', 'Throughput (msg/s)')
MESSAGES_TOTAL = Counter('ev_messages_total', 'Total de mensagens processadas')

PROMETHEUS_PORT = int(os.getenv('PROMETHEUS_PORT', '8000'))

KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
    'group.id': 'ev-speed-consumer',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': True,
    'fetch.wait.max.ms': 50,
}

def get_lag(consumer, topic: str, partition: int) -> int:
    """Calcula o lag do consumer para uma partição."""
    try:
        tp = TopicPartition(topic, partition)
        low, high = consumer.get_watermark_offsets(tp, timeout=1.0)
        positions = consumer.position([tp])
        if positions and positions[0].offset >= 0:
            return max(0, high - positions[0].offset)
    except Exception:
        pass
    return 0


def main():
    try:
        start_http_server(PROMETHEUS_PORT)
        print(f"[INFO] Prometheus metrics server em :{PROMETHEUS_PORT}")
    except Exception as e:
        print(f"[AVISO] Não foi possível iniciar Prometheus: {e}")

    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['telemetria_ev'])

    msg_count_window = 0
    latencias_window = []
    last_tick = time.time()

    print("--- Speed Layer Consumer Iniciado ---")

    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(f"[ERRO] Kafka: {msg.error()}")
                continue

            try:
                payload = JSON_LOADS(msg.value())
                ts_now = time.time()
                ts_envio = float(payload['ts_envio'])

                latencia = ts_now - ts_envio

                LATENCY_METRIC.observe(latencia)
                MESSAGES_TOTAL.inc()

                msg_count_window += 1
                latencias_window.append(latencia)

                if (ts_now - last_tick) >= 1.0:
                    lag = get_lag(consumer, msg.topic(), msg.partition())

                    CONSUMER_LAG.set(lag)
                    MSG_RATE.set(msg_count_window)

                    if latencias_window:
                        avg_lat = sum(latencias_window) / len(latencias_window)
                        max_lat = max(latencias_window)
                        LATENCY_AVG.set(avg_lat)
                        LATENCY_MAX.set(max_lat)

                    if lag > 2000:
                        print(f"[ALERTA] Lag alto: {lag}")

                    msg_count_window = 0
                    latencias_window = []
                    last_tick = ts_now

            except Exception:
                pass

    except KeyboardInterrupt:
        print("\n[INFO] Encerrando Speed Layer Consumer...")
    finally:
        consumer.close()
        print("[INFO] Speed Layer Consumer encerrado.")


if __name__ == '__main__':
    main()