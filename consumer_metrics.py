import json
import time
import sys
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Summary, Counter, Gauge
import csv

# --- Definição de Métricas (O Coração da Tese) ---

# 1. Latência (Histograma/Summary)
# O Summary calcula automaticamente quantis (P50, P90, P99) no client-side.
# P99 é vital para detectar "tail latency" em sistemas de tempo real.
LATENCY_METRIC = Summary('ev_latency_seconds', 'Latência End-to-End (Producer -> Metrics Consumer)')

# 2. Vazão (Contador)
# O Grafana usará isso para calcular "Mensagens por Segundo" com a função rate()
MESSAGES_PROCESSED = Counter('ev_messages_total', 'Total de mensagens processadas pelo consumidor de métricas')

# 3. Lag de Tempo Real (Gauge)
# Mostra a diferença entre "agora" e a mensagem mais recente processada.
# Útil para saber se o consumidor está "ficando para trás" em tempo real.
LAST_MSG_AGE = Gauge('ev_last_msg_age_seconds', 'Idade da última mensagem processada')

# Configuração Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-metrics-group', # Grupo isolado para receber cópia total dos dados
    'auto.offset.reset': 'latest',  # Em testes de latência, queremos o "agora"
    'enable.auto.commit': True
}

# Configuração do Arquivo de Log
LOG_FILE = f"resultados_tese_{int(time.time())}.csv"

# Cria o arquivo e escreve o cabeçalho
with open(LOG_FILE, mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp_leitura", "vehicle_id", "latencia_segundos"])

print(f"--- Salvando dados brutos em: {LOG_FILE} ---")

def main():
    # Inicia servidor HTTP para o Prometheus coletar
    start_http_server(8000)
    print("--- Monitor de Métricas Ativo (Porta 8000) ---")
    print("Métricas: ev_latency_seconds, ev_messages_total")

    consumer = Consumer(kafka_config)
    consumer.subscribe(['telemetria_ev'])

    try:
        # Abre o arquivo em modo 'append' (adicionar ao final)
        with open(LOG_FILE, mode='a', newline='') as f: # <--- 2. Abre arquivo
            writer = csv.writer(f)
        while True:
            msg = consumer.poll(1.0) # Timeout de 1s para não bloquear CPU em loop infinito

            if msg is None:
                continue
            
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Erro Kafka: {msg.error()}")
                    continue
            
            try:
                    payload = json.loads(msg.value().decode('utf-8'))
                    ts_now = time.time()
                    latencia = ts_now - payload['ts_envio']
                    
                    # Atualiza Prometheus (Mantém o Grafana vivo)
                    LATENCY_METRIC.observe(latencia)
                    MESSAGES_PROCESSED.inc()
                    
                    # <--- 3. SALVA NO CSV PARA A TESE
                    writer.writerow([ts_now, payload['vehicle_id'], latencia])
                    
                    # Print Debug
                    print(f"[<] {payload['vehicle_id']} | Lat: {latencia:.4f}s")
                
            except KeyError:
                print("Erro: JSON sem campo 'ts_envio'. Verifique o Producer.")
                
    except KeyboardInterrupt:
        print("Encerrando monitor...")
    finally:
        consumer.close()

if __name__ == '__main__':
    main()