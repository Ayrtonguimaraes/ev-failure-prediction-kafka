import json
import time
from confluent_kafka import Consumer, KafkaError
from prometheus_client import start_http_server, Gauge, Counter, Summary

# --- CONFIGURAÇÕES ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetria_ev"
GROUP_ID = "ev-speed-layer-v1" # Importante: Identidade deste consumidor

# Porta onde o Prometheus vai buscar as métricas
PROMETHEUS_PORT = 8000 

# --- DEFINIÇÃO DAS MÉTRICAS PROMETHEUS ---
# 1. Contador: Quantas mensagens processamos no total?
MSG_COUNTER = Counter('ev_processed_total', 'Total de mensagens processadas pela Speed Layer')

# 2. Gauge: Qual a latência da ÚLTIMA mensagem (velocímetro instantâneo)
LATENCY_GAUGE = Gauge('ev_latency_last_ms', 'Latência End-to-End da última mensagem (ms)')

# 3. Summary: Estatística (Média e Percentis) da latência
LATENCY_SUMMARY = Summary('ev_latency_stats_ms', 'Estatísticas de latência')

# --- FUNÇÕES AUXILIARES ---

def get_header_value(headers, key):
    """
    O Kafka entrega headers como uma lista de tuplas: [('key', b'value'), ...]
    Essa função busca o valor de uma chave específica.
    """
    if headers is None:
        return None
        
    for h_key, h_value in headers:
        if h_key == key:
            return h_value.decode('utf-8') # Decodifica bytes para string
    return None

def process_message(msg):
    """
    Lógica de processamento da Speed Layer
    """
    # 1. Parse do JSON (Simulação de processamento real)
    try:
        data = json.loads(msg.value().decode('utf-8'))
        # Aqui você faria análises rápidas (ex: "Velocidade > 120?")
    except json.JSONDecodeError:
        print("❌ Erro ao decodificar JSON")
        return

    # 2. RASTREABILIDADE (O cálculo da Tese)
    # Buscamos o carimbo que a Ponte colocou
    ts_bridge_str = get_header_value(msg.headers(), 'trace_bridge_ts')
    
    if ts_bridge_str:
        ts_bridge = int(ts_bridge_str)
        ts_now = int(time.time() * 1000)
        
        # A Conta Mágica: Agora - Hora que chegou na ponte
        latency_ms = ts_now - ts_bridge
        
        # Atualiza as métricas do Prometheus
        LATENCY_GAUGE.set(latency_ms)
        LATENCY_SUMMARY.observe(latency_ms)
        
    else:
        print("⚠️ Mensagem sem header de rastreabilidade!")

    # Incrementa contador
    MSG_COUNTER.inc()

# --- MAIN LOOP ---

if __name__ == '__main__':
    # 1. Inicia o servidor HTTP para o Prometheus (em Thread separada)
    print(f"📡 Iniciando servidor de métricas na porta {PROMETHEUS_PORT}...")
    start_http_server(PROMETHEUS_PORT)

    # 2. Configura o Consumer Kafka
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest', # Se cair, comece a ler do NOVO (Speed Layer não quer dado velho)
        'enable.auto.commit': True     # Commit automático para simplificar
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    print(f"👀 Speed Layer ouvindo: {KAFKA_TOPIC}...")

    try:
        while True:
            # Poll(1.0) espera até 1 segundo por uma mensagem
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Erro Consumer: {msg.error()}")
                    continue

            # Se chegou mensagem válida, processa
            process_message(msg)

    except KeyboardInterrupt:
        print("\n🛑 Parando Speed Layer...")
    finally:
        consumer.close()