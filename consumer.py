import json
import time
import psycopg2
from confluent_kafka import Consumer
from prometheus_client import start_http_server, Summary

# 1. Configuração de Métricas (Caminho A)
# Criamos uma métrica do tipo Summary para calcular a latência média
LATENCY_METRIC = Summary('ev_latency_seconds', 'Tempo de viagem da mensagem do sensor ao consumidor')

# 2. Configuração do Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-monitoring-group', # Identificador do grupo de consumo
    'auto.offset.reset': 'earliest'     # Começa a ler desde a primeira mensagem disponível
}
consumer = Consumer(kafka_config)
consumer.subscribe(['telemetria_ev'])

# 3. Configuração do PostgreSQL (Caminho B)
# Usamos as credenciais que você definiu no arquivo .env
def conectar_banco():
    return psycopg2.connect(
        host="localhost",
        database="db_ev_telemetry",
        user="user_ev",
        password="password_ev"
    )

print("Consumidor iniciado e aguardando mensagens...")

# Inicia o servidor de métricas do Prometheus na porta 8000
start_http_server(8000)

try:
    conn = conectar_banco()
    cursor = conn.cursor()

    while True:
        # 1. Tenta ler uma mensagem do Kafka (aguarda até 1 segundo)
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Erro no Kafka: {msg.error()}")
            continue

        # 2. Decodifica o JSON recebido
        dados = json.loads(msg.value().decode('utf-8'))
        
        # 3. Cálculo de Latência (Caminho A)
        ts_agora = time.time()
        latencia = ts_agora - dados['ts_envio']
        
        # Envia a métrica para o Prometheus
        LATENCY_METRIC.observe(latencia)
        
        # 4. Inserção no PostgreSQL (Caminho B)
        query = """
            INSERT INTO telemetria_ev (car_id, bateria, temperatura, velocidade, ts_envio, ts_recebimento)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        valores = (
            dados['car_id'], 
            dados['bateria'], 
            dados['temperatura'], 
            dados['velocidade'], 
            dados['ts_envio'], 
            ts_agora
        )
        
        cursor.execute(query, valores)
        conn.commit() # Confirma a gravação no banco

        print(f"Veículo: {dados['car_id']} | Latência: {latencia:.4f}s | Status: Salvo no DB")

except KeyboardInterrupt:
    print("Consumidor encerrado pelo usuário.")
finally:
    # Fecha as conexões ao sair
    consumer.close()
    if conn:
        cursor.close()
        conn.close()