import json
import psycopg2
from confluent_kafka import Consumer

# Configuração Kafka - Grupo específico do banco
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-database-group', # GRUPO B
    'auto.offset.reset': 'earliest'
}
consumer = Consumer(kafka_config)
consumer.subscribe(['telemetria_ev'])

# Conexão com o Banco
conn = psycopg2.connect(host="localhost", database="db_ev_telemetry", user="user_ev", password="password_ev")
cursor = conn.cursor()

print("Persistência no Banco ativa...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None: continue
        
        dados = json.loads(msg.value().decode('utf-8'))
        query = "INSERT INTO telemetria_ev (car_id, bateria, temperatura, velocidade, ts_envio) VALUES (%s, %s, %s, %s, %s)"
        cursor.execute(query, (dados['car_id'], dados['bateria'], dados['temperatura'], dados['velocidade'], dados['ts_envio']))
        conn.commit()
except KeyboardInterrupt:
    consumer.close()
    conn.close()