import json
import psycopg2
from confluent_kafka import Consumer, KafkaError

# --- CONFIGURAÇÕES ---
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "telemetria_ev"
GROUP_ID = "ev-batch-layer-v1" # <--- GRUPO DIFERENTE! (Para ler uma cópia separada dos dados)

# Configurações do Banco (bater com o docker-compose.yml)
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "db_telemetria"
DB_USER = "user_ev"
DB_PASS = "pass_ev"

# --- CONEXÃO COM O BANCO ---
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    return conn

# --- PERSISTÊNCIA ---
def salvar_no_banco(conn, data):
    """
    Insere o JSON no PostgreSQL.
    A tabela 'telemetria' foi criada pelo script schema.sql na Fase 1.
    """
    cursor = conn.cursor()
    try:
        sql = """
            INSERT INTO telemetria (
                vehicle_id, velocidade, bateria, temperatura_motor, 
                latitude, longitude, ts_sensor, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s / 1000.0), NOW())
        """
        cursor.execute(sql, (
            data['vehicle_id'],
            data['velocidade'],
            data['bateria'],
            data['temperatura_motor'],
            data['latitude'],
            data['longitude'],
            data['ts_sensor'] # Timestamp original do sensor
        ))
        conn.commit() # Confirma a transação no banco
    except Exception as e:
        conn.rollback() # Desfaz se der erro
        print(f"❌ Erro de Banco: {e}")
        raise e # Relança o erro para o Kafka não marcar como lido
    finally:
        cursor.close()

# --- MAIN LOOP ---
if __name__ == '__main__':
    # 1. Configura Consumer Kafka
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'earliest', # Batch Layer quer ler TUDO, desde o início da história
        'enable.auto.commit': False      # <--- IMPORTANTE: Nós controlamos o commit
    }
    
    consumer = Consumer(conf)
    consumer.subscribe([KAFKA_TOPIC])
    
    # 2. Conecta no Banco
    print("⏳ Conectando ao PostgreSQL...")
    try:
        db_conn = get_db_connection()
        print("✅ Conectado ao Banco de Dados!")
    except Exception as e:
        print(f"❌ Falha fatal no banco: {e}")
        exit(1)

    print(f"💾 Batch Layer (Arquivista) ouvindo: {KAFKA_TOPIC}...")

    try:
        while True:
            # Lê mensagem
            msg = consumer.poll(1.0)

            if msg is None: continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF: continue
                print(f"❌ Erro Consumer: {msg.error()}")
                continue

            # Processa
            try:
                payload = msg.value().decode('utf-8')
                data = json.loads(payload)
                
                # Salva no Banco
                salvar_no_banco(db_conn, data)
                
                # SÓ AGORA avisa o Kafka que leu com sucesso
                consumer.commit(asynchronous=True)
                
                # Feedback visual (Batch costuma ser silenciosa, mas vamos ver 1 por vez)
                # print(f"💾 Salvo: {data['vehicle_id']}")
                
            except Exception as e:
                print(f"⚠️ Erro ao processar mensagem: {e}")

    except KeyboardInterrupt:
        print("\n🛑 Parando Batch Layer...")
    finally:
        consumer.close()
        db_conn.close()