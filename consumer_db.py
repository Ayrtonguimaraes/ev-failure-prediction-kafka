"""Batch Layer Consumer - Persistência em PostgreSQL/PostGIS."""
import sys
import time
import os
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

try:
    import orjson as json
    JSON_LOADS = json.loads
except ImportError:
    import json
    JSON_LOADS = json.loads

load_dotenv()

# --- Configuração Kafka (Lazidis et al.) ---
KAFKA_CONFIG = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP', 'localhost:9092'),
    'group.id': 'ev-batch-consumer',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False,
    'fetch.min.bytes': 1024,
    'fetch.wait.max.ms': 100,
}

DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'database': os.getenv('POSTGRES_DB', 'db_telemetria'),
    'user': os.getenv('POSTGRES_USER', 'user_ev'),
    'password': os.getenv('POSTGRES_PASSWORD', 'pass_ev'),
}

# Configurações de Batch
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '1000'))
FLUSH_TIMEOUT = float(os.getenv('FLUSH_TIMEOUT', '1.0'))
ERROR_THRESHOLD = 0.01
DB_RETRY_DELAY = 5.0


def connect_db(max_retries: int = 5) -> psycopg2.extensions.connection:
    """Conecta ao PostgreSQL com retry."""
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"[INFO] Conectado ao PostgreSQL ({DB_CONFIG['host']}/{DB_CONFIG['database']})")
            return conn
        except psycopg2.OperationalError as e:
            print(f"[AVISO] Tentativa {attempt}/{max_retries} de conexão ao DB falhou: {e}")
            if attempt < max_retries:
                time.sleep(DB_RETRY_DELAY)
    print("[ERRO] Não foi possível conectar ao PostgreSQL.", file=sys.stderr)
    sys.exit(1)

def flush_batch(conn, cursor, buffer):
    """
    Realiza inserção em massa (Bulk Insert) usando COPY/execute_values
    """
    if not buffer:
        return 0

    query = """
        INSERT INTO telemetria_ev 
        (vehicle_id, battery_level, temperature, speed_kmh, location, ts_envio) 
        VALUES %s
    """
    
    # Template para transformar lat/lon em Geometria PostGIS dentro do execute_values
    # Note que passamos os dados crus e o SQL converte
    template = "(%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), to_timestamp(%s))"
    
    try:
        psycopg2.extras.execute_values(cursor, query, buffer, template=template)
        conn.commit()
        return len(buffer)
    except Exception as e:
        conn.rollback()
        print(f"[ERRO BATCH] Falha ao persistir lote: {e}")
        # Aqui você poderia implementar uma DLQ (Dead Letter Queue)
        return 0

def parse_message(data: dict) -> tuple:
    """Extrai campos do payload JSON e retorna tupla para inserção."""
    return (
        data.get('vehicle_id') or data.get('car_id'),
        data['battery_level'],
        data['temperature'],
        data['speed_kmh'],
        data['location']['lon'],
        data['location']['lat'],
        data['ts_envio']
    )


def main():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe(['telemetria_ev'])

    conn = connect_db()
    cursor = conn.cursor()

    buffer = []
    last_flush_time = time.time()
    total_processed = 0
    total_errors = 0

    print(f"--- Batch Consumer Iniciado (Batch Size: {BATCH_SIZE}, Flush: {FLUSH_TIMEOUT}s) ---")

    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[ERRO] Kafka: {msg.error()}")
                else:
                    try:
                        data = JSON_LOADS(msg.value())
                        row = parse_message(data)
                        buffer.append(row)
                    except Exception as e:
                        total_errors += 1
                        if total_processed > 0 and (total_errors / total_processed) > ERROR_THRESHOLD:
                            print(f"[ALERTA] Taxa de Erro > 1%: {(total_errors/total_processed):.2%}")

            # Lógica de Flush (Tamanho ou Tempo)
            current_time = time.time()
            is_batch_full = len(buffer) >= BATCH_SIZE
            is_timeout = (current_time - last_flush_time) >= FLUSH_TIMEOUT

            if (is_batch_full or is_timeout) and buffer:
                inserted = flush_batch(conn, cursor, buffer)

                if inserted > 0:
                    consumer.commit()
                    total_processed += inserted

                buffer = []
                last_flush_time = current_time

    except KeyboardInterrupt:
        print("\n[INFO] Encerrando Batch Consumer...")
    finally:
        if buffer:
            inserted = flush_batch(conn, cursor, buffer)
            if inserted > 0:
                consumer.commit()
                total_processed += inserted
        print(f"[INFO] Total processado: {total_processed}, Erros: {total_errors}")
        consumer.close()
        cursor.close()
        conn.close()
        print("[INFO] Batch Consumer encerrado.")


if __name__ == '__main__':
    main()