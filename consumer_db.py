import json
import time
import os
import sys
import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

load_dotenv()

# --- Configuração Otimizada (Lazidis et al.) ---
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-db-BATCH-V4', 
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False, # Commit manual após persistência no DB (Garantia de Entrega)
    # Fetch otimizado para Batch
    'fetch.min.bytes': 1024,     
    'fetch.wait.max.ms': 100    
}

DB_CONFIG = {
    "host": "localhost",
    "database": os.getenv("POSTGRES_DB", "db_telemetria"),
    "user": os.getenv("POSTGRES_USER", "user_ev"),
    "password": os.getenv("POSTGRES_PASSWORD", "pass_ev")
}

# Configurações de Batch
BATCH_SIZE = 1000       # Acumula 1000 mensagens
FLUSH_TIMEOUT = 1.0     # Ou escreve a cada 1 segundo
ERROR_THRESHOLD = 0.01  # 1% limite (Lazidis et al.)

def connect_db():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn

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

def main():
    consumer = Consumer(kafka_config)
    consumer.subscribe(['telemetria_ev'])
    
    conn = connect_db()
    cursor = conn.cursor()
    
    buffer = []
    last_flush_time = time.time()
    total_processed = 0
    total_errors = 0

    print(f"--- Batch Consumer Iniciado (Batch Size: {BATCH_SIZE}) ---")

    try:
        while True:
            msg = consumer.poll(0.1) # Poll não bloqueante curto

            # Lógica de Ingestão no Buffer
            if msg is not None:
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"Erro Kafka: {msg.error()}")
                else:
                    try:
                        # Parsing Rápido
                        data = json.loads(msg.value())
                        
                        # Prepara Tupla para o Buffer (Ordem deve bater com SQL)
                        # (vehicle_id, battery, temp, speed, lon, lat, ts)
                        row = (
                            data.get('vehicle_id') or data.get('car_id'),
                            data['battery_level'],
                            data['temperature'],
                            data['speed_kmh'],
                            data['location']['lon'], # PostGIS espera (Lon, Lat)
                            data['location']['lat'],
                            data['ts_envio']
                        )
                        buffer.append(row)
                        
                    except Exception as e:
                        total_errors += 1
                        # Métrica de Completeness
                        if total_processed > 0 and (total_errors / total_processed) > ERROR_THRESHOLD:
                            print(f"[ALERTA] Taxa de Erro > 1%: {(total_errors/total_processed):.2%}")

            # Lógica de Flush (Tamanho ou Tempo)
            current_time = time.time()
            is_batch_full = len(buffer) >= BATCH_SIZE
            is_timeout = (current_time - last_flush_time) >= FLUSH_TIMEOUT

            if (is_batch_full or is_timeout) and buffer:
                inserted = flush_batch(conn, cursor, buffer)
                
                # Commit do Offset no Kafka APÓS sucesso no Banco (Consistency)
                if inserted > 0:
                    consumer.commit()
                    total_processed += inserted
                
                # Debug de Performance
                # persist_latency = time.time() - buffer[-1][6] # Latência do último item
                # print(f"Flush: {len(buffer)} itens. Latência End-to-End (aprox): {persist_latency:.3f}s")
                
                buffer = []
                last_flush_time = current_time

    except KeyboardInterrupt:
        print("\nEncerrando...")
    finally:
        if buffer:
            flush_batch(conn, cursor, buffer) # Flush final
        consumer.close()
        conn.close()

if __name__ == '__main__':
    main()