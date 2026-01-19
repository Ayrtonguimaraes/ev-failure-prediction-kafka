import json
import os
import sys
import psycopg2
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv

# Carrega variáveis de ambiente (Segurança e Praticidade)
load_dotenv()

# Configuração Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'ev-database-group', # PERFEITO: Grupo separado para persistência
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

# Configuração Banco (Alinhado com docker-compose)
DB_CONFIG = {
    "host": "localhost",
    "database": os.getenv("POSTGRES_DB", "db_telemetria"),
    "user": os.getenv("POSTGRES_USER", "user_ev"),
    "password": os.getenv("POSTGRES_PASSWORD", "pass_ev")
}

def main():
    consumer = Consumer(kafka_config)
    consumer.subscribe(['telemetria_ev']) # Confirme se o tópico no Producer é esse mesmo

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        # OTIMIZAÇÃO: Autocommit evita o overhead de transação por linha
        # Essencial para testes de alta vazão
        conn.autocommit = True 
        cursor = conn.cursor()
        print("--- Persistência no Banco Iniciada (Modo Autocommit) ---")

    except Exception as e:
        print(f"Erro ao conectar no banco: {e}")
        sys.exit(1)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            
            # Tratamento de Erro do Kafka (Obrigatório em confluent-kafka)
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Erro no Kafka: {msg.error()}")
                    continue

            try:
                dados = json.loads(msg.value().decode('utf-8'))

                # Query preparada para PostGIS (Geo-localização)
                # Assume que seu JSON vem com 'location': {'lat': -23.5, 'lon': -46.6}
                query = """
                    INSERT INTO telemetria_ev 
                    (vehicle_id, battery_level, temperature, speed_kmh, location, ts_envio) 
                    VALUES (%s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326), to_timestamp(%s))
                """
                
                # Atenção ao mapeamento das chaves do seu JSON
                cursor.execute(query, (
                    dados.get('vehicle_id') or dados.get('car_id'), # Fallback se mudar o nome
                    dados['battery_level'], # No seu script estava 'bateria', padronize com o producer
                    dados['temperature'],   # No seu script estava 'temperatura'
                    dados['speed_kmh'],     # No seu script estava 'velocidade'
                    dados['location']['lon'], # PostGIS usa (Longitude, Latitude)
                    dados['location']['lat'],
                    dados['ts_envio']
                ))
                
            except KeyError as e:
                print(f"Erro de formato no JSON (chave faltando): {e}")
            except Exception as e:
                print(f"Erro na inserção: {e}")

    except KeyboardInterrupt:
        print("Encerrando consumidor...")
    finally:
        consumer.close()
        if cursor: cursor.close()
        if conn: conn.close()

if __name__ == '__main__':
    main()