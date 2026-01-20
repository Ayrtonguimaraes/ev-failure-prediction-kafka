import json
import time
import random
import argparse
import socket
from confluent_kafka import Producer

# Configuração simulada de frota
VEHICLE_IDS = [f"EV-{i:03d}" for i in range(1, 21)] # 20 Veículos

def delivery_report(err, msg):
    """Callback chamado quando o Kafka confirma o recebimento (ack)."""
    if err is not None:
        print(f'Falha na entrega: {err}')
    else:
        # ADICIONADO PARA DEBUG: Confirmação visual
        print(f"[>] Enviado: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")

def generate_telemetry():
    """Gera payload JSON simulando sensor IoT."""
    return {
        "vehicle_id": random.choice(VEHICLE_IDS),
        "battery_level": round(random.uniform(10.0, 100.0), 2),
        "temperature": round(random.uniform(20.0, 90.0), 1),
        "speed_kmh": round(random.uniform(0, 140), 1),
        "location": {
            "lat": round(random.uniform(-23.50, -23.60), 6),
            "lon": round(random.uniform(-46.60, -46.70), 6)
        },
        "ts_envio": time.time() # CRÍTICO: Timestamp de origem para cálculo de latência
    }

def main(args):
    # Configuração do Producer otimizada para testes de Trade-off
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': socket.gethostname(),
        'linger.ms': args.linger_ms,    # Controla o atraso artificial para formar lotes
        'batch.size': args.batch_size,  # Tamanho máximo do lote em bytes
        'acks': args.acks,              # 0=Rápido/Perigoso, 1=Líder, all=Lento/Seguro
        'compression.type': 'snappy'    # Compressão padrão em IoT
    }

    producer = Producer(conf)
    topic = "telemetria_ev"

    print(f"--- Iniciando Producer IoT ---")
    print(f"Parâmetros: Linger={args.linger_ms}ms | Batch={args.batch_size}bytes | Acks={args.acks} | Delay={args.delay}s")
    print("Pressione Ctrl+C para parar.")

    try:
        while True:
            data = generate_telemetry()
            
            producer.produce(
                topic, 
                key=data['vehicle_id'], # Garante ordem por veículo
                value=json.dumps(data), 
                callback=delivery_report
            )
            
            # poll(0) serve para disparar os callbacks de confirmação
            producer.poll(0)
            
            # Controle de taxa de envio (Simula frequência do sensor)
            if args.delay > 0:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nParando producer...")
    finally:
        # Garante que mensagens em buffer sejam enviadas antes de fechar
        producer.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Simulador de Sensores EV IoT")
    
    # Argumentos para testar o Triângulo de Lazidis
    parser.add_argument('--linger_ms', type=int, default=5, help="Tempo (ms) para agrupar mensagens (Aumenta vazão, Piora latência)")
    parser.add_argument('--batch_size', type=int, default=16384, help="Tamanho máx do batch em bytes")
    parser.add_argument('--acks', type=str, default='1', help="Nível de consistência (0, 1, all)")
    parser.add_argument('--delay', type=float, default=0.001, help="Intervalo entre mensagens (segundos). Use 0 para stress test.")
    
    args = parser.parse_args()
    main(args)