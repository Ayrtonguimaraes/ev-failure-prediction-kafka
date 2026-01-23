import json
import time
import random
import argparse
import paho.mqtt.client as mqtt

# Frota Simulada
VEHICLE_IDS = [f"EV-{i:03d}" for i in range(1, 21)]

def generate_telemetry():
    return {
        "vehicle_id": random.choice(VEHICLE_IDS),
        "battery_level": round(random.uniform(10.0, 100.0), 2),
        "temperature": round(random.uniform(20.0, 90.0), 1),
        "speed_kmh": round(random.uniform(0, 140), 1),
        "location": {
            "lat": round(random.uniform(-23.50, -23.60), 6),
            "lon": round(random.uniform(-46.60, -46.70), 6)
        },
        # CRÍTICO: O relógio começa a contar AQUI
        "ts_envio": time.time() 
    }

def main(args):
    # Configuração MQTT
    client = mqtt.Client()
    
    try:
        client.connect("localhost", 1883, 60)
        # loop_start cria uma thread em background para gerenciar a rede
        client.loop_start() 
        
        print(f"--- Producer MQTT Iniciado (Delay={args.delay}s) ---")
        print("Enviando dados para localhost:1883...")

        while True:
            data = generate_telemetry()
            
            # Tópico hierárquico: sensores/ev/EV-001
            topic = f"sensores/ev/{data['vehicle_id']}"
            payload = json.dumps(data)
            
            # Publica (QoS 0 para velocidade, QoS 1 para garantia)
            client.publish(topic, payload, qos=0)
            
            print(f"[>] MQTT Enviado: {topic}")
            
            if args.delay > 0:
                time.sleep(args.delay)

    except KeyboardInterrupt:
        print("\nParando producer MQTT...")
        client.loop_stop()
        client.disconnect()
    except Exception as e:
        print(f"Erro de conexão: {e}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--delay', type=float, default=0.1)
    args = parser.parse_args()
    main(args)