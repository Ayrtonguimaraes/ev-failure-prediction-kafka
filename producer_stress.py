import time
import json
import random
import csv
import sys
import os
import paho.mqtt.client as mqtt

# --- CONFIGURAÇÃO DO TESTE ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC_PREFIX = "sensores/ev/" 

# Configuração da Rampa
START_RATE = 50   # Começa em 50 msg/s
END_RATE = 500    # Termina em 500 msg/s
DURATION_SEC = 300 # 5 Minutos (Soak Test)
LOG_FILENAME = "resultado_mqtt_entrada.csv"

# Configuração de Suavização (Pacing)
# Divide o envio de 1 segundo em X fatias para evitar "micro-bursts"
BATCHES_PER_SEC = 10 

def get_payload(vehicle_id):
    return {
        "vehicle_id": vehicle_id,
        "battery_level": round(random.uniform(50.0, 100.0), 2),
        "temperature": round(random.uniform(20.0, 90.0), 2),
        "speed_kmh": round(random.uniform(0.0, 120.0), 2),
        "location": {"lat": -23.55, "lon": -46.63},
        "ts_envio": time.time() 
    }

def main():
    print(f"--- Iniciando Stress Test via MQTT (Rampa Suavizada) ---")
    print(f"Meta: {START_RATE} -> {END_RATE} msg/s em {DURATION_SEC} segundos")
    
    # Setup MQTT
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, "python_stress_loader")
    except AttributeError:
        client = mqtt.Client("python_stress_loader")

    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_start()
        print(f"Conectado ao Mosquitto em {MQTT_BROKER}:{MQTT_PORT}")
    except Exception as e:
        print(f"Erro ao conectar no MQTT: {e}")
        return

    # Prepara CSV
    # buffering=1 ajuda, mas o fsync é quem garante
    csvfile = open(LOG_FILENAME, 'w', newline='', buffering=1)
    logger = csv.writer(csvfile)
    logger.writerow(['timestamp_segundo', 'target_rate', 'sent_count'])

    start_time = time.time()
    total_sent = 0
    
    try:
        # Loop segundo a segundo
        for elapsed in range(DURATION_SEC):
            loop_start = time.time()
            
            # 1. Calcular a Taxa Alvo para este segundo
            progress = elapsed / DURATION_SEC
            current_target = int(START_RATE + (END_RATE - START_RATE) * progress)
            
            # --- LÓGICA DE PACING (SUAVIZAÇÃO) ---
            # Em vez de um loop único gigante, quebramos em mini-loops
            msgs_per_batch = current_target // BATCHES_PER_SEC
            remainder = current_target % BATCHES_PER_SEC
            
            msgs_sent_this_sec = 0

            for b in range(BATCHES_PER_SEC):
                batch_start = time.time()
                
                # Adiciona o resto da divisão no último lote para a conta fechar exata
                count = msgs_per_batch + (remainder if b == BATCHES_PER_SEC - 1 else 0)
                
                # Envia o mini-lote
                for i in range(count):
                    # ID único rotativo para simular frota
                    vid = f"EV-STRESS-{(total_sent + msgs_sent_this_sec + i) % 50}" 
                    topic = f"{MQTT_TOPIC_PREFIX}{vid}"
                    payload = get_payload(vid)
                    client.publish(topic, json.dumps(payload), qos=0)
                
                msgs_sent_this_sec += count
                
                # Dorme proporcionalmente (1/10 de segundo - tempo gasto)
                slice_duration = time.time() - batch_start
                sleep_time = max(0, (1.0 / BATCHES_PER_SEC) - slice_duration)
                time.sleep(sleep_time)

            total_sent += msgs_sent_this_sec
            
            # 3. Registrar no CSV e FORÇAR DISCO
            logger.writerow([int(time.time()), current_target, msgs_sent_this_sec])
            csvfile.flush()
            os.fsync(csvfile.fileno())
            
            # Log visual a cada 10s
            if elapsed % 10 == 0: 
                print(f"T={elapsed}s | Alvo: {current_target} | Real: {msgs_sent_this_sec}")
                
            # Correção fina de tempo global (opcional, mas bom para precisão)
            # Como já dormimos nos mini-batches, aqui é só para alinhar o clock se atrasou muito
            global_duration = time.time() - loop_start
            if global_duration < 1.0:
                time.sleep(1.0 - global_duration)

    except KeyboardInterrupt:
        print("\nTeste interrompido!")
    finally:
        csvfile.close()
        client.loop_stop()
        client.disconnect()

    print(f"\n--- FIM DO TESTE ---")
    print(f"Total Enviado via MQTT: {total_sent}")

if __name__ == "__main__":
    main()