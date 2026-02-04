import time
import json
import random
import paho.mqtt.client as mqtt

# --- CONFIGURAÇÕES DO EXPERIMENTO ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
TOPIC_PREFIX = "sensores/ev"

NUM_VEICULOS = 5        # Começamos com 5 carros
DELAY_ENTRE_ENVIOS = 0.1 # 0.1s = 10 mensagens por segundo (aprox)

# --- FUNÇÃO GERADORA DE DADOS (SIMULAÇÃO FÍSICA) ---
def gerar_telemetria(veiculo_id):
    """
    Gera um pacote de dados simulando um carro em movimento.
    """
    return {
        "vehicle_id": f"carro-{veiculo_id}",
        "velocidade": random.randint(0, 120),  # km/h
        "bateria": random.randint(10, 100),    # %
        "temperatura_motor": random.uniform(60.0, 110.0), # °C
        "latitude": -23.55 + random.uniform(-0.01, 0.01), # Simula SP
        "longitude": -46.63 + random.uniform(-0.01, 0.01),
        # CARIMBO DE ORIGEM (Rastreabilidade Nível 1)
        "ts_sensor": int(time.time() * 1000) 
    }

# --- CALLBACKS MQTT ---
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print(f"✅ Simulador conectado ao Broker! Preparando para enviar...")
    else:
        print(f"❌ Falha na conexão. Código: {rc}")

# --- MAIN LOOP ---
if __name__ == '__main__':
    client = mqtt.Client()
    client.on_connect = on_connect

    print(f"🏎️ Iniciando Simulador de Estresse: {NUM_VEICULOS} veículos...")
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.loop_start() # Inicia thread de rede em background

        while True:
            # Para cada veículo simulado...
            for i in range(NUM_VEICULOS):
                # 1. Gera o dado
                dados = gerar_telemetria(i)
                payload = json.dumps(dados)
                
                # 2. Define o tópico específico do carro
                topico = f"{TOPIC_PREFIX}/{dados['vehicle_id']}"
                
                # 3. Publica no MQTT (Fire and Forget)
                client.publish(topico, payload)
                
                # (Opcional) Print leve apenas para saber que está vivo
                # print(f"📤 Enviado: {dados['vehicle_id']}")

            # Controle de Frequência
            time.sleep(DELAY_ENTRE_ENVIOS)

    except KeyboardInterrupt:
        print("\n🛑 Parando simulação...")
        client.loop_stop()
        client.disconnect()