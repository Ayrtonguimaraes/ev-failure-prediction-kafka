import time
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# --- CONFIGURAÇÕES ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensores/ev/+" 

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "telemetria_ev"

conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'python-mqtt-bridge',
    'linger.ms': 0,
    'compression.type': 'snappy'
}
producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'❌ Erro Kafka: {err}')

def on_connect(client, userdata, flags, rc):
    print(f"🔌 Ponte conectada ao Mosquitto (Código: {rc})")
    client.subscribe(MQTT_TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    
    # --- RASTREABILIDADE TOTAL ---
    # Capturamos o tempo exato que a mensagem chegou na ponte
    # Multiplicamos por 1000 para ter em milissegundos (igual ao Java/JS)
    ts_chegada_ponte = int(time.time() * 1000)
    
    # Criamos os Headers (Metadados)
    # Kafka Headers esperam valores em bytes
    headers = [
        ('trace_bridge_ts', str(ts_chegada_ponte).encode('utf-8')),
        ('origem_mqtt_topic', msg.topic.encode('utf-8'))
    ]

    try:
        producer.produce(
            topic=KAFKA_TOPIC,
            key=msg.topic, 
            value=payload,
            headers=headers,  # <--- INJEÇÃO DOS HEADERS AQUI
            callback=delivery_report
        )
        producer.poll(0)
        
    except BufferError:
        print(f"⚠️ Kafka cheio! Freando...")
        time.sleep(1)
        producer.poll(1)

if __name__ == '__main__':
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    print(f"🚀 Ponte com Rastreabilidade Iniciada...")
    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        print("\n🛑 Parando...")
        producer.flush()
        mqtt_client.disconnect()