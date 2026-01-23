import paho.mqtt.client as mqtt
from confluent_kafka import Producer
import json

MQTT_BROKER = "localhost"
MQTT_TOPIC = "sensores/ev/+" 
KAFKA_TOPIC = "telemetria_ev"

# --- Tuning do Kafka Producer (Mostefaoui et al.) ---
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'bridge_v4',
    'compression.type': 'snappy',  # Vital para performance
    'linger.ms': 5,                # Baixa latência (evita jitter)
    'batch.size': 32768,           # 32KB
    'acks': '1'                    # Leader ack (rápido e seguro o suficiente para PoC)
}
producer = Producer(kafka_conf)

def on_message(client, userdata, msg):
    try:
        # Pass-through direto (Byte Array) para máxima performance
        producer.produce(
            KAFKA_TOPIC, 
            key=msg.topic, # Garante ordenação por veículo (Partitioning Key)
            value=msg.payload,
            on_delivery=lambda err, msg: print(f"Erro Kafka: {err}") if err else None
        )
        # Poll(0) serve para disparar callbacks de rede do Kafka
        producer.poll(0)
    except Exception as e:
        print(f"Erro Bridge: {e}")

if __name__ == '__main__':
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883)
    client.subscribe(MQTT_TOPIC)
    
    print(f"--- Bridge MQTT->Kafka Ativa (Snappy Enabled) ---")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        producer.flush()
        client.disconnect()