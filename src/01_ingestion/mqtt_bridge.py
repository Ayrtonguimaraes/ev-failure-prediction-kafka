"""Bridge MQTT -> Kafka com tratamento de backpressure e particionamento por veículo."""
import sys
import time
import paho.mqtt.client as mqtt
from confluent_kafka import Producer

# --- Configuração ---
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "sensores/ev/+"
KAFKA_TOPIC = "telemetria_ev"

# Tuning do Kafka Producer (Mostefaoui et al.)
kafka_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'mqtt_kafka_bridge',
    'compression.type': 'snappy',
    'linger.ms': 5,
    'batch.size': 32768,
    'acks': '1',
    'queue.buffering.max.messages': 100000,
    'queue.buffering.max.kbytes': 1048576,
}

producer = Producer(kafka_conf)

# Contadores para monitoramento
_stats = {'sent': 0, 'errors': 0, 'retries': 0}


def delivery_report(err, msg):
    """Callback de confirmação de entrega ao Kafka."""
    if err is not None:
        _stats['errors'] += 1
        print(f"[ERRO] Falha na entrega: {err}", file=sys.stderr)


def extract_vehicle_id(topic: str) -> str:
    """Extrai vehicle_id do tópico MQTT (sensores/ev/{vehicle_id})."""
    try:
        return topic.split('/')[-1]
    except Exception:
        return topic


def on_message(client, userdata, msg):
    """Callback para cada mensagem MQTT recebida."""
    topic_str = msg.topic if isinstance(msg.topic, str) else msg.topic.decode('utf-8')
    vehicle_id = extract_vehicle_id(topic_str)
    
    max_retries = 5
    for attempt in range(1, max_retries + 1):
        try:
            producer.produce(
                KAFKA_TOPIC,
                key=vehicle_id,
                value=msg.payload,
                on_delivery=delivery_report
            )
            producer.poll(0)
            _stats['sent'] += 1
            return
        except BufferError:
            _stats['retries'] += 1
            wait = min(1.0, 0.1 * (2 ** (attempt - 1)))
            print(f"[AVISO] Buffer cheio (tentativa {attempt}/{max_retries}). Aguardando {wait:.2f}s...")
            producer.poll(wait)
        except Exception as e:
            _stats['errors'] += 1
            print(f"[ERRO] Bridge: {e}", file=sys.stderr)
            return
    
    print(f"[ERRO] Descartando mensagem após {max_retries} tentativas.", file=sys.stderr)
    _stats['errors'] += 1


def on_connect(client, userdata, flags, rc, properties=None):
    """Callback de conexão MQTT."""
    if rc == 0:
        print(f"[INFO] Conectado ao MQTT broker {MQTT_BROKER}:{MQTT_PORT}")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"[ERRO] Falha na conexão MQTT: código {rc}", file=sys.stderr)


def main():
    # Setup MQTT client
    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt_kafka_bridge")
    except AttributeError:
        client = mqtt.Client(client_id="mqtt_kafka_bridge")
    
    client.on_connect = on_connect
    client.on_message = on_message
    
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar ao MQTT: {e}", file=sys.stderr)
        sys.exit(1)
    
    print("--- Bridge MQTT->Kafka Iniciada (Snappy, BufferError Handling) ---")
    
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        print("\n[INFO] Encerrando bridge...")
    finally:
        print(f"[INFO] Estatísticas: Enviadas={_stats['sent']}, Erros={_stats['errors']}, Retries={_stats['retries']}")
        producer.flush(timeout=10)
        client.disconnect()
        print("[INFO] Bridge encerrada.")


if __name__ == '__main__':
    main()