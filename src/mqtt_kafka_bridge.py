import sys
import time
import json
import kafka
import paho.mqtt.client as mqtt
from Config import Config
from my_logger import my_logger

#Initialize logger
logger = my_logger(__name__)

def mqtt_connect():
    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Connected to MQTT Broker successfully")
        else:
            logger.error(f"Connection failed with result code: {reason_code}.")
            sys.exit(1)

    broker = Config.BROKER_ADDRESS
    port = Config.BROKER_PORT
    client_id:str = 'MQTT_KAFKA_BRIDGE'
    username:str = Config.USERNAME
    password:str = Config.PASSWORD
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=client_id)
    client.username_pw_set(username=username, password=password)
    client.tls_set(Config.CERTIFICATE_FILE)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def kafka_create_producer(bootstrap_servers:list[str]):
    client_id:str = 'MQTT_KAFKA_BRIDGE'
    producer = kafka.KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id)
    return producer


def mqtt_subscribe(client, TOPICS:list[str]):
    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        if reason_code_list[0].is_failure:
            logger.error(f"Broker rejected subscription of {topic} with reason code: {reason_code_list[0]}.")
            sys.exit(1)
        else:
            logger.info(f"Broker granted QoS: {reason_code_list[0].value}")
    client.on_subscribe = on_subscribe
    for topic in TOPICS:
        client.subscribe(topic)
        logger.info(f"Attempting to subscribe to topic: {topic}")
        time.sleep(1)


def mqtt_message(mqtt_client, kafka_producer):
    def on_message(client, userdata, msg):
        payload = json.loads(msg.payload.decode("utf-8"))
        logger.info(f"Received message: {payload} from topic: {msg.topic}")
        message = json.dumps(payload).encode("utf-8")
        kafka_producer.send(topic=f"{msg.topic}", value=message)
        kafka_producer.flush()
    mqtt_client.on_message = on_message


def main():
    mqtt_client = mqtt_connect()
    kafka_producer = kafka_create_producer(Config.BOOTSTRAP_SERVERS)
    time.sleep(3)
    mqtt_subscribe(mqtt_client, Config.TOPICS)
    mqtt_message(mqtt_client, kafka_producer)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    main()




