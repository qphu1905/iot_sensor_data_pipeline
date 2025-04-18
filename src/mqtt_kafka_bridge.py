import sys
#Fix for missing kafka module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import time
import json
import kafka
import paho.mqtt.client as mqtt
from Config import Config
from my_logger import my_logger

#initialize logger
logger = my_logger(__name__)

#topics to subscribe
mqtt_source_topic = 'RAW-DATA'
kafka_producer_topic = 'RAW-DATA'

def mqtt_connect():
    """Connect to MQTT broker
    :parameter: None
    :return: mqtt_client: mqtt.client
    :raises: ConnectionError: Failed connection raises error with reason code
    """

    def on_connect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Connected to MQTT Broker successfully")
        else:
            logger.error(f"Connection failed with result code: {reason_code}.")
            raise ConnectionError(f"Connection failed with result code: {reason_code}.")

    #MQTT broker parameters
    broker = Config.BROKER_ADDRESS
    port = Config.BROKER_PORT

    #MQTT cloud service account
    client_id:str = 'MQTT_KAFKA_BRIDGE'
    username:str = Config.USERNAME
    password:str = Config.PASSWORD

    #create MQTT client and connect
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    mqtt_client.tls_set(Config.CERTIFICATE_FILE)
    mqtt_client.username_pw_set(username=username, password=password)
    mqtt_client.on_connect = on_connect
    mqtt_client.connect(broker, port)
    return mqtt_client


def mqtt_subscribe(mqtt_client):
    """Subscribe to MQTT broker topic
    :parameter: mqtt_client: mqtt.client
    :return: None
    :raises: Exception: failed subscription raises error with reason code
    """

    #action on on_subscribe callback
    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        if reason_code_list[0].is_failure:
            logger.error(f"Broker rejected subscription of topic with reason code: {reason_code_list[0]}.")
            raise Exception(f"Broker rejected subscription of topic with reason code: {reason_code_list[0]}.")
        else:
            logger.info(f"Broker granted QoS: {reason_code_list[0].value} to {mqtt_source_topic}")

    mqtt_client.on_subscribe = on_subscribe
    mqtt_client.subscribe(mqtt_source_topic)
    logger.info(f"Attempting to subscribe to topic")
    time.sleep(1)


def mqtt_message(mqtt_client, kafka_producer):
    """Receive message from MQTT broker and bridge that message to Kafka broker
    :parameter: mqtt_client: mqtt.client
    :parameter: kafka_producer: KafkaProducer
    :return: None
    """

    #produce message to Kafka broker on receiving message from MQTT broker
    def on_message(client, userdata, msg):

        #receive, decode, deserialize message from MQTT broker
        payload = json.loads(msg.payload.decode("utf-8"))
        logger.info(f"Received message: {payload} from topic: {msg.topic}")

        #encode, serialize, send message to Kafka broker
        message = json.dumps(payload).encode("utf-8")
        kafka_producer.send(topic=kafka_producer_topic, value=message)
        kafka_producer.flush()

    mqtt_client.on_message = on_message


def kafka_create_producer(bootstrap_servers:list[str]):
    """Create Kafka producer
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_producer: KafkaProducer
    """

    client_id:str = 'MQTT_KAFKA_BRIDGE'
    kafka_producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers,
                                         client_id=client_id)
    return kafka_producer


def main():
    mqtt_client = mqtt_connect()
    kafka_producer = kafka_create_producer(Config.BOOTSTRAP_SERVERS)
    mqtt_subscribe(mqtt_client)
    mqtt_message(mqtt_client, kafka_producer)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    main()




