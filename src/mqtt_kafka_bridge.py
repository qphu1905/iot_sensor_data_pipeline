import sys
#Fix for missing kafka module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import json

import kafka
import paho.mqtt.client as mqtt
from dotenv import dotenv_values, find_dotenv

from my_logger import my_logger

#load environment variables
dev_config = dotenv_values(find_dotenv(".env/.env.dev"))
secret_config = dotenv_values(find_dotenv(".env/.env.secret"))

MQTT_BROKER_ADDRESS = dev_config['MQTT_BROKER_ADDRESS']
MQTT_BROKER_PORT = int(dev_config['MQTT_BROKER_PORT'])
MQTT_BROKER_USERNAME = secret_config['MQTT_BROKER_USERNAME']
MQTT_BROKER_PASSWORD = secret_config['MQTT_BROKER_PASSWORD']
MQTT_BROKER_CERT = secret_config['MQTT_BROKER_CERT']

KAFKA_BROKER_ADDRESS = json.loads(dev_config['KAFKA_BROKER_ADDRESS'])

#initialize logger
logger = my_logger(__name__)

#topics to subscribe
mqtt_source_topic = 'RAW-DATA'
kafka_producer_topic = 'RAW-DATA'


def mqtt_connect() -> mqtt.Client:
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
    broker_address = MQTT_BROKER_ADDRESS
    broker_port = MQTT_BROKER_PORT

    #MQTT cloud service account
    client_id:str = 'MQTT_KAFKA_BRIDGE'
    username:str = MQTT_BROKER_USERNAME
    password:str = MQTT_BROKER_PASSWORD

    #create MQTT client and connect
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=client_id)
    mqtt_client.tls_set(MQTT_BROKER_CERT)
    mqtt_client.username_pw_set(username=username, password=password)
    mqtt_client.on_connect = on_connect
    mqtt_client.connect(broker_address, broker_port)
    return mqtt_client


def mqtt_subscribe(mqtt_client: mqtt.Client) -> None:
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


def mqtt_message(mqtt_client: mqtt.Client, kafka_producer: kafka.KafkaProducer) -> None:
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


def kafka_create_producer(bootstrap_servers: list[str]) -> kafka.KafkaProducer:
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
    kafka_producer = kafka_create_producer(KAFKA_BROKER_ADDRESS)
    mqtt_subscribe(mqtt_client)
    mqtt_message(mqtt_client, kafka_producer)
    mqtt_client.loop_forever()

if __name__ == '__main__':
    main()
