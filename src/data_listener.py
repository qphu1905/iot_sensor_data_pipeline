import sys
import time
import keyboard
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
    client_id:str = 'DATA_LISTENER'
    username:str = Config.USERNAME
    password:str = Config.PASSWORD
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,client_id=client_id)
    client.username_pw_set(username=username, password=password)
    client.tls_set(Config.CERTIFICATE_FILE)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def mqtt_disconnect(client):
    def on_disconnect(client, userdata, flags, reason_code, properties):
        if reason_code == 0:
            logger.info("Disconnected from MQTT Broker successfully!")
            sys.exit(0)
        else:
            logger.error(f"Problem with connection: {reason_code}.")
            logger.infor(f"Attempting to reconnect")
            client.reconnect()


    client.on_disconnect = on_disconnect
    client.loop_stop()
    client.disconnect()


def subscribe(client, TOPICS:list[str]):
    def on_subscribe(client, userdata, mid, reason_code_list, properties):
        if reason_code_list[0].is_failure:
            logger.error(f"Broker rejected subscription of {topic} with reason code: {reason_code_list[0]}.")
            sys.exit(0)
        else:
            logger.info(f"Broker granted QoS: {reason_code_list[0].value}")


    def on_message(client, userdata, msg):
        print(f'Received message: {msg.payload.decode()} from topic: {msg.topic}')


    client.on_message = on_message
    client.on_subscribe = on_subscribe
    for topic in TOPICS:
        client.subscribe(topic)
        logger.info(f"Attempting to subscribe to topic: {topic}")
        time.sleep(3)


def main():
    mqtt_client = mqtt_connect()
    time.sleep(3)
    subscribe(mqtt_client, Config.TOPICS)
    mqtt_client.loop_forever()
    while True:
        if keyboard.read_key().lower() == 'q':
            mqtt_disconnect(mqtt_client)


if __name__ == '__main__':
    main()




