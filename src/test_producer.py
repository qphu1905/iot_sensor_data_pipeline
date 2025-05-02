import random
import sys
import time

#Fix for missing kafka module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import json
import kafka
from dotenv import dotenv_values, find_dotenv

from my_logger import my_logger

dev_config = dotenv_values(find_dotenv(".env/env.dev"))
KAFKA_BROKER_ADDRESS = json.loads(dev_config['KAFKA_BROKER_ADDRESS'])

kafka_producer_topic = 'RAW-DATA'

#initialize logger
logger = my_logger(__name__)


def kafka_create_producer(bootstrap_servers: list[str]) -> kafka.KafkaProducer:
    """Create Kafka producer
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_producer: KafkaProducer
    """

    kafka_producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers,
                                         client_id='TRANSFORM-PRODUCER')
    logger.info('Kafka producer created!')
    return kafka_producer

kafka_producer = kafka_create_producer(KAFKA_BROKER_ADDRESS)

locations = ['F', 'M', 'MZ']
while True:
    location = locations[random.randint(0, len(locations) - 1)]
    temperature = round(random.uniform(0, 60), 2)
    humidity = random.randint(1, 99)
    msg = json.dumps({'location': location, 'temperature': temperature, 'humidity': humidity}).encode('utf-8')
    kafka_producer.send(kafka_producer_topic, msg)
    print(msg)
    kafka_producer.flush()
    time.sleep(60)


