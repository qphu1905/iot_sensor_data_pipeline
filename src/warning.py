import sys
#fix for missing kafka-python module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import kafka
import json
from Config import Config
from my_logger import my_logger


#initialize logger
logger = my_logger(__name__)

#Topics to subscribe
kafka_consumer_topic = 'TRANSFORMED-DATA'


def warning(apparent_temperature: float) -> int:
    if apparent_temperature <= 27:
        warning_level = 0
    elif 27.0 < apparent_temperature <= 32.0:
        warning_level = 1
    elif 32.0 < apparent_temperature <= 39.0:
        warning_level = 2
    elif 39.0 < apparent_temperature <= 51.0:
        warning_level = 3
    elif 51.0 < apparent_temperature <= 58.0:
        warning_level = 4
    else:
        warning_level = 5
    return warning_level


def create_kafka_consumer(bootstrap_servers: list[str]) -> kafka.KafkaConsumer:
    """Create Kafka consumer, consumer deserialize message from Kafka broker
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_consumer: KafkaConsumer
    """

    kafka_consumer = kafka.KafkaConsumer(kafka_consumer_topic,
                                         bootstrap_servers=bootstrap_servers,
                                         client_id='WARNING-CONSUMER',
                                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    logger.info('Kafka consumer created!')
    return kafka_consumer


def create_kafka_producer(bootstrap_servers: list[str]) -> kafka.KafkaProducer:
    """Create Kafka producer
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_producer: KafkaProducer
    """

    kafka_producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers,
                                         client_id='WARNING-PRODUCER')
    logger.info('Kafka producer created!')
    return kafka_producer




