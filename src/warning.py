import sys
#fix for missing kafka-python module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import kafka
import json
from dotenv import dotenv_values, find_dotenv

from my_logger import my_logger

#load environment variables
dev_config = dotenv_values(find_dotenv(".env/.env.dev"))
KAFKA_BROKER_ADDRESS = json.loads(dev_config['KAFKA_BROKER_ADDRESS'])

#initialize logger
logger = my_logger(__name__)

#Topics to subscribe
kafka_consumer_topic = 'TRANSFORMED-DATA'
kafka_producer_topic = 'WARNING'


def warning(apparent_temperature: float) -> int:
    """Calculate warning level for give apparent temperature.
    :parameter: apparent_temperature: float
    :return: warning_level: int
    """

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


def main():
    #create kafka consumer
    kafka_consumer = create_kafka_consumer(bootstrap_servers=KAFKA_BROKER_ADDRESS)
    #create kafka producer
    kafka_producer = create_kafka_producer(bootstrap_servers=KAFKA_BROKER_ADDRESS)

    for message in kafka_consumer:
        #get shaded apparent temperature
        apparent_temperature_shaded = message.value['apparent_temperature']
        #get direct sunshine apparent temperature
        apparent_temperature_sunshine = apparent_temperature_shaded + 8

        #calculate warning level
        warning_level_shaded = warning(apparent_temperature_shaded)
        warning_level_sunshine = warning(apparent_temperature_sunshine)

        #create and serialize message
        warnings = {
                    'location': message.value['location'],
                    'warning_level_shaded': warning_level_shaded,
                    'warning_level_sunshine': warning_level_sunshine
                    }
        warnings_message = json.dumps(warnings).encode('utf-8')

        #send message
        kafka_producer.send(kafka_producer_topic, value=warnings_message)


if __name__ == '__main__':
    main()
