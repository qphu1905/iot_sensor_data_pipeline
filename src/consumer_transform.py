import sys
#fix for missing kafka-python module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import datetime
import json

import kafka
from dotenv import dotenv_values, find_dotenv

from my_logger import my_logger

#load environment variables
dev_config = dotenv_values(find_dotenv(".env/env.dev"))
KAFKA_BROKER_ADDRESS = json.loads(dev_config['KAFKA_BROKER_ADDRESS'])

#initialize logger
logger = my_logger(__name__)

#Topics to subscribe
kafka_consumer_topic = 'RAW-DATA'
kafka_producer_topic = 'TRANSFORMED-DATA'


def calculate_apparent_temperature(temperature: float, humidity: float) -> float:
    """Calculate apparent temperature based on temperature and humidity
    :param temperature: float: temperature in degrees Celsius
    :param humidity: float: humidity in percent (0.00-1.00)
    :return: apparent_temperature: apparent temperature in degrees Celsius
    """

    #constants for calculation
    c1: float = -8.78469475556
    c2: float = 1.61139411
    c3: float = 2.33854883889
    c4: float = -0.14611605
    c5: float = -0.012308094
    c6: float = -0.0164248277778
    c7: float = 2.211732 * 10 ** (-3)
    c8: float = 7.2546 * 10 ** (-4)
    c9: float = -3.582 * 10 ** (-6)

    if humidity < 40:
        return temperature
    #lower limit of heat index
    else:
        if temperature < 27:
            return temperature
        #upper limit of heat index
        elif temperature > 66:
            return temperature
        else:
            apparent_temperature: float = (c1 + c2 * temperature + c3 * humidity
                                    + c4 * temperature * humidity + c5 * (temperature**2)+ c6 * (humidity**2)
                                    + c7 * (temperature**2) * humidity + c8 * temperature * (humidity**2) + c9 * (temperature**2) * (humidity**2))
            return apparent_temperature


def calculate_warning_level(apparent_temperature: float) -> int:
    """Calculate warning level for given apparent temperature.
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


def transform(message: dict) -> dict:
    """Transform and enrich raw data from sensor
     :parameter: message: dict: raw data from sensor
     :return: message: dict: transformed data
     """

    #calculate apparent temperature
    temperature = message['temperature']
    humidity = message['humidity']
    feels_like_temperature = calculate_apparent_temperature(temperature, humidity)
    warning = calculate_warning_level(feels_like_temperature)
    #calculate timestamp
    time_id = datetime.datetime.now().strftime('%H:%M:00')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')

    #transform message
    message['feels_like_temperature'] = feels_like_temperature
    message['warning_level'] = warning
    message['time_id'] = time_id
    message['date_id'] = date_id
    return message


def kafka_create_consumer(bootstrap_servers: list[str]) -> kafka.KafkaConsumer:
    """Create Kafka consumer, consumer deserialize message from Kafka broker
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_consumer: KafkaConsumer
    """

    kafka_consumer = kafka.KafkaConsumer(kafka_consumer_topic,
                                         bootstrap_servers=bootstrap_servers,
                                         client_id='TRANSFORM-CONSUMER',
                                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    logger.info('Kafka consumer created!')
    return kafka_consumer


def kafka_create_producer(bootstrap_servers: list[str]) -> kafka.KafkaProducer:
    """Create Kafka producer
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_producer: KafkaProducer
    """

    kafka_producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers,
                                         client_id='TRANSFORM-PRODUCER')
    logger.info('Kafka producer created!')
    return kafka_producer


def main():
    #create kafka consumer
    kafka_consumer = kafka_create_consumer(KAFKA_BROKER_ADDRESS)
    #create kafka producer
    kafka_producer = kafka_create_producer(KAFKA_BROKER_ADDRESS)

    for message in kafka_consumer:
        #transform, encode, serialize, then send message
        transformed_message = json.dumps(transform(message.value)).encode('utf-8')
        kafka_producer.send(topic=kafka_producer_topic, value=transformed_message)
        logger.info('Transformed message sent!')

if __name__ == '__main__':
    main()
