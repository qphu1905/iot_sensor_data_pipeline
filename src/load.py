import sys
#fix for kafka-python missing module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from urllib.parse import quote_plus
import kafka
import sqlalchemy as db
import json

from Config import Config
from my_logger import my_logger


#initialize logger
logger = my_logger(__name__)

#Topics to subscribe
kafka_consumer_topic = 'TRANSFORMED-DATA'


def kafka_create_consumer(bootstrap_servers: list[str]):
    """Create Kafka consumer, consumer deserialize message from Kafka broker
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_consumer: KafkaConsumer
    """

    kafka_consumer = kafka.KafkaConsumer(kafka_consumer_topic,
                                         bootstrap_servers=bootstrap_servers,
                                         client_id='LOAD-CONSUMER',
                                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    logger.info('Kafka consumer created!')
    return kafka_consumer


def create_database_engine():
    """Create postgresql database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine('postgresql+psycopg2://qphu1905:%s@192.168.1.210:5432/iot_sensor_data' % quote_plus('Quocphu!@#123'))
    logger.info('Database engine created!')
    return db_engine


def create_entry(message: dict) -> dict:
    entry = {
        "location_id": message['location'],
        "temperature": message['temperature'],
        "humidity": message['humidity'],
        "date_id": message['date_id'],
        "time_id": message['time_id']
    }
    return entry