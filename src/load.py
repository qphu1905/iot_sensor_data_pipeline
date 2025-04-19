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


def kafka_create_consumer(bootstrap_servers: list[str]) -> kafka.KafkaConsumer:
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


def create_database_engine() -> db.engine.Engine:
    """Create postgresql database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine('postgresql+psycopg2://qphu1905:%s@192.168.1.210:5432/iot_sensor_data' % quote_plus('Quocphu!@#123'))
    logger.info('Database engine created!')
    return db_engine


def create_entry(message: dict) -> dict:
    """Create entry to be inserted in database
    :parameter: message: dict
    :return: entry: dict
    """

    entry = {
        "location_id": message['location'],
        "temperature": message['temperature'],
        "humidity": message['humidity'],
        "date_id": message['date_id'],
        "time_id": message['time_id']
    }
    return entry


def main():
    kafka_consumer = kafka_create_consumer(Config.BOOTSTRAP_SERVERS)
    db_engine = create_database_engine()
    db_metadata = db.MetaData()
    weather_data = db.Table('weather_data', db_metadata, autoload_with=db_engine)

    #buffer to avoid individual insert statements
    buffer = []

    for message in kafka_consumer:
        message_value = message.value
        entry = create_entry(message_value)
        buffer.append(entry)

        if len(buffer) >= 100:
            with db_engine.begin() as conn:
                transaction = weather_data.insert()
                conn.execute(transaction, buffer)
                logger.info(f'{len(buffer)} entries inserted!')
            #clear buffer after writing to database
            buffer = []

if __name__ == '__main__':
    main()
