import sys
#fix for kafka-python missing module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from urllib.parse import quote_plus
import kafka
import sqlalchemy as db
import json
from dotenv import dotenv_values, find_dotenv

from my_logger import my_logger


#load environment variables
dev_config = dotenv_values(find_dotenv(".env/.env.dev"))
secret_config = dotenv_values(find_dotenv(".env/.env.secret"))

POSTGRES_SERVER_ADDRESS = dev_config['POSTGRES_SERVER_ADDRESS']
POSTGRES_USERNAME = secret_config['POSTGRES_USERNAME']
POSTGRES_PASSWORD = secret_config['POSTGRES_PASSWORD']

KAFKA_BROKER_ADDRESS = json.loads(dev_config['KAFKA_BROKER_ADDRESS'])

#initialize logger
logger = my_logger(__name__)

#Topics to subscribe
kafka_consumer_topic = 'TRANSFORMED-DATA'


def create_entry(message: dict) -> dict:
    """Create entry to be inserted in database
    :parameter: message: dict
    :return: entry: dict
    """

    entry = {
        "location_id": message['location'],
        "temperature": message['temperature'],
        "humidity": message['humidity'],
        "feels_like_temperature": message['feels_like_temperature'],
        "date_id": message['date_id'],
        "time_id": message['time_id']
    }
    return entry


def kafka_create_consumer(bootstrap_servers: list[str]) -> kafka.KafkaConsumer:
    """Create Kafka consumer, consumer deserialize message from Kafka broker
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_consumer: KafkaConsumer
    """

    kafka_consumer = kafka.KafkaConsumer(kafka_consumer_topic,
                                         bootstrap_servers=bootstrap_servers,
                                         client_id='LOAD-CONSUMER',
                                         value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                                         consumer_timeout_ms=300000)
    logger.info('Kafka consumer created!')
    return kafka_consumer


def create_database_engine() -> db.engine.Engine:
    """Create postgresql database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'postgresql+psycopg2://{POSTGRES_USERNAME}:%s@{POSTGRES_SERVER_ADDRESS}/iot_sensor_data' % quote_plus(POSTGRES_PASSWORD))
    logger.info('Database engine created!')
    return db_engine


def main():
    kafka_consumer = kafka_create_consumer(KAFKA_BROKER_ADDRESS)
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

    #write straggle messages to database
    if len(buffer) > 0:
        with db_engine.begin() as conn:
            transaction = weather_data.insert()
            conn.execute(transaction, buffer)
            logger.info(f'{len(buffer)} straggle entries inserted!')
        #clear buffer after writing to database
        buffer = []

if __name__ == '__main__':
    while True:
        main()
