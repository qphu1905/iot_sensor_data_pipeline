import sys
#Fix for missing kafka module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import kafka
import sqlalchemy as db
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
import json
from dotenv import dotenv_values, find_dotenv
from datetime import date, time, datetime


dev_config = dotenv_values(find_dotenv(".env/.env.dev"))
secret_config = dotenv_values(find_dotenv(".env/.env.secret"))

POSTGRES_SERVER_ADDRESS = dev_config['POSTGRES_SERVER_ADDRESS']
POSTGRES_USERNAME = secret_config['POSTGRES_USERNAME']
POSTGRES_PASSWORD = secret_config['POSTGRES_PASSWORD']

KAFKA_BROKER_ADDRESS = json.loads(dev_config['KAFKA_BROKER_ADDRESS'])

kafka_consumer_topic = 'WARNING'


def create_database_engine() -> db.engine.Engine:
    """Create postgresql database engine
    :parameter: None
    :return: db_engine: sqlalchemy.engine.Engine
    """

    db_engine = db.create_engine(f'postgresql+psycopg2://{POSTGRES_USERNAME}:%s@{POSTGRES_SERVER_ADDRESS}/iot_sensor_data' % quote_plus(POSTGRES_PASSWORD))
    return db_engine


def kafka_create_consumer(bootstrap_servers: list[str]) -> kafka.KafkaConsumer:
    """Create Kafka consumer, consumer deserialize message from Kafka broker
    :parameter: bootstrap_servers: list[str]: list of Kafka broker adresses
    :return: kafka_consumer: KafkaConsumer
    """

    kafka_consumer = kafka.KafkaConsumer(kafka_consumer_topic,
                                         bootstrap_servers=bootstrap_servers,
                                         client_id='TRANSFORM-CONSUMER',
                                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
    return kafka_consumer


def load_data_daily(location, date, start_time, end_time, db_engine):
    metadata = db.MetaData()
    weather_data = db.Table('weather_data', metadata, autoload_with=db_engine)
    location_dimension = db.Table('location_dimension', metadata, autoload_with=db_engine)
    time_dimension = db.Table('time_dimension', metadata, autoload_with=db_engine)
    date_dimension = db.Table('date_dimension', metadata, autoload_with=db_engine)
    j = (weather_data
         .join(location_dimension, weather_data.c.location_id == location_dimension.c.location_id)
         .join(time_dimension, weather_data.c.time_id == time_dimension.c.time_id)
         )
    with db_engine.begin() as connection:
        stmt = (db.select(location_dimension.c.location_name, weather_data.c.temperature, weather_data.c.time_id, weather_data.c.date_id)
                .select_from(j)
                .where(location_dimension.c.location_name == location)
                .where(weather_data.c.date_id == date)
                .where(db.between(weather_data.c.time_id, start_time, end_time))
                )
        result = connection.execute(stmt)
    df = pd.DataFrame(result)
    print(df)
    df = df.groupby('time_id')['temperature'].mean()
    print(df)
    return df

def load_data_period(location, start_date, end_date, db_engine):
    metadata = db.MetaData()
    weather_data = db.Table('weather_data', metadata, autoload_with=db_engine)
    location_dimension = db.Table('location_dimension', metadata, autoload_with=db_engine)
    time_dimension = db.Table('time_dimension', metadata, autoload_with=db_engine)
    date_dimension = db.Table('date_dimension', metadata, autoload_with=db_engine)
    j = (weather_data
         .join(location_dimension, weather_data.c.location_id == location_dimension.c.location_id)
         .join(date_dimension, weather_data.c.date_id == date_dimension.c.date_id)
         )
    with db_engine.begin() as connection:
        stmt = (db.select(location_dimension.c.location_name, weather_data.c.temperature, weather_data.c.time_id, weather_data.c.date_id)
                .select_from(j)
                .where(location_dimension.c.location_name == location)
                .where(db.between(weather_data.c.date_id, start_date, end_date))
                )
        result = connection.execute(stmt)
    df = pd.DataFrame(result)
    print(df)
    df = df.groupby('date_id')['temperature'].mean()
    print(df)
    return df

db_engine = create_database_engine()
load_data_daily('Mainz', date.fromisoformat('2025-04-21'), time.fromisoformat('21:15:00'), time.fromisoformat('21:30:00'), db_engine)
load_data_period('Mainz', date.fromisoformat('2025-04-21'), date.fromisoformat('2025-04-22'), db_engine)
# st.title('Dashboard')
# city = st.selectbox('Choose city', ['Frankfurt', 'Munchen'], placeholder='Choose city', index=None)
# date_range = st.checkbox('Date Range')
# if date_range:
#     start_date = st.date_input('Start Date')
#     end_date = st.date_input("End_date")
# else:
#     date = st.date_input('Date')
# col1, col2, col3 = st.columns(3, border=3)
# with col1:
#     col1.metric('TEMPERATURE', 20)
# with col2:
#     col2.metric('PRESSURE', 10)
# with col3:
#     col3.metric('WIND', 10)
# row1 = st.container()
# with row1:
#     row1.metric('Warning', 10, border=3)