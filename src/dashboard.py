import sys
#Fix for missing kafka module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import streamlit as st
import sqlalchemy as db
from urllib.parse import quote_plus
import pandas as pd
import numpy as np
import json
from dotenv import dotenv_values, find_dotenv
from datetime import date, time, datetime


dev_config = dotenv_values(find_dotenv(".env/env.dev"))
secret_config = dotenv_values(find_dotenv(".env/env.secret"))

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


def load_weather_data(db_engine, location_name):
    metadata = db.MetaData()
    weather_data = db.Table('weather_data', metadata, autoload_with=db_engine)
    location_dimension = db.Table('location_dimension', metadata, autoload_with=db_engine)
    time_dimension = db.Table('time_dimension', metadata, autoload_with=db_engine)
    date_dimension = db.Table('date_dimension', metadata, autoload_with=db_engine)
    warning_dimension = db.Table('warning_dimension', metadata, autoload_with=db_engine)
    j = (weather_data
         .join(location_dimension, weather_data.c.location_id == location_dimension.c.location_id)
         .join(time_dimension, weather_data.c.time_id == time_dimension.c.time_id)
         .join(date_dimension, weather_data.c.date_id == date_dimension.c.date_id)
         .join(warning_dimension, weather_data.c.warning_level == warning_dimension.c.warning_level)
         )
    with db_engine.begin() as connection:
        stmt = (
            db.select(
                weather_data.c.temperature, weather_data.c.humidity, weather_data.c.feels_like_temperature,
                location_dimension.c.location_name, location_dimension.c.latitude, location_dimension.c.longtitude,
                date_dimension.c.date_id, date_dimension.c.year, date_dimension.c.month, date_dimension.c.day, date_dimension.c.month_name, date_dimension.c.day_name,
                time_dimension.c.time_id, time_dimension.c.hour, time_dimension.c.minute,
                warning_dimension.c.warning_level, warning_dimension.c.warning_text
            )
            .select_from(j)
            .where(location_dimension.c.location_name == location_name)
            )
        result = connection.execute(stmt)
    df = pd.DataFrame(result)
    print(df)
    return df


def load_location_data(db_engine):
    metadata = db.MetaData()
    location_dimension = db.Table('location_dimension', metadata, autoload_with=db_engine)
    with db_engine.begin() as connection:
        stmt = (
            db.select(
                location_dimension.c.location_name,
                location_dimension.c.latitude,
                location_dimension.c.longtitude)
            .select_from(location_dimension)
        )
        result = connection.execute(stmt)
    result = pd.DataFrame(result)
    return result


def filter_daily(df: pd.DataFrame, location_name='Mainz', date_id=date(2025, 4, 25)) -> pd.DataFrame:
    print(df['location_name'])
    mask = df['date_id'] == date_id
    df = df[mask].dropna().reset_index(drop=True)

def main():
    db_engine = create_database_engine()

    st.title('Dashboard')

    location_data_container = st.container(border=3)

    with location_data_container:
        location_data = load_location_data(db_engine)
        location_name_list = location_data['location_name']
        location_name = st.selectbox(label='Select city:', options=location_name_list)
        if location_name is None:
            location_name = 'Frankfurt'
        location_data = location_data.loc[location_data['location_name'] == location_name].reset_index(drop=True)
        location_latitude = float(location_data.at[0, 'latitude'])
        location_longtitude = float(location_data.at[0, 'longtitude'])

        st.text('Location')
        st.header(location_name)
        col1, col2 = st.columns(2)
        with col1:
            st.metric('Latitude', location_latitude, border=3)
        with col2:
            st.metric('Longtitude', location_longtitude, border=3)

    location_weather_data = load_weather_data(db_engine, location_name)

    weather_data_container = st.container()
    with weather_data_container:
        col1, col2, col3 = st.columns(3)

        temperature = float(location_weather_data['temperature'].iloc[-1])
        feels_like_temperature = float(location_weather_data['feels_like_temperature'].iloc[-1])
        humidity = int(location_weather_data['humidity'].iloc[-1])

        with col1:
            st.metric('Temperature', temperature, border=3)
        with col2:
            st.metric('Feels Like', feels_like_temperature, border=3)
        with col3:
            st.metric('Humidity', f'{humidity}%', border=3)

main()
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