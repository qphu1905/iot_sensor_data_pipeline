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
from datetime import date, time, timedelta


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


def filter_daily(df: pd.DataFrame, date_id, groupby) -> pd.DataFrame:
    mask = df['date_id'] == date_id
    print(df[mask])
    df = df[mask].dropna().reset_index(drop=True)
    print(df)
    if groupby == 'minute':
        df = df[['temperature', 'feels_like_temperature', 'humidity', 'hour', 'minute']]
        df = df.groupby(['hour', 'minute']).mean().dropna().reset_index().sort_values(by=['hour', 'minute'])
        #set custom index for charting
        df['index'] = df.apply(lambda row: f'{row.hour}:{row.minute}', axis=1)
        df.set_index('index', inplace=True)
        df = df[['temperature', 'feels_like_temperature', 'humidity']]
    elif groupby == 'hour':
        df = df[['temperature', 'feels_like_temperature', 'humidity', 'hour']]
        df = df.groupby(['hour']).mean().dropna().sort_values(by=['hour'])
    else:
        #implement throwing wrong parameter error here
        pass
    return df


def filter_period(df: pd.DataFrame, start_date_id, end_date_id, groupby) -> pd.DataFrame:
    mask = df['date_id'].between(start_date_id, end_date_id)
    df[mask].dropna().reset_index(drop=True)
    if groupby == 'day':
        df = df[['temperature', 'feels_like_temperature', 'humidity', 'year', 'month', 'day']]
        df = df.groupby(['year', 'month', 'day']).mean().dropna().reset_index().sort_values(by=['year', 'month', 'day'])
        #set custom index for charting
        df['index'] = df.apply(lambda row: f'{row.year}-{row.month}-{row.day}', axis=1)
        df.set_index('index', inplace=True)
        df = df[['temperature', 'feels_like_temperature', 'humidity']]
    elif groupby == 'month':
        df = df[['temperature', 'feels_like_temperature', 'humidity', 'year', 'month']]
        df = df.groupby(['year', 'month']).mean().dropna().reset_index().sort_values(by=['year', 'month'])
        #set custom index for charting
        df['index'] = df.apply(lambda row: f'{row.year}-{row.month}', axis=1)
        df.set_index('index', inplace=True)
        df = df[['temperature', 'feels_like_temperature', 'humidity']]
    elif groupby == 'year':
        df = df[['temperature', 'feels_like_temperature', 'humidity', 'year']]
        df = df.groupby(['year']).mean().dropna().reset_index().sort_values(by=['year'])
    else:
        #implement throwing wrong parameter error here
        pass
    return df


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
        location_data = location_data.loc[location_data['location_name'] == location_name].reset_index()
        location_latitude = float(location_data.at[0, 'latitude'])
        location_longtitude = float(location_data.at[0, 'longtitude'])
        st.header(location_name)

        col1, col2 = st.columns(2)
        with col1:
            st.metric('Latitude', location_latitude, border=3)
        with col2:
            st.metric('Longtitude', location_longtitude, border=3)

    location_weather_data_df = load_weather_data(db_engine, location_name)

    weather_data_container = st.container()
    with weather_data_container:
        col1, col2, col3 = st.columns(3)

        temperature = float(location_weather_data_df['temperature'].iloc[-1])
        feels_like_temperature = float(location_weather_data_df['feels_like_temperature'].iloc[-1])
        humidity = int(location_weather_data_df['humidity'].iloc[-1])

        with col1:
            st.metric('Temperature', f'{temperature}°C', border=3)
        with col2:
            st.metric('Feels Like', f'{feels_like_temperature}°C', border=3)
        with col3:
            st.metric('Humidity', f'{humidity}%', border=3)

    daily_metric_graph_container = st.container()
    with daily_metric_graph_container:
        col1, col2 = st.columns(2)

        with col1:
            daily_metric_date_id = st.date_input(label='Select date:', value='today')
        with col2:
            daily_metric_groupby = st.selectbox(label='Group by:', options=['hour', 'minute'], format_func=lambda x: x.capitalize())

        filtered_daily_data_df = filter_daily(location_weather_data_df, daily_metric_date_id, daily_metric_groupby)

        col4, col5, col6 = st.columns(3)

        with col4:
            show_temperature = st.checkbox(label='Show temperature', value=True, key='show_temperature_daily')
        with col5:
            show_feels_like = st.checkbox(label='Show feels like temperature', value=True, key='show_feels_like_daily')
        with col6:
            show_humidity = st.checkbox(label='Show humidity', value=True, key='show_humidity_daily')
        if not show_temperature:
            filtered_daily_data_df.drop(['temperature'], axis=1, inplace=True)
        if not show_feels_like:
            filtered_daily_data_df.drop(['feels_like_temperature'], axis=1, inplace=True)
        if not show_humidity:
            filtered_daily_data_df.drop(['humidity'], axis=1, inplace=True)

        filtered_daily_data_df.rename(columns=lambda x: x.capitalize(), inplace=True)
        st.line_chart(filtered_daily_data_df)

    period_metric_graph_container = st.container()
    with period_metric_graph_container:
        col1, col2 = st.columns(2)

        with col1:
            period_metric_period = st.date_input(label='Select period:', value=(date.today() - timedelta(days=30), date.today()))
            period_start_date = period_metric_period[0]
            period_end_date = period_metric_period[1]
        with col2:
            period_metric_groupby = st.selectbox(label='Group by:', options=['year', 'month', 'day'], format_func=lambda x: x.capitalize(), index=2)

        filtered_period_data_df = filter_period(location_weather_data_df, period_start_date, period_end_date, period_metric_groupby)

        col4, col5, col6 = st.columns(3)

        with col4:
            show_temperature = st.checkbox(label='Show temperature', value=True, key='show_temperature_period')
        with col5:
            show_feels_like = st.checkbox(label='Show feels like temperature', value=True, key='show_feels_like_period')
        with col6:
            show_humidity = st.checkbox(label='Show humidity', value=True, key='show_humidity_period')
        if not show_temperature:
            filtered_period_data_df.drop(['temperature'], axis=1, inplace=True)
        if not show_feels_like:
            filtered_period_data_df.drop(['feels_like_temperature'], axis=1, inplace=True)
        if not show_humidity:
            filtered_period_data_df.drop(['humidity'], axis=1, inplace=True)

        filtered_period_data_df.rename(columns=lambda x: x.capitalize(), inplace=True)
        st.line_chart(filtered_period_data_df)
main()