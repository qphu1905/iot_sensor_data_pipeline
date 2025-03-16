import kafka
import json
import pandas as pd
import datetime
from Config import Config


def calculate_apparent_temperature(temp: float, rh: int) -> float:
    c1: float = -8.78469475556
    c2: float = 1.61139411
    c3: float = 2.33854883889
    c4: float = -0.14611605
    c5: float = -0.012308094
    c6: float = -0.0164248277778
    c7: float = 2.211732 * 10**(-3)
    c8: float = 7.2546 * 10**(-4)
    c9: float = -3.582 * 10**(-6)
    if temp < 27:
        return temp
    elif temp > 66:
        return temp
    else:
        apparent_temp: float = (c1 + c2 * temp + c3 * rh
                                + c4 * temp * rh + c5 * (temp**2)+ c6 * (rh**2)
                                + c7 * (temp**2) * rh + c8 * temp * (rh**2) + c9 * (temp**2) * (rh**2))
        return apparent_temp

# NEED TO FIX TRANSFORM
def transform(df: pd.DataFrame) -> pd.DataFrame:
    time: datetime = datetime.datetime.now()
    temp: float = df['temp']
    rh: int = df['rh']
    apparent_temp: float = calculate_apparent_temperature(temp, rh)
    df.insert(loc=0, column='time', value=time)
    df.insert(loc=-1, column='apparent_temp', value=apparent_temp)
    return df


def kafka_consumer(bootstrap_servers: list[str]):
    consumer = kafka.KafkaConsumer('RAW_DATA',
                                   bootstrap_servers=bootstrap_servers,
                                   client_id='TRANSFORM',
                                   value_deserializer=lambda m: json.loads(m.decode('utf-8'))
                                   )
    return consumer


def kafka_producer(bootstrap_servers: list[str]):
    producer = kafka.KafkaProducer(bootstrap_servers=bootstrap_servers,
                                   client_id='TRANSFORM',
                                   )
    return producer


def main():
    consumer = kafka_consumer(Config.BOOTSTRAP_SERVERS)
    producer = kafka_producer(Config.BOOTSTRAP_SERVERS)
    # NEED TO FIX TRANSFORM DATAFRAME NOT COMPATIBLE
    for msg in consumer:
        print(msg)
        print(msg.value)
        print(msg.value.items())
        df = pd.json_normalize(msg.value)
        df = transform(df)
        msg = df.to_json().encode('utf-8')
        producer.send(topic='RAW_DATA', value=msg)


if __name__ == '__main__':
    main()