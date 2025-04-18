import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import kafka
import json
import datetime
from Config import Config


def calculate_apparent_temperature(temperature: float, humidity: float) -> float:
    c1: float = -8.78469475556
    c2: float = 1.61139411
    c3: float = 2.33854883889
    c4: float = -0.14611605
    c5: float = -0.012308094
    c6: float = -0.0164248277778
    c7: float = 2.211732 * 10**(-3)
    c8: float = 7.2546 * 10**(-4)
    c9: float = -3.582 * 10**(-6)
    if temperature < 27:
        return temperature
    elif temperature > 66:
        return temperature
    else:
        apparent_temperature: float = (c1 + c2 * temperature + c3 * humidity
                                + c4 * temperature * humidity + c5 * (temperature**2)+ c6 * (humidity**2)
                                + c7 * (temperature**2) * humidity + c8 * temperature * (humidity**2) + c9 * (temperature**2) * (humidity**2))
        return apparent_temperature

# NEED TO FIX TRANSFORM
def transform(msg: dict) -> dict:
    temperature = msg['temperature']
    humidity = msg['humidity']
    feels_like_temperature = calculate_apparent_temperature(temperature, humidity)
    time_id = datetime.datetime.now().strftime('%H:%M:%S')
    date_id = datetime.datetime.now().strftime('%Y-%m-%d')
    msg['feels_like_temperature'] = feels_like_temperature
    msg['time_id'] = time_id
    msg['date_id'] = date_id
    print(msg)
    return msg


def kafka_consumer(bootstrap_servers: list[str]):
    consumer = kafka.KafkaConsumer('RAW-DATA',
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
        transformed_msg = json.dumps(transform(msg.value)).encode('utf-8')
        producer.send(topic='TRANSFORMED-DATA', value=transformed_msg)


if __name__ == '__main__':
    main()