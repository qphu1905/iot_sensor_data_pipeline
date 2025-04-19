import sys
#fix for missing kafka-python module
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

import kafka
import json
from Config import Config
from my_logger import my_logger


#initialize logger
logger = my_logger(__name__)

#Topics to subscribe
kafka_consumer_topic = 'TRANSFORMED-DATA'


def warning(apparent_temperature: float) -> int:
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



