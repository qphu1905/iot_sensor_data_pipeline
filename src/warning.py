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