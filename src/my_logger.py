import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def my_logger(name):
    logger = logging.getLogger(name)
    return logger
