import os
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)

KAFKA = {
    'brokers': os.environ.get('KAFKA_BOOTSTRAP_SERVER'),
    'consumer_group': os.environ.get('KAFKA_CONSUMER_GROUP', 'default_consumer_group'),
    'consumer_timeout': float(os.environ.get('KAFKA_CONSUMER_TIMEOUT', 1)),
    'auto_offset_reset': os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest'),
    'input_topic': os.environ.get('KAFKA_INPUT_TOPIC'),
    'output_topic': os.environ.get('KAFKA_OUTPUT_TOPIC'),
    'error_topic': os.environ.get('KAFKA_ERROR_TOPIC')
}

KAFKA_CONSUMER_CONFIG = {
    'bootstrap.servers': KAFKA['brokers'],
    'group.id': KAFKA['consumer_group'],
    'auto.offset.reset': KAFKA['auto_offset_reset']
}

KAFKA_PRODUCER_CONFIG = {
    'bootstrap.servers': KAFKA['brokers']
}

ES = {
    'url': os.environ.get('ES_URL'),
    'user': os.environ.get('ES_USER'),
    'password': os.environ.get('ES_PASSWORD'),
    'phone_index': os.environ.get('ES_PHONE_INDEX', 'phone_index'),
    'property_phone_number': os.environ.get('ES_PROPERTY_PHONE_NUMBER', 'phone_number'),
}

MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 4))
