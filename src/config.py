import os
import logging
import json
from uuid import UUID
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
}

ES_CONF = {
    'uid_namespace': UUID(os.environ.get('ES_UUID_NAMESPACE')),
    'entity_type': os.environ.get('ES_ENTITY_TYPE'),
    'add_top_entity': bool(os.environ.get('ADD_TOP_PHONE_ENTITY', 0))
}

THRESHOLDS = {
    'top_5_total_duration': int(os.environ.get('THRESHOLD_TOP_5_TOTAL_DURATION', 15)),
    'top_5_total_calls': int(os.environ.get('THRESHOLD_TOP_5_TOTAL_CALLS', 0))
}


with open("config.json", "r") as f:
    config = json.load(f)
    ES_PROPERTY = config.get('es_properties', {})
    ES_PHONE_PROPERTY = config.get('es_phone_properties', {})
    ES_PHONE_MD = config.get('es_phone_metadata', {})
    MES_FIELD = config.get('message_fields', {})
    MES_STRUCT = config.get('message_struct_fields', {})

if not KAFKA['brokers']:
    raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is not set. Please set it to the Kafka brokers address.")
# if not ES['url']:
#     raise ValueError("ES_URL environment variable is not set. Please set it to the Elasticsearch URL.")