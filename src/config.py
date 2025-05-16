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
    'suffix_pattern': os.environ.get('ES_PROPERTY_SUFFIX_PATTERN', ''),
}

ES_PROPERTY = {
    'phone_number_search': os.environ.get('ES_PROPERTY_PHONE_NUMBER_SEARCH', 'phone_number_s'),
    'internal_id': os.environ.get('ES_PROPERTY_INTERNAL_ID', 'fs_internal_id'),
    'phone_number': os.environ.get('ES_PROPERTY_PHONE_NUMBER', 'phone_number'),
    'metadata': os.environ.get('ES_PROPERTY_METADATA', 'metadata'),
    'total_calls': os.environ.get('ES_PROPERTY_TOTAL_CALLS', 'total_calls'),
    'call_from_rate': os.environ.get('ES_PROPERTY_CALL_FROM_RATE', 'call_from_rate'),
    'business_call_rate': os.environ.get('ES_PROPERTY_BUSINESS_CALL_RATE', 'business_call_rate'),
    'avg_duration_from': os.environ.get('ES_PROPERTY_AVG_DURATION_FROM', 'avg_duration_from'),
    'avg_duration_to': os.environ.get('ES_PROPERTY_AVG_DURATION_TO', 'avg_duration_to'),
    'total_weekend_calls': os.environ.get('ES_PROPERTY_TOTAL_WEEKEND_CALLS', 'total_weekend_calls'),
    'total_night_calls': os.environ.get('ES_PROPERTY_TOTAL_NIGHT_CALLS', 'total_night_calls'),
    'total_day_from': os.environ.get('ES_PROPERTY_TOTAL_DAY_FROM', 'total_day_from'),
    'total_contacts': os.environ.get('ES_PROPERTY_TOTAL_CONTACTS', 'total_contacts'),
    'most_district_from': os.environ.get('ES_PROPERTY_MOST_DISTRICT_FROM', 'most_district_from'),
    'top_5_contacts': os.environ.get('ES_PROPERTY_TOP_5_CONTACTS', 'top_5_contacts'),
}

ES_PROPERTY_MD = {
    'month': os.environ.get('ES_PROPERTY_METADATA_MONTH', 'month'),
    'total_calls': os.environ.get('ES_PROPERTY_METADATA_TOTAL_CALLS', 'total_calls'),
    'call_from_rate': os.environ.get('ES_PROPERTY_METADATA_CALL_FROM_RATE', 'call_from_rate'),
    'business_call_rate': os.environ.get('ES_PROPERTY_METADATA_BUSINESS_CALL_RATE', 'business_call_rate'),
    'avg_duration_from': os.environ.get('ES_PROPERTY_METADATA_AVG_DURATION_FROM', 'avg_duration_from'),
    'avg_duration_to': os.environ.get('ES_PROPERTY_METADATA_AVG_DURATION_TO', 'avg_duration_to'),
    'total_weekend_calls': os.environ.get('ES_PROPERTY_METADATA_TOTAL_WEEKEND_CALLS', 'total_weekend_calls'),
    'total_night_calls': os.environ.get('ES_PROPERTY_METADATA_TOTAL_NIGHT_CALLS', 'total_night_calls'),
    'total_day_from': os.environ.get('ES_PROPERTY_METADATA_TOTAL_DAY_FROM', 'total_day_from'),
    'total_contacts': os.environ.get('ES_PROPERTY_METADATA_TOTAL_CONTACTS', 'total_contacts'),
    'most_district_from': os.environ.get('ES_PROPERTY_METADATA_MOST_DISTRICT_FROM', 'most_district_from'),
    'top_5_contacts': os.environ.get('ES_PROPERTY_METADATA_TOP_5_CONTACTS', 'top_5_contacts'),
    'top_10_contacts': os.environ.get('ES_PROPERTY_METADATA_TOP_10_CONTACTS', 'top_10_contacts'),
}

MAX_WORKERS = int(os.environ.get('MAX_WORKERS', 4))
