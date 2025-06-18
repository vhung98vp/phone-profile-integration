import os
import logging
import json
from uuid import UUID
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
logger = logging.getLogger(__name__)


ES_CONF = {
    'uid_namespace': UUID(os.environ.get('ES_UUID_NAMESPACE')),
    'entity_type': os.environ.get('ES_ENTITY_TYPE')
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