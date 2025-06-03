import requests
import re
from datetime import datetime
from .config import logger, ES, ES_PROPERTY, ES_PHONE_PROPERTY, ES_PHONE_MD
from .utils import build_phone_uid


def query_elasticsearch(phone_number):
    """Fetch phone metadata document from Elasticsearch by phone number."""
    url = f"{ES['url']}/{ES['phone_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "match": {
                # f"properties.{ES_PROPERTY['phone_number_search']}": phone_number
                ES_PROPERTY['phone_id']: build_phone_uid(phone_number)
            }
        }
    }

    try:
        response = requests.get(url=url,
                                     headers=headers,
                                     auth=auth,
                                     json=query)
        response.raise_for_status()
        response_hits = response.json()['hits']['hits']
        return transform_properties(response_hits[0]['_source']['properties']) if response_hits else None
    except Exception as e:
        logger.exception(f"Failed to fetch from Elasticsearch: {e}")
        return None

def transform_properties(properties): 
    """Transform properties from Elasticsearch response to a dictionary with array of metadata."""
    if not properties:
        return None

    normal_dict = {}
    metadata_list = []
    date_pattern = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{9}Z$"
    es_arr_fields = [v for d in [ES_PHONE_PROPERTY, ES_PHONE_MD] 
                     for k, v in d.items() if re.match(r"top_.*", k)]

    for key, value in properties.items():
        if isinstance(value, list) and len(value) == 1\
            and not any(item in key for item in es_arr_fields):
            value = value[0]
        if isinstance(value, str) and re.match(date_pattern, value):
            try: # Convert date string to milliseconds since epoch
                value = int(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1000)
            except ValueError:
                logger.warning(f"Invalid date format for key {key}: {value}")

        if ES_PROPERTY['suffix_pattern']:
            key = re.sub(ES_PROPERTY['suffix_pattern'], '', key)

        match_key = re.match(rf"{ES_PROPERTY['metadata']}\[(\d+)\]\.(.+)", key)
        if match_key:
            index = int(match_key.group(1))
            field = match_key.group(2)
            while len(metadata_list) <= index:
                metadata_list.append({})
            metadata_list[index][field] = value
        else:
            normal_dict[key] = value

    if metadata_list:
        normal_dict['metadata'] = metadata_list
    return normal_dict
