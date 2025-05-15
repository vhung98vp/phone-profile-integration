import requests
import re
from datetime import datetime
from .config import logger, ES, ES_PROPERTY


def query_elasticsearch(phone_number):
    """Fetch phone metadata document from Elasticsearch by phone number."""
    url = f"{ES['url']}/{ES['phone_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "match": {
                f"properties.{ES_PROPERTY['phone_number']}": phone_number
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
    for key, value in properties.items():
        if isinstance(value, list) and len(value) == 1:
            value = value[0]
        if isinstance(value, str) and re.match(date_pattern, value):
            try:
                value = str(datetime.fromisoformat(value.replace('Z', '+00:00')).timestamp() * 1000)
            except ValueError:
                logger.warning(f"Invalid date format for key {key}: {value}")

        if ES['suffix_pattern']:
            sub_key = re.sub(ES['suffix_pattern'], '', key)

        match_key = re.match(f"{ES_PROPERTY['metadata']}\[(\d+)\]\.(.+)", sub_key)
        if match_key:
            index = int(match_key.group(1))
            field = match_key.group(2)
            while len(metadata_list) <= index:
                metadata_list.append({})
            metadata_list[index][field] = value
        else:
            normal_dict[sub_key] = value

    if metadata_list:
        normal_dict['metadata'] = metadata_list
    return normal_dict
