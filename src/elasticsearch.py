import requests
from .config import logger, ES


def query_elasticsearch(phone_number):
    """Fetch phone metadata document from Elasticsearch by phone number."""
    url = f"{ES['url']}/{ES['phone_index']}/_search"
    auth = (ES['user'], ES['password']) if ES['user'] and ES['password'] else None
    headers = {'Content-Type': 'application/json'}
    query = {
        "query": {
            "match": {
                "properties.phone_number": phone_number
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
        return response_hits[0]['_source'] if response_hits else None
    except Exception as e:
        logger.exception(f"Failed to fetch from Elasticsearch: {e}")
        return None

