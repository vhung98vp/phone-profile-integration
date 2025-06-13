from .utils import build_phone_uid, flat_list
from .config import ES_PHONE_MD,  ES_PROPERTY
from .elasticsearch import query_elasticsearch


def build_phone_entity(phone_number, agg_data={}, meta_list=[]):
    return {
        ES_PROPERTY['internal_id']: build_phone_uid(phone_number),
        ES_PROPERTY['phone_number']: phone_number,
        **agg_data,
        **flat_list(ES_PROPERTY['metadata'], meta_list)
    }

def build_top_phone_entities(agg_data):
    top_phones = agg_data.get(ES_PROPERTY["top_5_phone_number"], [])
    entities = []
    for phone_num in top_phones:
        es_record = query_elasticsearch(phone_num)
        if not es_record:
            entities.append(build_phone_entity(phone_num))
    return entities
