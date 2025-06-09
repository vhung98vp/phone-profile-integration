import json
import uuid
from collections import defaultdict
from .config import ES_PHONE_MD, ES_PHONE_PROPERTY, MES_FIELD, MES_STRUCT, ES_CONF


def build_phone_uid(phone_number, entity_type=ES_CONF['entity_type'], namespace=ES_CONF['uid_namespace']):
    return str(uuid.uuid5(namespace, f"{entity_type}:{phone_number}"))


### METADATA
def metadata_index(metadata_list, new_meta):
    for idx, md in enumerate(metadata_list):
        if int(md[ES_PHONE_MD['month']]) == int(new_meta[ES_PHONE_MD['month']]):
            return idx
    return -1


def is_metadata_exist(found_meta, new_meta):
    if int(found_meta[ES_PHONE_MD['total_calls']]) == int(new_meta[ES_PHONE_MD['total_calls']]) \
        and float(found_meta[ES_PHONE_MD['call_from_rate']]) == float(new_meta[ES_PHONE_MD['call_from_rate']]):
            return True
    return False


def build_agg_metadata(new_meta):
    result = {}
    for k, v in ES_PHONE_PROPERTY.items():
        if k in ES_PHONE_MD and ES_PHONE_MD[k] in new_meta:
            result[v] = new_meta.get(ES_PHONE_MD[k])
        elif k == "top_5_contacts":
            result[v] = new_meta.get(ES_PHONE_MD["top_10_contacts"], [])[:5]
        elif k == "top_5_phone_number":
            result[v] = new_meta.get(ES_PHONE_MD["top_10_phone_number"], [])[:5]
    return result

def merge_metadata(metadata_list):
    fields = [ 
        "total_calls", "total_weekend_calls", "total_night_calls", 
        "total_day_from", "total_day_to", "total_contacts_from", "total_contacts_to"
    ]
    calc_fields = [ 
        "total_calls_from", "total_calls_to", 
        "total_duration_from", "total_duration_to", "total_business_call" 
    ]

    agg = defaultdict(float)
    count = len(metadata_list)

    for md in metadata_list:
        calc_md = {}
        total_calls = int(md.get(ES_PHONE_MD['total_calls'], 0))
        calc_md["total_calls_from"] = float(md.get(ES_PHONE_MD['call_from_rate'], 0)) * total_calls
        calc_md["total_calls_to"] = total_calls - calc_md["total_calls_from"]
        calc_md["total_duration_from"] = float(md.get(ES_PHONE_MD['avg_duration_from'], 0)) * calc_md["total_calls_from"]
        calc_md["total_duration_to"] = float(md.get(ES_PHONE_MD['avg_duration_to'], 0)) * calc_md["total_calls_to"]
        calc_md["total_business_call"] = float(md.get(ES_PHONE_MD['business_call_rate'], 0)) * total_calls
        for field in fields:
            agg[field] += float(md.get(ES_PHONE_MD[field], 0))
        for field in calc_fields:
            agg[field] += calc_md[field]

    agg["most_district_from"] = get_latest_district(metadata_list)
    agg["top_5_contacts"], agg["top_5_phone_number"] = merge_contacts(metadata_list, top_n=5)
    
    return {
        ES_PHONE_PROPERTY["total_calls"]: agg["total_calls"] / count,
        ES_PHONE_PROPERTY["call_from_rate"]: agg["total_calls_from"] / agg["total_calls"],
        ES_PHONE_PROPERTY["business_call_rate"]: agg["total_business_call"] / agg["total_calls"],
        ES_PHONE_PROPERTY["avg_duration_from"]: agg["total_duration_from"] / agg["total_calls_from"] if agg["total_calls_from"] > 0 else 0,
        ES_PHONE_PROPERTY["avg_duration_to"]: agg["total_duration_to"] / agg["total_calls_to"] if agg["total_calls_to"] > 0 else 0,
        ES_PHONE_PROPERTY["total_weekend_calls"]: agg["total_weekend_calls"] / count,
        ES_PHONE_PROPERTY["total_night_calls"]: agg["total_night_calls"] / count,
        ES_PHONE_PROPERTY["total_day_from"]: agg["total_day_from"] / count,
        ES_PHONE_PROPERTY["total_day_to"]: agg["total_day_to"] / count,
        ES_PHONE_PROPERTY["total_contacts_from"]: agg["total_contacts_from"] / count,
        ES_PHONE_PROPERTY["total_contacts_to"]: agg["total_contacts_to"] / count,
        ES_PHONE_PROPERTY["most_district_from"]: agg["most_district_from"],
        ES_PHONE_PROPERTY["top_5_contacts"]: agg["top_5_contacts"],
        ES_PHONE_PROPERTY["top_5_phone_number"]: agg["top_5_phone_number"]
    }

def map_metadata(new_meta, mes_key=MES_FIELD, mes_st=MES_STRUCT, new_key=ES_PHONE_MD):
    mes_key = {v: k for k, v in mes_key.items()}
    result = {}
    for k, v in new_meta.items():
        nk = mes_key.get(k)
        if nk == 'most_district_from':
            mdf = json.loads(v)
            result[new_key[nk]] = get_most_district_str(mdf)
        elif nk == 'top_10_contacts':
            t10c = [json.loads(tc) for tc in v]
            result[new_key[nk]] = [get_top_contact_str(tc) for tc in t10c]
            result[new_key['top_10_phone_number']] = [tc[mes_st['tc_phone_number']] for tc in t10c]
            result[new_key['top_10_total_duration']] = [tc[mes_st['tc_total_duration']] for tc in t10c]
            result[new_key['top_10_total_calls']] = [tc[mes_st['tc_total_calls']] for tc in t10c]
        elif nk:
            result[new_key[nk]] = v
    return result


## ADDITIONAL UTILS
def merge_contacts(metadata_list, top_n=5):
    # top_contacts = [contact for md in metadata_list for contact in md.get(ES_PHONE_MD['top_10_contacts'], [])]
    # contact_pattern = r'(\d+)\s*\((\d+)s-(\d+)c\)'
    contact_map = defaultdict(lambda: {"total_duration": 0, "total_calls": 0})

    for md in metadata_list:
        for phone_number, duration, calls in zip(
            md.get(ES_PHONE_MD['top_10_phone_number'], []),
            md.get(ES_PHONE_MD['top_10_total_duration'], []),
            md.get(ES_PHONE_MD['top_10_total_calls'], [])
        ):
            contact_map[phone_number]["total_calls"] += float(duration)
            contact_map[phone_number]["total_duration"] += float(calls)

    top_contacts = sorted(
        contact_map.items(),
        key=lambda x: (-x[1]["total_duration"], -x[1]["total_calls"])
    )[:top_n]

    return [
        f"{phone_number} ({data['total_duration']}s-{data['total_calls']}c)"
        for phone_number, data in top_contacts
    ], [ phone_number for phone_number, _ in top_contacts ]

def get_latest_district(metadata_list):
    latest_metadata = max(metadata_list, key=lambda md: int(md.get(ES_PHONE_MD['month'], 0)))
    return latest_metadata.get(ES_PHONE_MD['most_district_from'], '')


def flat_list(prefix, list_item):
    result = {}
    for i, item in enumerate(list_item):
        for key, value in item.items():
            flat_key = f"{prefix}[{i}].{key}"
            result[flat_key] = str(value) if not isinstance(value, list) else value
    return result


def get_most_district_str(mdf, key=MES_STRUCT):
    province = mdf.get(key['mdf_province'], '')
    district = mdf.get(key['mdf_district'], '')
    total_calls = mdf.get(key['mdf_total_calls'], '')
    total_duration = mdf.get(key['mdf_total_duration'], '')
    return f"{province}-{district} ({total_duration}s-{total_calls}c)"

def get_top_contact_str(tc, key=MES_STRUCT):
    return f"{tc[key['tc_phone_number']]} ({tc[key['tc_total_duration']]}s-{tc[key['tc_total_calls']]}c)"
