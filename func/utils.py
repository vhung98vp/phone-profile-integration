import json
import uuid
from collections import defaultdict
from .config import ES_CONF, THRESHOLDS, ES_PROPERTY, ES_PHONE_PROPERTY, ES_PHONE_MD, MES_FIELD, MES_STRUCT


# BUILD
def build_phone_uid(phone_number, entity_type=ES_CONF['entity_type'], namespace=ES_CONF['uid_namespace']):
    return str(uuid.uuid5(namespace, f"{entity_type}:{phone_number}"))

def build_phone_entity(phone_number, agg_data={}, meta_list=[], property=ES_PROPERTY):
    return {
        property['internal_id']: build_phone_uid(phone_number),
        property['phone_number']: phone_number,
        **agg_data,
        **flat_list(property['metadata'], meta_list)
    }


# MAP AND AGG TO ES FORMAT
def map_agg_meta_list(meta_list):
    metalist = [map_metadata(meta) for meta in meta_list]
    es_metalist = sorted(metalist, key=lambda x: x.get(ES_PHONE_MD["month"], 0), reverse=True)
    agg_data = merge_metadata(es_metalist)
    return agg_data, es_metalist


def merge_metadata(metadata_list, property=ES_PHONE_PROPERTY, meta=ES_PHONE_MD):
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
        total_calls = int(md.get(meta['total_calls'], 0))
        calc_md["total_calls_from"] = float(md.get(meta['call_from_rate'], 0)) * total_calls
        calc_md["total_calls_to"] = total_calls - calc_md["total_calls_from"]
        calc_md["total_duration_from"] = float(md.get(meta['avg_duration_from'], 0)) * calc_md["total_calls_from"]
        calc_md["total_duration_to"] = float(md.get(meta['avg_duration_to'], 0)) * calc_md["total_calls_to"]
        calc_md["total_business_call"] = float(md.get(meta['business_call_rate'], 0)) * total_calls
        for field in fields:
            agg[field] += float(md.get(meta[field], 0))
        for field in calc_fields:
            agg[field] += calc_md[field]

    agg["most_district_from"] = metadata_list[0].get(meta['most_district_from'], '')
    agg["top_5_contacts"], agg["top_5_phone_number"] = merge_contacts(metadata_list, top_n=5)
    
    return {
        property["total_calls"]: agg["total_calls"] / count,
        property["call_from_rate"]: agg["total_calls_from"] / agg["total_calls"],
        property["business_call_rate"]: agg["total_business_call"] / agg["total_calls"],
        property["avg_duration_from"]: agg["total_duration_from"] / agg["total_calls_from"] if agg["total_calls_from"] > 0 else 0,
        property["avg_duration_to"]: agg["total_duration_to"] / agg["total_calls_to"] if agg["total_calls_to"] > 0 else 0,
        property["total_weekend_calls"]: agg["total_weekend_calls"] / count,
        property["total_night_calls"]: agg["total_night_calls"] / count,
        property["total_day_from"]: agg["total_day_from"] / count,
        property["total_day_to"]: agg["total_day_to"] / count,
        property["total_contacts_from"]: agg["total_contacts_from"] / count,
        property["total_contacts_to"]: agg["total_contacts_to"] / count,
        property["most_district_from"]: agg["most_district_from"],
        property["top_5_contacts"]: agg["top_5_contacts"],
        property["top_5_phone_number"]: agg["top_5_phone_number"]
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
def merge_contacts(metadata_list, top_n=5, meta=ES_PHONE_MD):
    contact_map = defaultdict(lambda: {"total_duration": 0, "total_calls": 0})

    for md in metadata_list:
        for phone_number, duration, calls in zip(
            md.get(meta['top_10_phone_number'], []),
            md.get(meta['top_10_total_duration'], []),
            md.get(meta['top_10_total_calls'], [])
        ):
            contact_map[phone_number]["total_duration"] += float(duration)
            contact_map[phone_number]["total_calls"] += float(calls)

    filtered_contacts = {
        phone_number: data
        for phone_number, data in contact_map.items()
        if data["total_duration"] >= THRESHOLDS["top_5_total_duration"]
        and data["total_calls"] >= THRESHOLDS["top_5_total_calls"]
    }

    top_contacts = sorted(
        filtered_contacts.items(),
        key=lambda x: (-x[1]["total_duration"], -x[1]["total_calls"])
    )[:top_n]

    return [
        get_top_contact_str(tc, None)
        for tc in top_contacts
    ], [ phone_number for phone_number, _ in top_contacts ]


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
    if not key:
        return f"{tc[0]} ({int(tc[1]['total_duration'])}s-{int(tc[1]['total_calls'])}c)"
    return f"{tc[key['tc_phone_number']]} ({tc[key['tc_total_duration']]}s-{tc[key['tc_total_calls']]}c)"
