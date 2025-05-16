import re
from collections import defaultdict
from .config import ES_PHONE_MD, ES_PHONE_PROPERTY, MES_FIELD


def build_agg_metadata(new_meta):
    return {
        v: new_meta[ES_PHONE_MD[k]] 
        for k, v in ES_PHONE_PROPERTY.items()
    }

def merge_metadata(metadata_list):
    fields = [ 
        "total_calls", "total_weekend_calls", "total_night_calls", 
        "total_day_from", "total_contacts"
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
            agg[field] += int(md.get(ES_PHONE_MD[field], 0))
        for field in calc_fields:
            agg[field] += calc_md[field]

    agg["most_district_from"] = get_latest_district(metadata_list)
    agg["top_5_contacts"] = merge_contacts(metadata_list, top_n=5)
    
    return {
        ES_PHONE_PROPERTY["total_calls"]: agg["total_calls"] / count,
        ES_PHONE_PROPERTY["call_from_rate"]: agg["total_calls_from"] / agg["total_calls"],
        ES_PHONE_PROPERTY["business_call_rate"]: agg["total_business_call"] / agg["total_calls"],
        ES_PHONE_PROPERTY["avg_duration_from"]: agg["total_duration_from"] / agg["total_calls_from"],
        ES_PHONE_PROPERTY["avg_duration_to"]: agg["total_duration_to"] / agg["total_calls_to"],
        ES_PHONE_PROPERTY["total_weekend_calls"]: agg["total_weekend_calls"] / count,
        ES_PHONE_PROPERTY["total_night_calls"]: agg["total_night_calls"] / count,
        ES_PHONE_PROPERTY["total_day_from"]: agg["total_day_from"] / count,
        ES_PHONE_PROPERTY["total_contacts"]: agg["total_contacts"] / count,
        ES_PHONE_PROPERTY["most_district_from"]: agg["most_district_from"],
        ES_PHONE_PROPERTY["top_5_contacts"]: agg["top_5_contacts"]
    }

def merge_contacts(metadata_list, top_n=5):
    top_contacts = [contact for md in metadata_list for contact in md.get(ES_PHONE_MD['top_10_contacts'], [])]
    contact_pattern = r'(\d+)\s*\((\d+)s-(\d+)c\)'
    contact_map = defaultdict(lambda: {"total_duration": 0, "total_calls": 0})

    for item in top_contacts:
        match = re.search(contact_pattern, item)
        phone_number, total_duration, total_calls = match.groups()
        contact_map[phone_number]["total_calls"] += int(total_calls)
        contact_map[phone_number]["total_duration"] += int(total_duration)

    sorted_contacts = sorted(
        contact_map.items(),
        key=lambda x: (-x[1]["total_duration"], -x[1]["total_calls"])
    )

    return [
        f"{phone_number} ({data['total_duration']}s-{data['total_calls']}c)"
        for phone_number, data in sorted_contacts[:top_n]
    ]

def get_latest_district(metadata_list):
    latest_metadata = max(metadata_list, key=lambda md: int(md.get(ES_PHONE_MD['month'], 0)))
    return latest_metadata.get(ES_PHONE_MD['most_district_from'], '')


def flat_list(prefix, list):
    result = {}
    for i, item in enumerate(list):
        for key, value in item.items():
            flat_key = f"{prefix}[{i}].{key}"
            result[flat_key] = value
    return result

def map_metadata(new_meta, old_key=MES_FIELD, new_key=ES_PHONE_MD):
    old_key = {v: k for k, v in old_key.items()}
    result = {}
    for k, v in new_meta.items():
        nk = old_key.get(k)
        if nk:
            result[new_key[nk]] = v 
    return result
