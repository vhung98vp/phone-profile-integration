import re
from collections import defaultdict
from config import ES_PROPERTY, ES_PROPERTY_MD


def build_agg_metadata(new_meta):
    agg_fields = [ 'total_calls', 'call_from_rate', 'business_call_rate',
        'avg_duration_from', 'avg_duration_to', 'total_weekend_call', 'total_night_call',
        'total_day_from', 'total_contacts', 'most_district_from', 'top_5_contacts'
    ]
    return {
        ES_PROPERTY[field]: new_meta[ES_PROPERTY_MD[field]] 
        for field in agg_fields
    }

def merge_metadata(metadata_list):
    fields = [ 
        "total_calls", "total_weekend_call", "total_night_call", 
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
        total_calls = int(md.get(ES_PROPERTY_MD['total_calls'], 0))
        calc_md["total_calls_from"] = float(md.get(ES_PROPERTY_MD['call_from_rate'], 0)) * total_calls
        calc_md["total_calls_to"] = total_calls - calc_md["total_calls_from"]
        calc_md["total_duration_from"] = float(md.get(ES_PROPERTY_MD['avg_duration_from'], 0)) * calc_md["total_calls_from"]
        calc_md["total_duration_to"] = float(md.get(ES_PROPERTY_MD['avg_duration_to'], 0)) * calc_md["total_calls_to"]
        calc_md["total_business_call"] = float(md.get(ES_PROPERTY_MD['business_call_rate'], 0)) * total_calls
        for field in fields:
            agg[field] += int(md.get(ES_PROPERTY_MD[field], 0))
        for field in calc_fields:
            agg[field] += calc_md[field]

    agg["most_district_from"] = get_latest_district(metadata_list)
    agg["top_5_contacts"] = merge_contacts(metadata_list, top_n=5)
    
    return {
        ES_PROPERTY["total_calls"]: agg["total_calls"] / count,
        ES_PROPERTY["call_from_rate"]: agg["total_calls_from"] / agg["total_calls"],
        ES_PROPERTY["business_call_rate"]: agg["total_business_call"] / agg["total_calls"],
        ES_PROPERTY["avg_duration_from"]: agg["total_duration_from"] / agg["total_calls_from"],
        ES_PROPERTY["avg_duration_to"]: agg["total_duration_to"] / agg["total_calls_to"],
        ES_PROPERTY["total_weekend_call"]: agg["total_weekend_call"] / count,
        ES_PROPERTY["total_night_call"]: agg["total_night_call"] / count,
        ES_PROPERTY["total_day_from"]: agg["total_day_from"] / count,
        ES_PROPERTY["total_contacts"]: agg["total_contacts"] / count,
        ES_PROPERTY["most_district_from"]: agg["most_district_from"],
        ES_PROPERTY["top_5_contacts"]: agg["top_5_contacts"]
    }

def merge_contacts(metadata_list, top_n=5):
    top_contacts = [contact for md in metadata_list for contact in md.get(ES_PROPERTY_MD['top_10_contacts'], [])]
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
    latest_metadata = max(metadata_list, key=lambda md: int(md.get(ES_PROPERTY_MD['month'], 0)))
    return latest_metadata.get(ES_PROPERTY_MD['most_district_from'], '')


def flat_list(prefix, list):
    result = {}
    for i, item in enumerate(list):
        for key, value in item.items():
            flat_key = f"{prefix}[{i}].{key}"
            result[flat_key] = value
    return result
