from collections import defaultdict


def construct_metadata(new_meta):
    return {
            "avg_calls": new_meta["total_calls"],
            "call_from_rate": new_meta["call_from_rate"],
            "business_call_rate": new_meta["business_call_rate"],
            "avg_duration_from": new_meta["avg_duration_from"],
            "avg_duration_to": new_meta["avg_duration_to"],
            "avg_weekend_call": new_meta["total_weekend_call"],
            "avg_night_call": new_meta["total_night_call"],
            "avg_day_from": new_meta["total_day_from"],
            "avg_contacts": new_meta["total_contacts"],
            "most_district_from": new_meta["most_district_from"],
            "top_5_contacts": new_meta["top_5_contacts"]
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
        total_calls = md.get('total_calls', 0)
        calc_md["total_calls_from"] = md.get('call_from_rate', 0) * total_calls
        calc_md["total_calls_to"] = total_calls - calc_md["total_calls_from"]
        calc_md["total_duration_from"] = md.get('total_duration_from', 0) * calc_md["total_calls_from"]
        calc_md["total_duration_to"] = md.get('total_duration_to', 0) * calc_md["total_calls_to"]
        calc_md["total_business_call"] = md.get('business_call_rate', 0) * total_calls
        for field in fields:
            agg[field] += md.get(field, 0)
        for field in calc_fields:
            agg[field] += calc_md[field]

    agg["most_district_from"] = get_latest_district(metadata_list)

    agg["top_5_contacts"] = merge_contacts(
        [md.get("top_10_contacts", []) for md in metadata_list],
        top_n=5
    )
    
    return {
        "avg_calls": agg["total_calls"] / count,
        "call_from_rate": agg["total_calls_from"] / agg["total_calls"],
        "business_call_rate": agg["total_business_call"] / agg["total_calls"],
        "avg_duration_from": agg["total_duration_from"] / agg["total_calls_from"],
        "avg_duration_to": agg["total_duration_to"] / agg["total_calls_to"],
        "avg_weekend_call": agg["total_weekend_call"] / count,
        "avg_night_call": agg["total_night_call"] / count,
        "avg_day_from": agg["total_day_from"] / count,
        "avg_contacts": agg["total_contacts"] / count,
        "most_district_from": agg["most_district_from"],
        "top_5_contacts": agg["top_5_contacts"]
    }

def merge_contacts(meatadata_list, top_n=5):
    contact_map = defaultdict(lambda: {"total_duration": 0, "total_calls": 0})

    for group in meatadata_list:
        for c in group:
            p = c["phone"]
            contact_map[p]["total_calls"] += c["total_calls"]
            contact_map[p]["total_duration"] += c["total_duration"]

    sorted_contacts = sorted(
        [{"phone": k, **v} for k, v in contact_map.items()],
        key=lambda x: (-x["total_duration"], -x["total_calls"])
    )

    return sorted_contacts[:top_n]

def get_latest_district(metadata_list):
    latest_metadata = max(metadata_list, key=lambda md: md.get("month", ""))
    return latest_metadata.get("most_district_from")
