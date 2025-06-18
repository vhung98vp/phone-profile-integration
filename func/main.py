from .utils import map_agg_meta_list, build_phone_entity

def main(phone_number, meta_list):
    agg_data, es_metalist = map_agg_meta_list(meta_list)
    message = build_phone_entity(phone_number, agg_data, es_metalist)
    # Send output message to Kafka
