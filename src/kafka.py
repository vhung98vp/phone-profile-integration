import json
import time
import uuid
from confluent_kafka import Consumer, Producer
# from concurrent.futures import ThreadPoolExecutor
from .config import logger, KAFKA, KAFKA_CONSUMER_CONFIG, \
    KAFKA_PRODUCER_CONFIG, MAX_WORKERS, MES_FIELD, ES_PROPERTY
from .utils import merge_metadata, build_agg_metadata, flat_list, map_metadata, metadata_index, is_metadata_exist, map_to_str
from .elasticsearch import query_elasticsearch

# Kafka setup
producer = Producer(KAFKA_PRODUCER_CONFIG)
consumer = Consumer(KAFKA_CONSUMER_CONFIG)
consumer.subscribe([KAFKA['input_topic']])

# executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def process_message(msg_key, msg):
    start_time = time.time()
    try:
        data = json.loads(msg)
        phone_number = data.get(MES_FIELD['phone_number'])
        new_meta = data.get(MES_FIELD['metadata'])
        if not phone_number or not new_meta:
            logger.warning(f"Invalid message data: {msg}")
            return
        new_meta = map_metadata(new_meta)

        es_record = query_elasticsearch(phone_number)
        if es_record:
            # Check if in metadata array exist new metadata monthly data
            cur_index = metadata_index(es_record['metadata'], new_meta)
            if cur_index != -1:
                if is_metadata_exist(es_record['metadata'][cur_index], new_meta):
                    logger.info(f"Monthly data already exists for {phone_number}.")
                    return
                else:
                    logger.info(f"Updating metadata for {phone_number}.")
                    es_record['metadata'][cur_index] = new_meta
            else:
                es_record['metadata'].append(new_meta)
            agg_data = merge_metadata(es_record['metadata'])
        else:
            es_record = {'metadata': [new_meta]}
            agg_data = build_agg_metadata(new_meta)

        phone_uid = str(uuid.uuid5(uuid.NAMESPACE_DNS, phone_number))
        result = {
            ES_PROPERTY['internal_id']: phone_uid,
            ES_PROPERTY['phone_number']: phone_number,
            **agg_data,
            **flat_list(ES_PROPERTY['metadata'], es_record['metadata'])
        }

        send_output_to_kafka(map_to_str(result))
        logger.info(f"Updated metadata to Kafka for phone number: {phone_number}")

    except Exception as e:
        logger.exception(f"Error while processing message {msg_key}:{msg}: {e}")
        log_error_to_kafka(msg_key, { 
            "error": str(e), 
            "message": msg 
        })
    finally:
        logger.info(f"Processed message {msg_key} in {time.time() - start_time:.4f} seconds")


def start_kafka_consumer():
    processed_count = 0
    error_count = 0
    last_wait_time = 0
    try:
        while True:
            msg = consumer.poll(KAFKA['consumer_timeout'])
            if msg is None or msg.error():
                if msg is None:
                    cur_time = time.time()
                    if cur_time - last_wait_time > 60:
                        logger.info("Waiting for messages...")
                        last_wait_time = cur_time
                else:
                    logger.error(f"Message error: {msg.error()}")
                continue
            try:
                message = msg.value().decode("utf-8")
                message_key = msg.key().decode("utf-8") if msg.key() else None
                if not message_key:
                    logger.warning(f"Received message without key: {message}")
                # executor.submit(process_message, message_key, message)
                process_message(message_key, message)
                consumer.commit(asynchronous=False)
                processed_count += 1
            except Exception as e:
                logger.exception(f"Failed to process message: {e}")
                error_count += 1
    except Exception as e:
        logger.exception(f"Consumer process terminated: {e}")
    finally:
        consumer.close()
        producer.flush()
        logger.info(f"Processed {processed_count} messages with {error_count} errors.")


def send_output_to_kafka(result: dict):
    try:
        producer.produce(KAFKA['output_topic'], key=str(uuid.uuid4()), value=json.dumps(result))
        producer.poll(0)
    except Exception as e:
        logger.exception(f"Error sending result to output topic: {e}")


def log_error_to_kafka(msg_key, error_info: dict):
    try:
        producer.produce(KAFKA['error_topic'], key=msg_key, value=json.dumps(error_info))
        producer.flush()
    except Exception as e:
        logger.exception(f"Error sending to error topic: {e}")
