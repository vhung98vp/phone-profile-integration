import json
import time
import uuid
from confluent_kafka import Consumer, Producer
from .config import logger, KAFKA, KAFKA_CONSUMER_CONFIG, KAFKA_PRODUCER_CONFIG, MES_FIELD
from .utils import map_agg_meta_list
from .build_entity import build_phone_entity, build_top_phone_entities

# Kafka setup
producer = Producer(KAFKA_PRODUCER_CONFIG)
consumer = Consumer(KAFKA_CONSUMER_CONFIG)
consumer.subscribe([KAFKA['input_topic']])


def process_message(msg_key, msg):
    start_time = time.time()
    try:
        data = json.loads(msg)
        phone_number = data.get(MES_FIELD['phone_number'])
        meta_list = [json.loads(meta) for meta in data.get(MES_FIELD['metadata'], [])]
        agg_data, es_metalist = map_agg_meta_list(meta_list)

        phone_entity = build_phone_entity(phone_number, agg_data, es_metalist)
        send_output_to_kafka(phone_entity)

        # top_phone_entities = build_top_phone_entities(agg_data)
        # for item in top_phone_entities:
        #     send_output_to_kafka(item)

        logger.info(f"Updated metadata for phone: {phone_number}.")

    except Exception as e:
        logger.exception(f"Error while processing message {msg_key}:{msg}: {e}")
        log_error_to_kafka(msg_key, { 
            "error": str(e), 
            "message": msg 
        })
        raise e
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
                process_message(message_key, message)
                processed_count += 1
            except Exception as e:
                error_count += 1
    except Exception as e:
        logger.exception(f"Consumer process terminated: {e}")
    finally:
        consumer.close()
        producer.flush()
        logger.info(f"Processed {processed_count} messages with {error_count} errors.")


def send_output_to_kafka(result: dict):
    try:
        producer.produce(KAFKA['output_topic'], key=str(uuid.uuid4()), value=json.dumps(result, ensure_ascii=False))
        producer.poll(0)
    except Exception as e:
        logger.exception(f"Error sending result to output topic: {e}")


def log_error_to_kafka(msg_key, error_info: dict):
    try:
        producer.produce(KAFKA['error_topic'], key=msg_key, value=json.dumps(error_info, ensure_ascii=False))
        producer.flush()
    except Exception as e:
        logger.exception(f"Error sending to error topic: {e}")
