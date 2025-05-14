import json
import time
from confluent_kafka import Consumer, Producer
from concurrent.futures import ThreadPoolExecutor
from threading import Thread
from .config import logger, KAFKA, KAFKA_CONSUMER_CONFIG, KAFKA_PRODUCER_CONFIG, MAX_WORKERS
from .utils import merge_metadata, construct_metadata
from .elasticsearch import query_elasticsearch

# Kafka setup
producer = Producer(KAFKA_PRODUCER_CONFIG)
consumer = Consumer(KAFKA_CONSUMER_CONFIG)
consumer.subscribe([KAFKA['input_topic']])

executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)


def process_message(msg_key, msg):
    start_time = time.time()
    try:
        data = json.loads(msg)
        phone_number = data.get("phone_number")
        new_meta = data.get("metadata")

        record = query_elasticsearch(phone_number)
        if record:
            # Check if in metadata array exist new_metadata monthly data
            if any(new_meta["month"] == meta["month"] 
                    and new_meta["total_calls"] == meta["total_calls"] 
                    for meta in record["metadata"]):
                logger.info(f"Monthly data already exists for {phone_number}.")
                return
            record["metadata"].append(new_meta)
            agg_data = merge_metadata(record["metadata"])
        else:
            record = {"phone_number": phone_number, "metadata": [new_meta]}
            agg_data = construct_metadata(new_meta)

        result = {
            "phone_number": phone_number,
            "metadata": record["metadata"],
            **agg_data
        }

        send_output_to_kafka(result)
        logger.info(f"Processed message for {phone_number}!")

    except Exception as e:
        logger.exception(f"Error while processing message {msg_key}:{msg}: {e}")
        log_error_to_kafka({
            "error": str(e),
            "key": msg_key,
            "message": msg
        })
    finally:
        logger.info(f"Processed message in {time.time() - start_time:.4f} seconds")


def start_kafka_consumer():
    processed_count = 0
    error_count = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                if msg.error():
                    logger.error(f"Message error: {msg.error()}")
                else:
                    logger.info("Waiting for messages...")
                continue
            try:
                message = msg.value().decode("utf-8")
                message_key = msg.key().decode("utf-8") if msg.key() else None
                if not message_key:
                    logger.warning(f"Received message without key: {message}")
                executor.submit(process_message, message_key, message)
                consumer.commit(asynchronous=False)
                processed_count += 1
            except Exception as e:
                logger.exception(f"Failed to process message: {e}")
                error_count += 1
    except Exception as e:
        logger.exception(f"Consumer process terminated: {e}")
    finally:
        consumer.close()
        logger.info(f"Processed {processed_count} messages with {error_count} errors.")


def send_output_to_kafka(result: dict):
    try:
        producer.produce(KAFKA['output_topic'], value=json.dumps(result))
        producer.poll(0)
    except Exception as e:
        logger.exception(f"Error sending result to output topic: {e}")


def log_error_to_kafka(error_info: dict):
    try:
        producer.produce(KAFKA['error_topic'], value=json.dumps(error_info))
        producer.flush()
    except Exception as e:
        logger.exception(f"Error sending to error topic: {e}")
