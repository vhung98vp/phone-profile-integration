from src.kafka import start_kafka_consumer
from src.config import logger

if __name__ == "__main__":
    logger.info("Starting Kafka consumer and worker threads...")
    start_kafka_consumer()
