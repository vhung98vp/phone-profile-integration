# Phone Profile Integration
This project is designed to manage and integrate phone profile data into the company's systems. 

## Features

- Integration Kafka with phone profile elasticsearch entity.
- Scalable and modular architecture.
- Comprehensive logging and error handling.

## Configuration
- `ES_URL`: URL of the Elasticsearch instance
- `ES_USER`: Elasticsearch username
- `ES_PASSWORD`: Elasticsearch password
- `ES_PHONE_INDEX`: Name of the Elasticsearch index for phone profiles
- `KAFKA_BOOTSTRAP_SERVER`: Kafka bootstrap server address
- `KAFKA_CONSUMER_GROUP`: Kafka consumer group name
- `KAFKA_CONSUMER_TIMEOUT`: Kafka consumer timeout (in second)
- `KAFKA_AUTO_OFFSET_RESET`: Kafka offset reset policy
- `KAFKA_INPUT_TOPIC`: Kafka topic for input messages
- `KAFKA_OUTPUT_TOPIC`: Kafka topic for output messages
- `KAFKA_ERROR_TOPIC`: Kafka topic for error messages
- `MAX_WORKERS`: Maximum number of worker threads
