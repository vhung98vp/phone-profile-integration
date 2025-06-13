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
- `ES_UUID_NAMESPACE`: UUID Namespace to gen entity UUID
- `ES_ENTITY_TYPE`: Entity type of phone in Elasticsearch
- `ADD_TOP_PHONE_ENTITY`: Option to add empty entity in top 5 to Elasticsearch
- `THRESHOLD_TOP_5_TOTAL_DURATION`: Threshold duration to add number to top 5 overall
- `THRESHOLD_TOP_5_TOTAL_CALLS`: Threshold calls to add number to top 5 overall
- `KAFKA_BOOTSTRAP_SERVER`: Kafka bootstrap server address
- `KAFKA_CONSUMER_GROUP`: Kafka consumer group name
- `KAFKA_CONSUMER_TIMEOUT`: Kafka consumer timeout (in second)
- `KAFKA_AUTO_OFFSET_RESET`: Kafka offset reset policy
- `KAFKA_INPUT_TOPIC`: Kafka topic for input messages
- `KAFKA_OUTPUT_TOPIC`: Kafka topic for output messages
- `KAFKA_ERROR_TOPIC`: Kafka topic for error messages
<!-- - `MAX_WORKERS`: Maximum number of worker threads -->

1 _s_srcha, _dt_srcha
2 field typeField
3 dict > string
4 month > mini_timestamp (thang)
5 es string > int, float, mini_timestamp