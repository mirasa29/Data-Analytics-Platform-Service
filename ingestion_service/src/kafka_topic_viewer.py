from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import yaml
import logging
from datetime import datetime
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def check_kafka_topic(config_path, num_messages_to_check=10, timeout_ms=5000):
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)

    kafka_config = config['kafka_config']
    topic = kafka_config['topic']

    conf = {
        'bootstrap.servers': kafka_config['bootstrap_servers'],
        'group.id': 'topic_checker_group_' + str(datetime.now().timestamp()), # Unique group ID for fresh read
        'auto.offset.reset': 'earliest', # Start from beginning for verification
        'enable.auto.commit': False # Don't commit offsets, just inspect
    }
    consumer = Consumer(conf)

    try:
        consumer.subscribe([topic])
        logging.info(f"Subscribed to topic '{topic}'. Waiting for messages...")

        messages_count = 0
        while messages_count < num_messages_to_check: # Loop to check a limited number of messages
            msg = consumer.poll(timeout=timeout_ms / 1000.0) # Poll with timeout in seconds

            if msg is None:
                logging.info(f"No more messages received within {timeout_ms / 1000.0} seconds. Total received: {messages_count}")
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logging.info("Reached end of partition(s).")
                    break
                else:
                    logging.error(f"Kafka error: {msg.error()}")
                    break
            else:
                # Message is valid
                record_key = msg.key().decode('utf-8') if msg.key() else 'N/A'
                record_value = msg.value().decode('utf-8')

                logging.info(f"--- Message {messages_count + 1} ---")
                logging.info(f"  Key: {record_key}")
                # Print value, maybe pretty-printed if it's JSON
                try:
                    pretty_value = json.dumps(json.loads(record_value), indent=2)
                    logging.info(f"  Value (JSON): \n{pretty_value[:500]}...") # Print first 500 chars of pretty JSON
                except json.JSONDecodeError:
                    logging.info(f"  Value (Raw): {record_value[:500]}...") # Fallback for non-JSON
                logging.info(f"Topic: {msg.topic()}, Partition: {msg.partition()}, Offset: {msg.offset()}")

                messages_count += 1
                if messages_count % 10 == 0:
                    logging.info(f"Received {messages_count} messages so far...")

        logging.info(f"Finished checking. Total messages received: {messages_count}")

    except KeyboardInterrupt:
        logging.info("Kafka topic check stopped by user.")
    except Exception as e:
        logging.error(f"An unhandled error occurred: {e}")
    finally:
        consumer.close()
        logging.info("Kafka consumer closed.")


if __name__ == "__main__":
    contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'transactions_contract.yaml')
    check_kafka_topic(contract_path, num_messages_to_check=5) # Check first 5 messages
    # For a more exhaustive check, remove num_messages_to_check or set to a large number
    # check_kafka_topic(contract_path, num_messages_to_check=1000)