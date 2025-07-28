from confluent_kafka import Consumer, KafkaException, KafkaError
from minio import Minio
from datetime import datetime
import json
import yaml
import logging
import io
import os # Added for path joining in __main__

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class KafkaS3Loader:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.kafka_config = self.config['kafka_config']
        self.s3_config = self.config['s3_landing_config']
        self.dataset_config = self.config['dataset'] # Added for potential future use

        self.topic = self.kafka_config['topic']

        self.consumer = None
        self.minio_client = None

    def _get_kafka_consumer(self):
        """Initializes and returns a Kafka Consumer."""
        conf = {
            'bootstrap.servers': self.kafka_config['bootstrap_servers'],
            'group.id': 's3_ingestion_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        try:
            consumer = Consumer(conf)
            logging.info(f"Kafka Consumer initialized for group '{conf['group.id']}'.")
            return consumer
        except Exception as e:
            logging.error(f"Error initializing Kafka Consumer: {e}")
            raise

    def _get_minio_client(self):
        """Initializes and returns a MinIO client."""
        try:
            client = Minio(
                endpoint=self.s3_config['endpoint'],
                access_key=self.s3_config['access_key'],
                secret_key=self.s3_config['secret_key'],
                secure=self.s3_config.get('secure', False)
            )
            # Ensure the bucket exists
            if not client.bucket_exists(self.s3_config['bucket_name']):
                client.make_bucket(self.s3_config['bucket_name'])
                logging.info(f"MinIO bucket '{self.s3_config['bucket_name']}' created.")
            else:
                logging.info(f"MinIO bucket '{self.s3_config['bucket_name']}' already exists.")
            return client
        except Exception as e:
            logging.error(f"Error initializing MinIO client or checking/creating bucket: {e}")
            raise

    def load_from_kafka_to_s3(self, num_messages_to_process=None, timeout_ms=1000):
        """Consumes messages from Kafka and loads them into MinIO."""
        self.consumer = self._get_kafka_consumer()
        self.minio_client = self._get_minio_client()

        self.consumer.subscribe([self.topic])
        logging.info(f"Subscribed to Kafka topic: {self.topic}")

        messages_processed = 0
        running = True

        while running:
            try:
                msg = self.consumer.poll(timeout=timeout_ms / 1000.0)

                if msg is None:
                    logging.info("No message received within timeout.")
                    if num_messages_to_process is not None and messages_processed > 0:
                        running = False
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                        if num_messages_to_process is not None and messages_processed > 0:
                            running = False
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    record_value = msg.value().decode('utf-8')
                    try:
                        data = json.loads(record_value)
                    except json.JSONDecodeError as e:
                        logging.error(f"Failed to decode JSON from message: {record_value}. Error: {e}")
                        continue

                    # Use 'invoicedate' from your transaction data for partitioning
                    # Fallback to current date if 'invoicedate' is not in the JSON or is invalid
                    try:
                        event_date_str = data.get('invoicedate')
                        if event_date_str:
                            event_date = datetime.fromisoformat(event_date_str)
                        else:
                            event_date = datetime.now()
                    except (ValueError, TypeError):
                        event_date = datetime.now()

                    file_name = f"{msg.topic()}-{msg.partition()}-{msg.offset()}.json"
                    object_name = (
                        f"{self.s3_config['prefix']}" # Use the prefix directly from contract
                        f"year={event_date.year}/"
                        f"month={event_date.month:02d}/"
                        f"day={event_date.day:02d}/"
                        f"{file_name}"
                    )

                    try:
                        data_bytes = record_value.encode('utf-8')
                        self.minio_client.put_object(
                            self.s3_config['bucket_name'],
                            object_name,
                            io.BytesIO(data_bytes),
                            len(data_bytes),
                            content_type="application/json"
                        )
                        logging.info(f"Successfully uploaded {object_name} to MinIO.")
                        messages_processed += 1
                        if num_messages_to_process is not None and messages_processed >= num_messages_to_process:
                            running = False
                            logging.info(f"Processed {messages_processed} messages as requested.")
                    except Exception as e:
                        logging.error(f"Failed to upload {object_name} to MinIO. Error: {e}")

            except KeyboardInterrupt:
                logging.info("Stopping Kafka consumer due to KeyboardInterrupt.")
                running = False
            except Exception as e:
                logging.error(f"An unhandled error occurred in consumer loop: {e}")
                running = False
            finally:
                if self.consumer:
                    self.consumer.close()
                    logging.info("Kafka Consumer closed.")


# Note: This __main__ block is for isolated testing, actual orchestration is in main_ingestion_job.py
if __name__ == "__main__":
    contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'transactions_contract.yaml')
    loader = KafkaS3Loader(contract_path)
    logging.info("Running KafkaS3Loader in isolated test mode (checking first 100 messages)...")
    loader.load_from_kafka_to_s3(num_messages_to_process=100)  # Process first 100 messages then exit
    # For a continuous test run (will run until no messages for timeout), remove num_messages_to_process
    # loader.load_from_kafka_to_s3()

