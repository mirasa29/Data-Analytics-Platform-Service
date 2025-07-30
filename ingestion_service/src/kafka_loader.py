from confluent_kafka import Consumer, KafkaException, KafkaError
from minio import Minio
from datetime import datetime
import json
import yaml
import logging
import io
import os  # only for isolated testing, remove in production

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class KafkaLoaderError(Exception):
    """Custom exception for KafkaLoader errors."""
    def __init__(self, error: str):
        super().__init__(f"Kafka Loader failure: {error}")
        logging.error(f"Kafka Loader failure: {error}")


class KafkaS3Loader:
    def __init__(self, config_path):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.kafka_config = self.config['kafka_config']
        self.s3_config = self.config['s3_landing_config']
        self.dataset_config = self.config['dataset']

        self.topic = self.kafka_config['topic']

        self.consumer = None
        self.minio_client = None

    def _get_kafka_consumer(self, testing_mode=False):
        """Initializes and returns a Kafka Consumer."""
        group_id = 's3_ingestion_group'
        if testing_mode:
            group_id = f"s3_test_group_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"  # Unique ID for testing

        conf = {
            'bootstrap.servers': self.kafka_config['bootstrap_servers'],
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        try:
            consumer = Consumer(conf)
            logging.info(f"Kafka Consumer initialized for group '{conf['group.id']}'.")
            return consumer
        except Exception as error:
            raise KafkaLoaderError(f"Failed to initialize Kafka Consumer: {error}")

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
        except ValueError as value_error:
            raise KafkaLoaderError(f"Invalid MinIO configuration: {value_error}")
        except ConnectionError as conn_error: # Minio client raises ConnectionError
            raise KafkaLoaderError(f"Connection to MinIO server failed: {conn_error}")
        except Exception as error:
            raise KafkaLoaderError(f"Unexpected error initializing MinIO client: {error}")

    @staticmethod # <--- This decorator is correct
    def _get_event_date(event_date_str): # <--- REMOVED 'self' from here
        """Parses the event date or defaults to the current date."""
        try:
            return datetime.fromisoformat(event_date_str) if event_date_str else datetime.now()
        except (ValueError, TypeError):
            # Fallback for invalid formats or types. Log for debugging.
            logging.warning(f"Invalid event_date_str '{event_date_str}'. Defaulting to current datetime.")
            return datetime.now()

    def _generate_object_name(self, msg, event_date):
        """Generates the object name for MinIO upload."""
        file_name = f"{msg.topic()}-{msg.partition()}-{msg.offset()}.json"
        return (
            f"{self.s3_config['prefix']}"
            f"year={event_date.year}/"
            f"month={event_date.month:02d}/"
            f"day={event_date.day:02d}/"
            f"{file_name}"
        )

    def _upload_to_minio(self, record_value, object_name):
        """Uploads the record to MinIO."""
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
        except Exception as error: # Catch specific MinIO exceptions if needed, otherwise general
            raise KafkaLoaderError(f"Failed to upload {object_name} to MinIO. Error: {error}")

    def load_from_kafka_to_s3(self, num_messages_to_process=None, timeout_ms=1000, testing_mode=False):
        """Consumes messages from Kafka and loads them into MinIO."""
        self.consumer = self._get_kafka_consumer(testing_mode=testing_mode)
        self.minio_client = self._get_minio_client()
        self.consumer.subscribe([self.topic])
        logging.info(f"Subscribed to Kafka topic: {self.topic}")

        messages_processed = 0
        is_running = True # Use a clear flag for loop control

        try:
            while is_running:
                msg = self.consumer.poll(timeout=timeout_ms / 1000.0)

                if msg is None:
                    logging.info("Timeout lapsed - No message received.")
                    # If num_messages_to_process is None (continuous mode) AND we've processed *some* messages
                    # AND we then hit a timeout, we assume no more data for this batch interval
                    if num_messages_to_process is None and messages_processed > 0:
                        is_running = False # Exit loop
                    continue # Continue polling if no messages but still expect more (or if messages_processed == 0)

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(
                            f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                        # Similar logic for EOF: if continuous and processed some, assume end of current stream
                        if num_messages_to_process is None and messages_processed > 0:
                            is_running = False
                    else:
                        raise KafkaLoaderError(f"Kafka error: {msg.error()}") # Raise custom error for Kafka errors
                    continue # Continue to next poll, even on EOF

                # Process valid message
                record_value = msg.value().decode('utf-8')
                try:
                    data = json.loads(record_value)
                except json.JSONDecodeError as error:
                    logging.error(f"Failed to decode JSON: {record_value}. Error: {error}. Skipping message.")
                    continue

                event_date = self._get_event_date(data.get('invoicedate')) # <--- Corrected call here
                object_name = self._generate_object_name(msg, event_date)

                self._upload_to_minio(record_value, object_name) # _upload_to_minio handles its own KafkaLoaderError
                messages_processed += 1
                if num_messages_to_process is not None and messages_processed >= num_messages_to_process:
                    logging.info(f"Processed {messages_processed} messages as requested.")
                    is_running = False # Exit loop if target messages processed

        except KeyboardInterrupt:
            logging.info("Stopping Kafka consumer due to KeyboardInterrupt.")
        except KafkaLoaderError: # Catch custom errors and log before re-raising
            raise # Re-raise if it's our custom error
        except Exception as error: # Catch any other unexpected errors
            raise KafkaLoaderError(f"An unhandled error occurred in consumer loop: {error}") # Wrap and re-raise
        finally:
            if self.consumer:
                self.consumer.close()
                logging.info("Kafka Consumer closed.")


# Note: This __main__ block is for isolated testing, actual orchestration is in main_ingestion_job.py
# if __name__ == "__main__":
#     contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'transactions_contract.yaml')
#     loader = KafkaS3Loader(contract_path)
#     logging.info("Running KafkaS3Loader in isolated test mode (checking first 100 messages)...")
#     loader.load_from_kafka_to_s3(num_messages_to_process=100, testing_mode=True)  # Process first 100 messages then exit
#     # For a continuous test run (will run until no messages for timeout), remove num_messages_to_process
#     # loader.load_from_kafka_to_s3()

