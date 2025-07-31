from confluent_kafka import Consumer, KafkaException, KafkaError
from minio import Minio
from datetime import datetime
import json
import yaml
import logging
import io
import uuid  # for generating unique batch IDs
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

        self.flush_message_count = self.s3_config.get('flush_message_count', 1000)  # Default to 1000
        self.flush_interval_seconds = self.s3_config.get('flush_interval_seconds', 10)  # Default to 10 seconds
        self.message_buffer = []  # stores (record_value, partition, offset)
        self.last_flush_time = datetime.now()

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
        except ConnectionError as conn_error:
            raise KafkaLoaderError(f"Connection to MinIO server failed: {conn_error}")
        except Exception as error:
            raise KafkaLoaderError(f"Unexpected error initializing MinIO client: {error}")

    @staticmethod
    def _get_event_date(event_date_str):
        """Parses the event date or defaults to the current date."""
        try:
            return datetime.fromisoformat(event_date_str) if event_date_str else datetime.now()
        except (ValueError, TypeError):
            # Fallback for invalid formats or types. Log for debugging.
            logging.warning(f"Invalid event_date_str '{event_date_str}'. Defaulting to current datetime.")
            return datetime.now()

    def _generate_batch_object_name(self, event_date, partition_id, min_offset, max_offset): # UPDATED signature
        """Generates the object name for MinIO upload for a batch."""
        # event_date for partitioning, and a unique ID + offset range for the batch file
        batch_uuid = uuid.uuid4()
        # partition and min/max offsets for traceability
        file_name = f"{event_date.strftime('%Y%m%d%H%M%S')}-{partition_id}-{min_offset}-{max_offset}-{batch_uuid}.jsonl"
        return (
            f"{self.s3_config['prefix']}"
            f"year={event_date.year}/"
            f"month={event_date.month:02d}/"
            f"day={event_date.day:02d}/"
            f"{file_name}"
        )

    def _flush_buffer_to_s3(self):
        """
        Writes the accumulated messages in the buffer to a single file in MinIO.
        Calculates min/max offsets from buffered messages for naming.
        """
        if not self.message_buffer:
            return

        logging.info(f"Flushing buffer of {len(self.message_buffer)} messages to S3...")

        min_offset = float('inf')
        max_offset = -1
        representative_partition = None
        batch_contents_list = []  # List to hold just the JSON string values

        for record_value_str, partition, offset in self.message_buffer:
            batch_contents_list.append(record_value_str)
            min_offset = min(min_offset, offset)
            max_offset = max(max_offset, offset)
            if representative_partition is None:  # Take the partition of the first message
                representative_partition = partition

        # partitioning the S3 path, as opposed to offsets in filename
        try:
            event_date = self._get_event_date(json.loads(batch_contents_list[-1]).get('invoicedate'))
        except json.JSONDecodeError:
            logging.warning("Failed to decode JSON from the last message in the buffer. Defaulting to current datetime.")
            event_date = datetime.now()  # Fallback if last message date is problematic

        # Ensures valid partition ID for filename even if buffer empty or unexpected
        if representative_partition is None:
            representative_partition = 0

        object_name = self._generate_batch_object_name(event_date, representative_partition, min_offset, max_offset)

        # Join all messages in the buffer + newline to create a JSONL string
        batch_content = "\n".join(batch_contents_list) + "\n"
        data_bytes = batch_content.encode('utf-8')

        try:
            self.minio_client.put_object(
                self.s3_config['bucket_name'],
                object_name,
                io.BytesIO(data_bytes),
                len(data_bytes),
                content_type="application/x-jsonlines"  # Standard type for JSONL
            )
            logging.info(f"Successfully uploaded batch file {object_name} with {len(self.message_buffer)} records.")
            self.message_buffer = []  # Clear buffer after successful flush
            self.last_flush_time = datetime.now()  # Reset flush timer
        except Exception as error:
            raise KafkaLoaderError(f"Failed to upload batch file {object_name} to MinIO. Error: {error}")

    def load_from_kafka_to_s3(self, num_messages_to_process=None, timeout_ms=1000, testing_mode=False):
        """Consumes messages from Kafka and loads them into MinIO in batches."""
        self.consumer = self._get_kafka_consumer(testing_mode=testing_mode)
        self.minio_client = self._get_minio_client()
        self.consumer.subscribe([self.topic])
        logging.info(f"Subscribed to Kafka topic: {self.topic}")
        messages_processed = 0
        is_running = True
        self.last_flush_time = datetime.now()  # Initialize flush timer at start of run
        try:
            while is_running:
                # smaller timeout for polling so flush conditions are checked more frequently
                # ideally be less than or equal to flush_interval_seconds
                poll_timeout = min(timeout_ms / 1000.0, self.flush_interval_seconds / 2.0)
                msg = self.consumer.poll(timeout=poll_timeout)
                if not msg:
                    logging.info("Timeout lapsed - No message received.")
                    # flush time and there's data in buffer, flush it
                    if (datetime.now() - self.last_flush_time).total_seconds() >= self.flush_interval_seconds:
                        if self.message_buffer:
                            self._flush_buffer_to_s3()
                    # exit when no more messages and buffer is empty
                    if num_messages_to_process is None and messages_processed > 0 and not self.message_buffer:
                        is_running = False
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(
                            f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}. Flushing any remaining buffer...")
                        self._flush_buffer_to_s3()  # ensures no data is left behind
                        if not num_messages_to_process and not self.message_buffer:  # Exit if continuous mode and buffer is now empty
                            is_running = False
                    else:
                        raise KafkaLoaderError(f"Kafka error: {msg.error()}")
                    continue
                    # Process valid message
                record_value = msg.value().decode('utf-8')
                try:
                    # - append (value, partition, offset) to buffer
                    self.message_buffer.append((record_value, msg.partition(), msg.offset()))
                    messages_processed += 1
                except Exception as error:
                    logging.error(
                        f"Failed to process message from Kafka: {record_value}. Error: {error}. Skipping message.")
                    continue
                # flush conditions - count or time interval
                if len(self.message_buffer) >= self.flush_message_count:
                    self._flush_buffer_to_s3()
                # flush by time only
                elif (datetime.now() - self.last_flush_time).total_seconds() >= self.flush_interval_seconds:
                    if self.message_buffer:
                        self._flush_buffer_to_s3()
                # if target messages processed if num_messages_to_process is set
                if num_messages_to_process and messages_processed >= num_messages_to_process:
                    logging.info(f"Processed {messages_processed} messages as requested.")
                    is_running = False
        except KeyboardInterrupt:
            logging.info("Stopping Kafka consumer due to KeyboardInterrupt.")
        except KafkaLoaderError:
            raise
        except Exception as error:
            raise KafkaLoaderError(f"An unhandled error occurred in consumer loop: {error}")
        finally:
            # any remaining messages in the buffer are flushed before closing
            if self.message_buffer:
                logging.info("Flushing remaining messages in buffer before closing...")
                try:
                    self._flush_buffer_to_s3()
                except Exception as error:
                    logging.error(f"Failed to flush remaining buffer on exit: {error}")
            if self.consumer:
                self.consumer.close()
                logging.info("Kafka Consumer closed.")


# Note: This __main__ block is for isolated testing, actual orchestration is in main_ingestion_job.py
# if __name__ == "__main__":
#     contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config',
#                                  'transactions_contract.yaml')
#     loader = KafkaS3Loader(contract_path)
#     logging.info("Running KafkaS3Loader in isolated test mode (checking first 100 messages)...")
#     loader.load_from_kafka_to_s3(num_messages_to_process=100, testing_mode=True)  # Process first 100 messages then exit
#     # For a continuous test run (will run until no messages for timeout), remove num_messages_to_process
#     # loader.load_from_kafka_to_s3()

