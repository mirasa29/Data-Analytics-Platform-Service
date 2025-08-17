from confluent_kafka import Consumer, KafkaException, KafkaError
from minio import Minio
from datetime import datetime
import json
import logging
import io
import uuid
from typing import List, Tuple
# isolated tests
import os
import yaml
from models.pipeline_config import PipelineConfig, KafkaConfig, S3LandingConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class KafkaLoaderError(Exception):
    """Custom exception for KafkaLoader errors."""

    def __init__(self, error: str):
        super().__init__(f"Kafka Loader failure: {error}")
        logging.error(f"Kafka Loader failure: {error}")


class KafkaS3Loader:
    def __init__(self):
        self.message_buffer: List[Tuple[str, int, int]] = []
        self.last_flush_time = datetime.now()
        self.consumer = None
        self.minio_client = None

    @staticmethod
    def _get_kafka_consumer(kafka_config: KafkaConfig, testing_mode=False) -> Consumer:
        """Initializes and returns a Kafka Consumer."""
        group_id = 's3_ingestion_group'
        if testing_mode:
            group_id = f"s3_test_group_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        conf = {
            'bootstrap.servers': kafka_config.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        try:
            return Consumer(conf)
        except Exception as error:
            raise KafkaLoaderError(f"Failed to initialize Kafka Consumer: {error}")

    @staticmethod
    def _get_minio_client(s3_config: S3LandingConfig) -> Minio:
        """Initializes and returns a MinIO client."""
        try:
            client = Minio(
                endpoint=s3_config.endpoint,
                access_key=s3_config.access_key,
                secret_key=s3_config.secret_key,
                secure=s3_config.secure
            )
            if not client.bucket_exists(s3_config.bucket_name):
                client.make_bucket(s3_config.bucket_name)
                logging.info(f"MinIO bucket '{s3_config.bucket_name}' created.")
            else:
                logging.info(f"MinIO bucket '{s3_config.bucket_name}' already exists.")
            return client
        except ValueError as value_error:
            raise KafkaLoaderError(f"Invalid MinIO configuration: {value_error}")
        except ConnectionError as conn_error:
            raise KafkaLoaderError(f"Connection to MinIO server failed: {conn_error}")
        except Exception as error:
            raise KafkaLoaderError(f"Unexpected error initializing MinIO client: {error}")

    @staticmethod
    def _get_event_date(event_date_str) -> datetime:
        """Parses the event date or defaults to the current date."""
        try:
            return datetime.fromisoformat(event_date_str) if event_date_str else datetime.now()
        except (ValueError, TypeError):
            logging.warning(f"Invalid event_date_str '{event_date_str}'. Defaulting to current datetime.")
            return datetime.now()

    @staticmethod
    def _generate_batch_object_name(s3_config: S3LandingConfig, event_date: datetime, partition_id: int,
                                    min_offset: int, max_offset: int) -> str:
        """Generates the object name for MinIO upload for a batch."""
        batch_uuid = uuid.uuid4()
        file_name = f"{event_date.strftime('%Y%m%d%H%M%S')}-{partition_id}-{min_offset}-{max_offset}-{batch_uuid}.jsonl"
        return (
            f"{s3_config.prefix}"
            f"year={event_date.year}/"
            f"month={event_date.month:02d}/"
            f"day={event_date.day:02d}/"
            f"{file_name}"
        )

    def _flush_buffer_to_s3(self, s3_config: S3LandingConfig):
        """Writes the accumulated messages in the buffer to a single file in MinIO."""
        if not self.message_buffer:
            return

        logging.info(f"Flushing buffer of {len(self.message_buffer)} messages to S3...")

        min_offset = float('inf')
        max_offset = -1
        representative_partition = None
        batch_contents_list = []

        for record_value_str, partition, offset in self.message_buffer:
            batch_contents_list.append(record_value_str)
            min_offset = min(min_offset, offset)
            max_offset = max(max_offset, offset)
            if representative_partition is None:
                representative_partition = partition

        try:
            last_message_data = json.loads(batch_contents_list[-1])
            event_date = self._get_event_date(last_message_data.get('invoicedate'))
        except (json.JSONDecodeError, IndexError):
            logging.warning("Failed to decode JSON from last message in buffer. Defaulting to current datetime.")
            event_date = datetime.now()

        if representative_partition is None:
            representative_partition = 0

        object_name = self._generate_batch_object_name(s3_config, event_date, representative_partition, min_offset,
                                                       max_offset)
        batch_content = "\n".join(batch_contents_list) + "\n"
        data_bytes = batch_content.encode('utf-8')

        try:
            self.minio_client.put_object(
                s3_config.bucket_name,
                object_name,
                io.BytesIO(data_bytes),
                len(data_bytes),
                content_type="application/x-jsonlines"
            )
            logging.info(f"Successfully uploaded batch file {object_name} with {len(self.message_buffer)} records.")
            self.message_buffer = []
            self.last_flush_time = datetime.now()
        except Exception as error:
            raise KafkaLoaderError(f"Failed to upload batch file {object_name} to MinIO. Error: {error}")

    def load_from_kafka_to_s3(self, pipeline_config: PipelineConfig, num_messages_to_process=None, timeout_ms=1000,
                              testing_mode=False):
        """Consumes messages from Kafka and loads them into MinIO in batches."""
        self.consumer = self._get_kafka_consumer(pipeline_config.kafka_config, testing_mode=testing_mode)
        self.minio_client = self._get_minio_client(pipeline_config.s3_landing_config)
        self.consumer.subscribe([pipeline_config.kafka_config.topic])
        logging.info(f"Subscribed to Kafka topic: {pipeline_config.kafka_config.topic}")

        messages_processed = 0
        is_running = True
        self.last_flush_time = datetime.now()

        try:
            while is_running:
                poll_timeout = min(timeout_ms / 1000.0, pipeline_config.s3_landing_config.flush_interval_seconds / 2.0)
                msg = self.consumer.poll(timeout=poll_timeout)
                if not msg:
                    logging.info("Timeout lapsed - No message received.")
                    if (
                            datetime.now() - self.last_flush_time).total_seconds() >= pipeline_config.s3_landing_config.flush_interval_seconds:
                        if self.message_buffer:
                            self._flush_buffer_to_s3(pipeline_config.s3_landing_config)
                    if num_messages_to_process is None and messages_processed > 0 and not self.message_buffer:
                        is_running = False
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logging.info(
                            f"End of partition reached {msg.topic()} [{msg.partition()}] at offset {msg.offset()}. Flushing any remaining buffer...")
                        self._flush_buffer_to_s3(pipeline_config.s3_landing_config)
                        if num_messages_to_process is None and not self.message_buffer:
                            is_running = False
                    else:
                        raise KafkaLoaderError(f"Kafka error: {msg.error()}")
                    continue
                record_value = msg.value().decode('utf-8')
                try:
                    self.message_buffer.append((record_value, msg.partition(), msg.offset()))
                    messages_processed += 1
                except Exception as error:
                    logging.error(
                        f"Failed to process message from Kafka: {record_value}. Error: {error}. Skipping message.")
                    continue

                if len(self.message_buffer) >= pipeline_config.s3_landing_config.flush_message_count:
                    self._flush_buffer_to_s3(pipeline_config.s3_landing_config)
                elif (
                        datetime.now() - self.last_flush_time).total_seconds() >= pipeline_config.s3_landing_config.flush_interval_seconds:
                    if self.message_buffer:
                        self._flush_buffer_to_s3(pipeline_config.s3_landing_config)

                if num_messages_to_process is not None and messages_processed >= num_messages_to_process:
                    logging.info(f"Processed {messages_processed} messages as requested.")
                    is_running = False
        except KeyboardInterrupt:
            logging.info("Stopping Kafka consumer due to KeyboardInterrupt.")
        except KafkaLoaderError:
            raise
        except Exception as error:
            raise KafkaLoaderError(f"An unhandled error occurred in consumer loop: {error}")
        finally:
            if self.message_buffer:
                logging.info("Flushing remaining messages in buffer before closing...")
                try:
                    self._flush_buffer_to_s3(pipeline_config.s3_landing_config)
                except Exception as error:
                    logging.error(f"Failed to flush remaining buffer on exit: {error}")
            if self.consumer:
                self.consumer.close()
                logging.info("Kafka Consumer closed.")


# note: only for isolated testing, actual orchestration is through main_ingestion_job.py
# if __name__ == "__main__":
#     contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'transactions_contract.yaml')
#
#     try:
#         with open(contract_path, 'r') as f:
#             config_dict = yaml.safe_load(f)
#         pipeline_config = PipelineConfig.model_validate(config_dict)
#
#         loader = KafkaS3Loader()
#         logging.info("Running KafkaS3Loader in isolated tests mode (checking first 100 messages)...")
#         loader.load_from_kafka_to_s3(pipeline_config, num_messages_to_process=100, testing_mode=True)
#
#     except Exception as error:
#         logging.error(f"Isolated tests run failed: {error}")
