import time
import logging
from datetime import datetime, timedelta
import json
import io
import os
import uuid
import psycopg2
import csv
from confluent_kafka import Producer, Consumer, KafkaException, KafkaError
from minio import Minio
from decimal import Decimal
from typing import List, Tuple, Any

from src.state_manager import StateManager
from models.pipeline_config import PipelineConfig, KafkaConfig, S3LandingConfig, SourceConfig


class IngestionWorkerError(Exception):
    """Custom exception for IngestionWorker errors with specific types."""

    def __init__(self, error: str, error_type: str = "Generic"):
        super().__init__(f"{error_type} Ingestion Worker failure: {error}")
        logging.error(f"{error_type} Ingestion Worker failure: {error}")


class CustomJsonEncoder(json.JSONEncoder):
    """Customised JSON Encoder for data serialization
    (for MVP: scope only handles decimal, datetime)
    """

    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        if isinstance(obj, (bytes, bytearray)):
            try:
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                return obj.hex()
        return json.JSONEncoder.default(self, obj)


class IngestionWorker:
    def __init__(self, pipeline_config: PipelineConfig, state_manager: StateManager):
        self.config = pipeline_config
        self.state_manager = state_manager

        self.messages_produced = 0
        self.messages_consumed = 0

        self.poll_interval = timedelta(seconds=self.config.s3_landing_config.flush_interval_seconds)
        self.last_check_time = datetime.min
        self.last_ingestion_time = datetime.now()  # Variable to track last successful ingestion

        self.db_conn = None
        self.producer = None
        self.consumer = None
        self.minio_client = None
        self.message_buffer: List[Tuple[str, int, int]] = []
        self.last_flush_time = datetime.now()

    # --- Database and File Extraction Methods ---
    def _get_db_connection(self) -> None:
        """Establishes a connection to the source database."""
        try:
            self.db_conn = psycopg2.connect(
                host=self.config.producer.source_config.host,
                port=self.config.producer.source_config.port,
                database=self.config.producer.source_config.database,
                user=self.config.producer.source_config.user,
                password=self.config.producer.source_config.password
            )
            logging.info("Successfully connected to source Database.")
        except psycopg2.Error as db_error:
            raise IngestionWorkerError(f"Database connection error {db_error}", "Database")

    def _build_select_query(self) -> str:
        """Builds the SELECT clause for the SQL query based on the dataset schema."""
        columns = [field.name for field in self.config.dataset.data_schema.fields]
        return ", ".join(columns)

    def _extract_from_rdbms_and_produce(self, last_extracted_value) -> str:
        """Extracts data from the RDBMS and produces messages to Kafka."""
        new_max_watermark_value = last_extracted_value
        cursor = self.db_conn.cursor()

        try:
            select_clause = self._build_select_query()
            query_sql = ""
            query_params = []

            if self.config.dataset.incremental_load.mode == 'watermark':
                watermark_column = self.config.dataset.incremental_load.watermark_column
                query_sql = (f"SELECT {select_clause} FROM {self.config.dataset.entity_name} "
                             f"WHERE {watermark_column} > %s "
                             f"ORDER BY {watermark_column} ASC;")
                query_params.append(last_extracted_value)
            else:
                query_sql = f"SELECT {select_clause} FROM {self.config.dataset.entity_name}"

            logging.info(f"Executing query: {query_sql} with params: {query_params}")
            cursor.execute(query_sql, tuple(query_params) if query_params else None)

            columns_from_db = [desc[0] for desc in cursor.description]
            while True:
                rows = cursor.fetchmany(self.config.dataset.fetch_size)
                if not rows:
                    break
                for row in rows:
                    record = dict(zip(columns_from_db, row))
                    if self.config.dataset.incremental_load.mode == 'watermark':
                        watermark_column = self.config.dataset.incremental_load.watermark_column
                        current_row_watermark = record.get(watermark_column)
                        if current_row_watermark is not None and (
                                isinstance(current_row_watermark, datetime) or isinstance(current_row_watermark, int)):
                            if new_max_watermark_value is None or current_row_watermark > new_max_watermark_value:
                                new_max_watermark_value = current_row_watermark

                    try:
                        pk_values = {k: record.get(k) for k in self.config.dataset.primary_keys}
                        self.producer.produce(
                            self.config.kafka_config.topic,
                            key=json.dumps(pk_values, cls=CustomJsonEncoder).encode('utf-8'),
                            value=json.dumps(record, cls=CustomJsonEncoder).encode('utf-8'),
                            callback=self._delivery_report
                        )
                        self.messages_produced += 1
                        self.producer.poll(0)
                    except BufferError:
                        logging.warning("Local producer queue is full, waiting for messages to be delivered...")
                        self.producer.poll(1)
                    except Exception as error:
                        logging.error(f"Failed to produce message for record: {record}. Error: {error}")

            if new_max_watermark_value != last_extracted_value:
                if isinstance(new_max_watermark_value, datetime):
                    return new_max_watermark_value.isoformat()
                return str(new_max_watermark_value)
            else:
                if isinstance(last_extracted_value, datetime):
                    return last_extracted_value.isoformat()
                return str(last_extracted_value)
        except Exception as error:
            raise IngestionWorkerError(f"Extraction or production failed: {error}")
        finally:
            if cursor:
                cursor.close()

    def _extract_from_file_and_produce(self) -> int:
        """
        Extracts data from a file and produces messages to Kafka.
        Returns the number of messages produced.
        """
        try:
            file_path = self.config.producer.source_config.file_path
            logging.info(f"Extracting data from file: {file_path}")

            with open(file_path, 'r') as file:
                if file_path.endswith('.json'):
                    data = json.load(file)
                elif file_path.endswith('.csv'):
                    reader = csv.DictReader(file)
                    data = list(reader)
                else:
                    raise IngestionWorkerError(f"Unsupported file type for ingestion: {file_path}", "Dataset")

                for record in data:
                    try:
                        pk_values = {k: record.get(k) for k in self.config.dataset.primary_keys}
                        self.producer.produce(
                            self.config.kafka_config.topic,
                            key=json.dumps(pk_values, cls=CustomJsonEncoder).encode('utf-8'),
                            value=json.dumps(record, cls=CustomJsonEncoder).encode('utf-8'),
                            callback=self._delivery_report
                        )
                        self.messages_produced += 1
                        self.producer.poll(0)
                    except BufferError:
                        logging.warning("Local producer queue is full, waiting for messages to be delivered...")
                        self.producer.poll(1)
                    except Exception as error:
                        logging.error(f"Failed to produce message for record: {record}. Error: {error}")

            return len(data)  # Return the number of records produced
        except FileNotFoundError:
            raise IngestionWorkerError(f"Source file not found at: {file_path}", "Dataset")
        except (json.JSONDecodeError, csv.Error) as error:
            raise IngestionWorkerError(f"Error decoding file: {error}", "Dataset")
        except Exception as error:
            raise IngestionWorkerError(f"Error extracting from file: {error}", "Dataset")

    # --- Kafka and S3 related methods ---
    def _get_kafka_producer(self) -> None:
        """Initializes the Kafka Producer with the configuration."""
        try:
            conf = {'bootstrap.servers': self.config.kafka_config.bootstrap_servers}
            self.producer = Producer(conf)
            logging.info(f"Kafka Producer initialized for brokers: {conf['bootstrap.servers']}")
        except Exception as error:
            raise IngestionWorkerError(f"Failed to initialize Kafka Producer: {error}", "Kafka")

    def _get_kafka_consumer(self, testing_mode=False) -> Consumer:
        """Initializes the Kafka Consumer with the configuration."""
        group_id = 's3_ingestion_group'
        if testing_mode:
            group_id = f"s3_test_group_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"

        conf = {
            'bootstrap.servers': self.config.kafka_config.bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        try:
            return Consumer(conf)
        except Exception as error:
            raise IngestionWorkerError(f"Failed to initialize Kafka Consumer: {error}", "Kafka")

    def _get_minio_client(self) -> None:
        """Initializes the MinIO client for S3 storage."""
        try:
            self.minio_client = Minio(
                endpoint=self.config.s3_landing_config.endpoint,
                access_key=self.config.s3_landing_config.access_key,
                secret_key=self.config.s3_landing_config.secret_key,
                secure=self.config.s3_landing_config.secure
            )
            if not self.minio_client.bucket_exists(self.config.s3_landing_config.bucket_name):
                self.minio_client.make_bucket(self.config.s3_landing_config.bucket_name)
                logging.info(f"MinIO bucket '{self.config.s3_landing_config.bucket_name}' created.")
            else:
                logging.info(f"MinIO bucket '{self.config.s3_landing_config.bucket_name}' already exists.")
        except ValueError as value_error:
            raise IngestionWorkerError(f"Invalid Minio configuration: {value_error}", "S3")
        except ConnectionError as conn_error:
            raise IngestionWorkerError(f"Connection to Minio server failed: {conn_error}", "S3")
        except Exception as error:
            raise IngestionWorkerError(f"Unexpected error initializing Minio client: {error}", "S3")

    @staticmethod
    def _get_event_date(event_date_str: Any) -> datetime:
        """Parses the event date string and returns a datetime object."""
        try:
            return datetime.fromisoformat(event_date_str) if event_date_str else datetime.now()
        except (ValueError, TypeError):
            logging.warning(f"Invalid event_date_str '{event_date_str}'. Defaulting to current datetime.")
            return datetime.now()

    def _generate_batch_object_name(self, event_date: datetime, partition_id: int, min_offset: int,
                                    max_offset: int) -> str:
        """Generates a unique object name for the batch file to be stored in S3."""
        batch_uuid = uuid.uuid4()
        file_name = f"{event_date.strftime('%Y%m%d%H%M%S')}-{partition_id}-{min_offset}-{max_offset}-{batch_uuid}.jsonl"
        return (
            f"{self.config.s3_landing_config.prefix}"
            f"year={event_date.year}/"
            f"month={event_date.month:02d}/"
            f"day={event_date.day:02d}/"
            f"{file_name}"
        )

    def _flush_buffer_to_s3(self) -> None:
        """Flushes the message buffer to S3 as a batch file."""
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
            event_date = datetime.now()

        if representative_partition is None:
            representative_partition = 0

        object_name = self._generate_batch_object_name(event_date, representative_partition, min_offset, max_offset)
        batch_content = "\n".join(batch_contents_list) + "\n"
        data_bytes = batch_content.encode('utf-8')

        try:
            self.minio_client.put_object(
                self.config.s3_landing_config.bucket_name,
                object_name,
                io.BytesIO(data_bytes),
                len(data_bytes),
                content_type="application/x-jsonlines"
            )
            logging.info(f"Successfully uploaded batch file {object_name} with {len(self.message_buffer)} records.")
            self.message_buffer = []
            self.last_flush_time = datetime.now()
        except Exception as error:
            raise IngestionWorkerError(f"Failed to upload batch file {object_name} to MinIO. Error: {error}", "S3")

    def _consume_and_load(self) -> int:
        """Consumes messages from Kafka and loads them into S3."""
        try:
            poll_timeout = min(1000.0 / 1000.0, self.config.s3_landing_config.flush_interval_seconds / 2.0)
            msg = self.consumer.poll(timeout=poll_timeout)
            if not msg:
                if (
                        datetime.now() - self.last_flush_time).total_seconds() >= self.config.s3_landing_config.flush_interval_seconds:
                    if self.message_buffer:
                        self._flush_buffer_to_s3()
                return 0

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self._flush_buffer_to_s3()
                else:
                    raise IngestionWorkerError(f"Kafka error: {msg.error()}", "Kafka")
                return 0

            record_value = msg.value().decode('utf-8')
            try:
                self.message_buffer.append((record_value, msg.partition(), msg.offset()))
                self.messages_consumed += 1
            except Exception as error:
                logging.error(
                    f"Failed to process message from Kafka: {record_value}. Error: {error}. Skipping message.")

            if len(self.message_buffer) >= self.config.s3_landing_config.flush_message_count:
                self._flush_buffer_to_s3()
            elif (
                    datetime.now() - self.last_flush_time).total_seconds() >= self.config.s3_landing_config.flush_interval_seconds:
                if self.message_buffer:
                    self._flush_buffer_to_s3()
            return 1  # Return 1 to indicate a message was consumed
        except Exception as error:
            raise IngestionWorkerError(f"An unhandled error occurred in consumer loop: {error}")

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Callback for Kafka message delivery report."""
        if err:
            raise IngestionWorkerError(f"Message delivery failed for key {msg.key()}: {err}", "Kafka")
        logging.info(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def run(self) -> None:
        logging.info(f"Starting ingestion worker for dataset: {self.config.dataset.entity_name}")

        source_type = self.config.producer.source_type
        if source_type == 'rdbms':
            self._get_db_connection()
        elif source_type == 'flat_file':
            pass
        else:
            raise IngestionWorkerError(f"Unsupported source type: {source_type}", "Config")

        self._get_kafka_producer()
        self._get_minio_client()

        last_extracted_value = self.state_manager.load_state().get('last_watermark_value',
                                                                   self.config.dataset.incremental_load.initial_watermark_value)
        if isinstance(last_extracted_value, str) and self.config.dataset.incremental_load.watermark_type == 'timestamp':
            last_extracted_value = datetime.fromisoformat(last_extracted_value)

        self.consumer = self._get_kafka_consumer()
        self.consumer.subscribe([self.config.kafka_config.topic])

        poll_interval = timedelta(seconds=self.config.s3_landing_config.flush_interval_seconds)
        self.last_check_time = datetime.now()

        try:
            # RDBMS sources are continuous, while flat files are a one-time process
            if source_type == 'rdbms':
                ingestion_timeout = timedelta(seconds=self.config.s3_landing_config.ingestion_timeout_seconds)
                while True:
                    if (datetime.now() - self.last_check_time) >= poll_interval:
                        self._get_db_connection()
                        new_watermark = self._extract_from_rdbms_and_produce(last_extracted_value)

                        if new_watermark and new_watermark != last_extracted_value:
                            self.state_manager.save_state({'last_watermark_value': new_watermark})
                            last_extracted_value = new_watermark
                            logging.info(f"New watermark saved: {new_watermark}")

                        if self.db_conn:
                            self.db_conn.close()
                            self.db_conn = None

                        self.last_check_time = datetime.now()
                        if self.producer:
                            self.producer.flush()

                    self._consume_and_load()

                    if (datetime.now() - self.last_ingestion_time) >= ingestion_timeout:
                        logging.info("Ingestion inactivity timeout reached. Shutting down worker gracefully.")
                        break
            elif source_type == 'flat_file':
                logging.info("Starting one-time file ingestion and consumption.")
                self._get_kafka_producer()
                self._get_minio_client()

                new_watermark = self._extract_from_file_and_produce()

                self.producer.flush()

                while self.messages_consumed < self.messages_produced:
                    self._consume_and_load()

                logging.info(
                    f"All {self.messages_produced} messages from file ingestion have been consumed and landed.")

                # After one-time ingestion, the worker's job is done.
            else:
                raise IngestionWorkerError(f"Unsupported source type: {source_type}", "Config")

        except Exception as error:
            raise IngestionWorkerError(f"An unhandled error occurred in the worker's run loop: {error}")
        finally:
            if self.consumer:
                self.consumer.close()
            if self.producer:
                self.producer.flush()
            if self.db_conn:
                self.db_conn.close()
