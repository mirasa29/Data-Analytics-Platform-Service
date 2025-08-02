import psycopg2
import json
import logging
from datetime import datetime
from decimal import Decimal
# isolated tests
import yaml
import os
from confluent_kafka import Producer
from ..models.pipeline_config import PipelineConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PostgresExtractorError(Exception):
    """Custom exception for PostgresExtractor errors."""

    def __init__(self, error: str):
        super().__init__(f"Postgres Extractor failure: {error}")
        logging.error(f"Postgres Extractor failure: {error}"     )


class CustomJsonEncoder(json.JSONEncoder):
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


class PostgresExtractor:
    """ PG SQL Data extractor class for incremental data load """

    def __init__(self):
        self.db_conn = None
        self.producer = None

    def _set_db_connection(self, source_config):
        """Establishes and returns a PostgreSQL database connection."""
        try:
            self.db_conn = psycopg2.connect(
                host=source_config.host,
                port=source_config.port,
                database=source_config.database,
                user=source_config.user,
                password=source_config.password
            )
            logging.info("Successfully connected to PostgreSQL.")
            return self.db_conn
        except psycopg2.Error as db_error:
            raise PostgresExtractorError(f"Error connecting to PostgreSQL: {db_error}")

    def _get_kafka_producer(self, kafka_config):
        """Initializes and returns a Kafka Producer."""
        try:
            conf = {'bootstrap.servers': kafka_config.bootstrap_servers}
            self.producer = Producer(conf)
            logging.info(f"Kafka Producer initialized for brokers: {conf['bootstrap.servers']}")
            return self.producer
        except Exception as error:
            raise PostgresExtractorError(f"Failed to initialize Kafka Producer: {error}")

    @staticmethod
    def _delivery_report(err, msg):
        if err:
            raise PostgresExtractorError(f"Message delivery failed for key {msg.key()}: {err}")
        logging.info(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    @staticmethod
    def _build_select_query(dataset_config):
        """Builds a SELECT clause from the schema in the data contract."""
        columns = [field.name for field in dataset_config.data_schema.fields]
        return ", ".join(columns)

    def extract_and_produce(self, pipeline_config: PipelineConfig,
                            last_extracted_value):  # UPDATED: takes pipeline_config
        """
        Extracts data incrementally from PostgreSQL and produces to Kafka.
        Returns the new highest watermark value found in this run.
        """
        source_config = pipeline_config.producer.source_config
        dataset_config = pipeline_config.dataset
        kafka_config = pipeline_config.kafka_config

        self.db_conn = self._set_db_connection(source_config)
        self.producer = self._get_kafka_producer(kafka_config)
        cursor = self.db_conn.cursor()

        incremental_cfg = dataset_config.incremental_load
        new_max_watermark_value = last_extracted_value
        watermark_column = None

        try:
            select_clause = self._build_select_query(dataset_config)

            query_sql = ""
            query_params = []

            if incremental_cfg.mode == 'watermark':
                watermark_column = incremental_cfg.watermark_column
                query_sql = (f"SELECT {select_clause} FROM {dataset_config.entity_name} "
                             f"WHERE {watermark_column} > %s "
                             f"ORDER BY {watermark_column} ASC;")
                query_params.append(last_extracted_value)
            else:
                query_sql = f"SELECT {select_clause} FROM {dataset_config.entity_name}"

            logging.info(f"Executing query: {query_sql} with params: {query_params}")
            # parametrize the query to prevent SQL injection
            cursor.execute(query_sql, tuple(query_params) if query_params else None)

            # list columns once
            columns_from_db = [desc[0] for desc in cursor.description]

            count = 0
            while True:
                rows = cursor.fetchmany(dataset_config.fetch_size)
                if not rows:
                    break

                for row in rows:
                    record = dict(zip(columns_from_db, row))

                    if incremental_cfg.mode == 'watermark' and watermark_column:
                        current_row_watermark = record.get(watermark_column)
                        if current_row_watermark is not None:
                            if new_max_watermark_value is None or current_row_watermark > new_max_watermark_value:
                                new_max_watermark_value = current_row_watermark

                    try:
                        pk_values = {k: record.get(k) for k in dataset_config.primary_keys}
                        self.producer.produce(
                            kafka_config.topic,
                            key=json.dumps(pk_values, cls=CustomJsonEncoder).encode('utf-8'),
                            value=json.dumps(record, cls=CustomJsonEncoder).encode('utf-8'),
                            callback=self._delivery_report
                        )
                        count += 1
                        self.producer.poll(0)
                    except BufferError:
                        logging.warning("Local producer queue is full, waiting for messages to be delivered...")
                        self.producer.poll(1)
                    except Exception as error:
                        logging.error(f"Failed to produce message for record: {record}. Error: {error}")

            logging.info(f"Finished extraction. Produced {count} messages to Kafka topic '{kafka_config.topic}'.")

            if new_max_watermark_value != last_extracted_value:
                if isinstance(new_max_watermark_value, datetime):
                    return new_max_watermark_value.isoformat()
                return str(new_max_watermark_value)
            else:
                if isinstance(last_extracted_value, datetime):
                    return last_extracted_value.isoformat()
                return str(last_extracted_value)

        except Exception as error:
            raise PostgresExtractorError(f"Extraction or production failed: {error}")
        finally:
            cursor.close()
            self.producer.flush()
            self.db_conn.close()
            logging.info("Resources released.")


# note: only for isolated testing, actual orchestration is through main_ingestion_job.py
# if __name__ == "__main__":
#     contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config',
#                                  'transactions_contract.yaml')
#
#     try:
#         with open(contract_path, 'r') as f:
#             config_dict = yaml.safe_load(f)
#         pipeline_config = PipelineConfig.model_validate(config_dict)
#
#         initial_watermark_str = pipeline_config.dataset.incremental_load.initial_watermark_value
#         watermark_type = pipeline_config.dataset.incremental_load.watermark_type
#
#         initial_watermark = None
#         if watermark_type == 'timestamp':
#             initial_watermark = datetime.fromisoformat(initial_watermark_str)
#         elif watermark_type == 'integer':
#             initial_watermark = int(initial_watermark_str)
#
#         extractor = PostgresExtractor()
#         new_watermark = extractor.extract_and_produce(pipeline_config, initial_watermark)
#         logging.info(f"Extractor finished. New watermark: {new_watermark}")
#
#     except Exception as error:
#         logging.error(f"Isolated tests run failed: {error}")
