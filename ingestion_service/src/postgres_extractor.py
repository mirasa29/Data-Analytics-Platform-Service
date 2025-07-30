import psycopg2
import json
from confluent_kafka import Producer
import yaml
import logging
from datetime import datetime
from decimal import Decimal
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PostgresExtractorError(Exception):
    """Custom exception for PostgresExtractor errors."""
    def __init__(self, error: str):
        super().__init__(f"Postgres Extractor failure: {error}")
        logging.error(f"Postgres Extractor failure: {error}")


class CustomJsonEncoder(json.JSONEncoder):
    """ Customised JSON Encoder for data serialization
    (for MVP: scope only handles decimal, datetime) 
    """
    
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return str(obj)
        # Handle UUIDs or other binary types that psycopg2 might return as 'bytes'
        if isinstance(obj, (bytes, bytearray)):
            try:
                # return if it's actually a text
                return obj.decode('utf-8')
            except UnicodeDecodeError:
                # Fallback if it's actual binary data, might need base64 encoding or different handling
                return obj.hex()  # Or base64.b64encode(obj).decode('utf-8')
        return json.JSONEncoder.default(self, obj)


class PostgresExtractor:
    """ PG SQL Data extractor class for incremental data load """

    def __init__(self, config_path):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)

        # TODO:
        #  make class stateless by creating pydantic
        #  models and passing them as arguments
        #  perform validation on pydantic models

        self.producer_config = self.config['producer']
        self.dataset_config = self.config['dataset']

        self.source_config = self.producer_config['source_config']
        self.schema_fields = self.dataset_config['schema']['fields']
        self.kafka_config = self.config['kafka_config']  

        self.table_name = self.dataset_config['entity_name']
        self.primary_keys = self.dataset_config['primary_keys']  
        self.fetch_size = self.dataset_config.get('fetch_size', 10000)
        self.topic = self.kafka_config['topic']

        self.db_conn = None
        self.producer = None

    def _set_db_connection(self):
        """Establishes and returns a PostgreSQL database connection."""
        try:
            connection = psycopg2.connect(**self.source_config)
            logging.info("Successfully connected to PostgreSQL.")
            return connection
        except ValueError as value_error:
            raise PostgresExtractorError(f"Invalid configuration for PostgreSQL connection: {value_error}")
        except psycopg2.Error as db_error:
            raise PostgresExtractorError(f"Error connecting to PostgreSQL: {db_error}")

    def _get_kafka_producer(self):
        """Initializes and returns a Kafka Producer."""
        try:
            conf = {'bootstrap.servers': self.kafka_config['bootstrap_servers']}
            producer = Producer(conf)
            logging.info(f"Kafka Producer initialized for brokers: {conf['bootstrap.servers']}")
            return producer
        except Exception as error:
            raise PostgresExtractorError(f"Failed to initialize Kafka Producer: {error}")

    @staticmethod
    def _delivery_report(err, msg):
        """Callback for Kafka message delivery."""
        if err:
            raise PostgresExtractorError(f"Message delivery failed for key {msg.key()}: {err}")
        logging.info(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    def extract_and_produce(self, last_extracted_value):
        """
        Extracts data incrementally from PostgreSQL and produces to Kafka.
        Returns the new highest watermark value found in this run.
        """
        self.db_conn = self._set_db_connection()
        self.producer = self._get_kafka_producer()
        cursor = self.db_conn.cursor()

        incremental_cfg = self.dataset_config.get('incremental_load')

        # Pre-process watermark value once
        watermark_column = None
        watermark_type = None
        new_max_watermark_value = None

        if incremental_cfg and incremental_cfg['mode'] == 'watermark':
            watermark_column = incremental_cfg['watermark_column']
            watermark_type = incremental_cfg['watermark_type']

            # Ensure last_extracted_value is correctly typed for comparison
            if watermark_type == 'timestamp':
                # Convert string watermark to datetime object if it's not already
                if isinstance(last_extracted_value, str):
                    try:
                        last_extracted_value = datetime.fromisoformat(last_extracted_value)
                    except ValueError:
                        raise PostgresExtractorError(
                            f"Invalid watermark value format: {last_extracted_value}. Extraction aborted.")
                elif not isinstance(last_extracted_value, datetime):
                    raise PostgresExtractorError(
                        f"Unexpected type for watermark: {type(last_extracted_value)}. Extraction aborted.")
            elif watermark_type == 'integer':
                try:
                    last_extracted_value = int(last_extracted_value)
                except ValueError:
                    raise PostgresExtractorError(
                        f"Invalid integer watermark value: {last_extracted_value}. Extraction aborted.")

            new_max_watermark_value = last_extracted_value  # Start tracking from here

        try:
            query_sql = ""
            if incremental_cfg and incremental_cfg['mode'] == 'watermark':
                # Convert datetime object to ISO string for SQL query if needed
                sql_watermark_param = (last_extracted_value.isoformat()
                                       if isinstance(last_extracted_value, datetime)
                                       else str(last_extracted_value))

                query_sql = (f"SELECT * FROM {self.table_name} "
                             f"WHERE {watermark_column} > '{sql_watermark_param}' "
                             f"ORDER BY {watermark_column} ASC;")
            else:
                query_sql = f"SELECT * FROM {self.table_name}"

            logging.info(f"Executing query: {query_sql}")
            cursor.execute(query_sql)

            # Get column names once
            columns_from_db = [desc[0] for desc in cursor.description]

            count = 0
            while True:
                rows = cursor.fetchmany(self.fetch_size)
                if not rows:
                    break

                for row in rows:
                    # Create dictionary using columns_from_db and current row data
                    record = dict(zip(columns_from_db, row))

                    # Direct comparison for watermark
                    # skipping fromisoformat as psycopg2 returns datetime objects for TIMESTAMP columns
                    if incremental_cfg and incremental_cfg['mode'] == 'watermark' and watermark_column:
                        current_row_watermark = record.get(watermark_column)
                        # Ensure current_row_watermark is not None before comparison
                        if current_row_watermark is not None:
                            if new_max_watermark_value is None or current_row_watermark > new_max_watermark_value:
                                new_max_watermark_value = current_row_watermark

                    try:
                        pk_values = {k: record.get(k) for k in self.primary_keys}
                        # Use CustomJsonEncoder for efficient serialization ---
                        self.producer.produce(
                            self.topic,
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

            logging.info(f"Finished extraction. Produced {count} messages to Kafka topic '{self.topic}'.")

            # Return the highest watermark value from this run, converted to ISO string if datetime
            if isinstance(new_max_watermark_value, datetime):
                return new_max_watermark_value.isoformat()
            # If new_max_watermark_value remained None (e.g., no incremental load or no data)
            # return original last_extracted_value as string for consistency in state file
            return new_max_watermark_value if new_max_watermark_value is not None else last_extracted_value

        except Exception as error:
            logging.error(f"Error during data extraction or production: {error}")
            raise
        finally:
            cursor.close()
            self.producer.flush()
            self.db_conn.close()
            logging.info("Resources released.")


# note: only for isolated testing, actual orchestration is through main_ingestion_job.py
# if __name__ == "__main__":
#     contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'transactions_contract.yaml')
#
#     with open(contract_path, 'r') as f:
#         config = yaml.safe_load(f)
#     initial_watermark = config['dataset']['incremental_load']['initial_watermark_value']
#
#     extractor = PostgresExtractor(contract_path)
#     new_watermark = extractor.extract_and_produce(initial_watermark)
#     logging.info(f"Extractor finished. New watermark: {new_watermark}")
