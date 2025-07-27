import psycopg2
import json
from confluent_kafka import Producer
import yaml
import logging
from datetime import datetime
from decimal import Decimal

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class PostgresExtractorError(Exception):
    """Custom exception for PostgresExtractor errors."""
    def __init__(self, error: str):
        super().__init__(f"Postgres Extractor failure: {error}")
        logging.error(f"Postgres Extractor failure: {error}")


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
        self.kafka_config = self.config['kafka_config']  # Still top-level

        self.table_name = self.dataset_config['entity_name']
        self.primary_keys = self.dataset_config['primary_keys']  # For Kafka key
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
            logging.error(f"Error initializing Kafka Producer: {error}")
            raise

    @staticmethod
    def _delivery_report(err, msg):
        """Callback for Kafka message delivery."""
        if err is not None:
            raise PostgresExtractorError(f"Message delivery failed for key {msg.key()}: {err}")
        else:
            logging.info(f"Message delivered to topic {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

    @staticmethod
    def _convert_to_json_compatible(value, field_type):
        """Converts a value to a JSON-compatible format based on schema type."""

        if value is None:
            logging.warning(f"Value is None for field type '{field_type}'. Returning None.")
            return None

        if field_type == 'timestamp':
            if isinstance(value, datetime):
                return value.isoformat()
            return value  # Assume string already ISO formatted
        elif field_type == 'decimal':
            if isinstance(value, Decimal):
                return str(value)
            return value
        # Add more type if needed
        # For binary (e.g., UUID from Postgres), convert to string
        if isinstance(value, (bytes, bytearray)):
            return value.decode('utf-8')
        return value

    def extract_and_produce(self, last_extracted_value):
        """
        Extracts data incrementally from PostgreSQL and produces to Kafka.
        Returns the new highest watermark value found in this run.
        """

        self.db_conn = self._set_db_connection()
        self.producer = self._get_kafka_producer()
        cursor = self.db_conn.cursor()

        new_max_watermark_value = last_extracted_value  # Initialize with previous value
        incremental_cfg = self.dataset_config.get('incremental_load')

        try:
            if incremental_cfg and incremental_cfg['mode'] == 'watermark':
                watermark_column = incremental_cfg['watermark_column']
                watermark_type = incremental_cfg['watermark_type']

                # Convert last_extracted_value to correct Python type for query
                query_watermark = last_extracted_value  # Default, if it's already correct
                if watermark_type == 'timestamp':
                    if isinstance(last_extracted_value, str):
                        query_watermark = datetime.fromisoformat(last_extracted_value)
                elif watermark_type == 'integer':
                    query_watermark = int(last_extracted_value)
                # Ensure timestamp is in ISO format for SQL query eg: "2023-10-01T12:00:00"
                sql_watermark_value = query_watermark.isoformat() if isinstance(query_watermark, datetime) else str(
                    query_watermark)

                query_sql = (f"SELECT * FROM {self.table_name} WHERE {watermark_column} > '{sql_watermark_value}' "
                             f"ORDER BY {watermark_column} ASC;")
            else:  # Full load - mode: 'full'
                query_sql = f"SELECT * FROM {self.table_name}"

            logging.info(f"Executing query: {query_sql}")
            cursor.execute(query_sql)

            count = 0
            # field:types map based on contract {'invoiceno': 'string', description': 'string', ...}
            schema_type_map = {field['name']: field['type'] for field in self.schema_fields}

            while True:
                rows = cursor.fetchmany(self.fetch_size)
                if not rows:
                    break

                columns_from_db = [desc[0] for desc in cursor.description]

                # transpose rows to JSON records
                for row in rows:
                    record = {}
                    current_row_watermark = None

                    for i, col_name_from_db in enumerate(columns_from_db):
                        schema_field_type = schema_type_map.get(col_name_from_db)
                        if schema_field_type:
                            record[col_name_from_db] = self._convert_to_json_compatible(row[i], schema_field_type)
                        else:
                            record[col_name_from_db] = row[i]
                            logging.warning(f"Column '{col_name_from_db}' not found in contract schema. Storing as-is.")

                    # Update max watermark value for this run
                    if incremental_cfg and incremental_cfg['mode'] == 'watermark':
                        watermark_col_value = record.get(watermark_column)
                        if watermark_col_value is not None:
                            if watermark_type == 'timestamp':
                                current_row_watermark = datetime.fromisoformat(watermark_col_value) if isinstance(
                                    watermark_col_value, str) else watermark_col_value
                            else:  # For integer, just use as is
                                current_row_watermark = watermark_col_value

                            if current_row_watermark > new_max_watermark_value:
                                new_max_watermark_value = current_row_watermark

                    try:
                        pk_values = {k: record.get(k) for k in self.primary_keys}  # Use self.primary_keys
                        self.producer.produce(
                            self.topic,
                            key=json.dumps(pk_values).encode('utf-8'),
                            value=json.dumps(record).encode('utf-8'),
                            callback=self._delivery_report
                        )
                        count += 1
                        self.producer.poll(0)
                    except BufferError:
                        logging.warning("Local producer queue is full, waiting for messages to be delivered...")
                        self.producer.poll(1)
                    except Exception as e:
                        logging.error(f"Failed to produce message for record: {record}. Error: {e}")

            logging.info(f"Finished extraction. Produced {count} messages to Kafka topic '{self.topic}'.")

            # Return the highest watermark value from this run
            if isinstance(new_max_watermark_value, datetime):
                return new_max_watermark_value.isoformat()
            return new_max_watermark_value

        except Exception as e:
            logging.error(f"Error during extraction or production: {e}")
            raise
        finally:
            if cursor:
                cursor.close()
            if self.producer:
                self.producer.flush()
                logging.info("Kafka Producer flushed.")
            if self.db_conn:
                self.db_conn.close()
                logging.info("PostgreSQL connection closed.")


# Note: This __main__ block is for isolated testing, actual orchestration is in main_ingestion_job.py
if __name__ == "__main__":
    contract_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config',
                                 'transactions_contract.yaml')

    with open(contract_path, 'r') as f:
        config = yaml.safe_load(f)
    # UPDATED: Access initial_watermark_value via dataset_config
    initial_watermark = config['dataset']['incremental_load']['initial_watermark_value']

    extractor = PostgresExtractor(contract_path)
    new_watermark = extractor.extract_and_produce(initial_watermark)
    logging.info(f"Extractor finished. New watermark: {new_watermark}")
