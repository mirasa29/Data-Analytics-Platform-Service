import pytest
from unittest.mock import MagicMock
import os
from confluent_kafka import Producer, Consumer
from minio import Minio
from ingestion_service.src.init_design_backup.postgres_extractor import PostgresExtractor
from ingestion_service.src.init_design_backup.kafka_loader import KafkaS3Loader
from ingestion_service.models.pipeline_config import PipelineConfig
from ingestion_service.src.state_manager import StateManager


@pytest.fixture
def mock_pipeline_config() -> PipelineConfig:
    """Provides a mocked PipelineConfig object for tests."""
    config_dict = {
        'producer': {
            'name': 'test_producer',
            'source_system': 'test_system',
            'description': 'Test Data',
            'source_type': 'rdbms',
            'source_config': {'db_type': 'postgresql', 'host': 'test_db', 'port': 5432, 'database': 'test_db', 'user': 'test_user', 'password': 'test_password'}
        },
        'dataset': {
            'entity_name': 'test_table',
            'version': '1.0',
            'primary_keys': ['id'],
            'fetch_size': 2,
            'incremental_load': {
                'mode': 'watermark',
                'watermark_column': 'updated_at',
                'watermark_type': 'timestamp',
                'initial_watermark_value': '2023-01-01T00:00:00.000000'
            },
            'data_schema': {'fields': [{'name': 'id', 'type': 'integer'}, {'name': 'name', 'type': 'string'}, {'name': 'updated_at', 'type': 'timestamp'}]}
        },
        'kafka_config': {'bootstrap_servers': 'test_kafka:9092', 'topic': 'test_topic'},
        's3_landing_config': {
            'bucket_name': 'test_bucket', 'prefix': 'bronze/test_app/test_entity/', 'data_format': 'jsonl',
            'access_key': 'test_key', 'secret_key': 'test_secret', 'endpoint': 'test_minio:9000',
            'secure': False, 'flush_message_count': 2, 'flush_interval_seconds': 1
        }
    }
    return PipelineConfig.model_validate(config_dict)


@pytest.fixture
def loader_instance():
    """Provides a KafkaS3Loader instance."""
    return KafkaS3Loader()


@pytest.fixture
def state_manager_instance(temp_state_dir):
    """Provides a StateManager instance for testing, with a clean state file."""
    test_state_file = os.path.join(temp_state_dir, "test_state.json")
    return StateManager(test_state_file)


@pytest.fixture
def temp_state_dir(tmp_path):
    """Provides a temporary directory for state files for each tests."""
    state_dir = tmp_path / "test_state"
    state_dir.mkdir()
    return state_dir


@pytest.fixture
def extractor_instance():
    """Provides a PostgresExtractor instance."""
    return PostgresExtractor()


@pytest.fixture
def mock_db_conn_and_cursor():
    """Mocks a psycopg2 database connection and cursor."""
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor


@pytest.fixture
def mock_kafka_producer():
    """Mocks a Confluent Kafka Producer."""
    return MagicMock(spec=Producer)


@pytest.fixture
def mock_kafka_consumer():
    """Mocks a Confluent Kafka Consumer."""
    return MagicMock(spec=Consumer)


@pytest.fixture
def mock_minio_client():
    """Mocks a MinIO client."""
    return MagicMock(spec=Minio)
