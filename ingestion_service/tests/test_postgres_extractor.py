# ingestion_service/tests/test_postgres_extractor.py
import pytest
import psycopg2
import json
from confluent_kafka import Producer
from datetime import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

from ingestion_service.src.postgres_extractor import PostgresExtractor, PostgresExtractorError, CustomJsonEncoder
from ingestion_service.models.pipeline_config import PipelineConfig


def test_set_db_connection_success(extractor_instance, mock_pipeline_config, mock_db_conn_and_cursor):
    """Test successful database connection."""
    mock_conn, _ = mock_db_conn_and_cursor
    with patch('psycopg2.connect', return_value=mock_conn):
        conn = extractor_instance._set_db_connection(mock_pipeline_config.producer.source_config)
        assert conn is mock_conn
        psycopg2.connect.assert_called_once_with(
            host='test_db', port=5432, database='test_db', user='test_user', password='test_password'
        )


# Does finish - might need to fix mocks
def test_extract_and_produce_incremental_load_with_data(extractor_instance, mock_pipeline_config,
                                                        mock_db_conn_and_cursor, mock_kafka_producer):
    """Test incremental load with new data, ensuring correct production and watermark update."""
    mock_conn, mock_cursor = mock_db_conn_and_cursor
    # Patch psycopg2.connect() to prevent a real connection attempt
    with patch('psycopg2.connect', return_value=mock_conn):
        extractor_instance.db_conn = mock_conn
        extractor_instance.producer = mock_kafka_producer

        last_watermark_dt = datetime(2023, 1, 1, 0, 0, 0)

        mock_data = [
            (1, "item_A", datetime(2023, 1, 1, 10, 0, 0)),
            (2, "item_B", datetime(2023, 1, 1, 11, 0, 0)),
            (3, "item_C", datetime(2023, 1, 1, 12, 0, 0))
        ]
        mock_cursor.description = [('id',), ('name',), ('updated_at',)]
        extractor_instance.fetch_size = 2  # Test with fetch_size=2
        mock_cursor.fetchmany.side_effect = [
            mock_data[:extractor_instance.fetch_size],
            mock_data[extractor_instance.fetch_size:],
            []
        ]

        new_watermark_result = extractor_instance.extract_and_produce(mock_pipeline_config, last_watermark_dt)

        assert mock_kafka_producer.produce.call_count == len(mock_data)
        assert new_watermark_result == datetime(2023, 1, 1, 12, 0, 0).isoformat()

        expected_query = "SELECT id, name, updated_at FROM test_table WHERE updated_at > %s ORDER BY updated_at ASC;"
        mock_cursor.execute.assert_called_once_with(expected_query, (last_watermark_dt,))

        expected_producer_calls = [
            call('test_topic', key=json.dumps({'id': 1}).encode('utf-8'),
                 value=json.dumps({'id': 1, 'name': 'item_A', 'updated_at': datetime(2023, 1, 1, 10, 0, 0)},
                                  cls=CustomJsonEncoder).encode('utf-8'), callback=extractor_instance._delivery_report),
            call('test_topic', key=json.dumps({'id': 2}).encode('utf-8'),
                 value=json.dumps({'id': 2, 'name': 'item_B', 'updated_at': datetime(2023, 1, 1, 11, 0, 0)},
                                  cls=CustomJsonEncoder).encode('utf-8'), callback=extractor_instance._delivery_report),
            call('test_topic', key=json.dumps({'id': 3}).encode('utf-8'),
                 value=json.dumps({'id': 3, 'name': 'item_C', 'updated_at': datetime(2023, 1, 1, 12, 0, 0)},
                                  cls=CustomJsonEncoder).encode('utf-8'), callback=extractor_instance._delivery_report),
        ]
        mock_kafka_producer.produce.assert_has_calls(expected_producer_calls, any_order=False)


def test_extract_and_produce_database_error(extractor_instance, mock_pipeline_config, mock_db_conn_and_cursor):
    """Test handling of a database connection error during data fetching."""
    mock_conn, mock_cursor = mock_db_conn_and_cursor
    mock_cursor.execute.side_effect = psycopg2.Error("DB query error")

    with patch('psycopg2.connect', return_value=mock_conn):
        extractor_instance.db_conn = mock_conn
        extractor_instance.producer = MagicMock()
        with pytest.raises(PostgresExtractorError) as excinfo:
            extractor_instance.extract_and_produce(mock_pipeline_config, datetime(2023, 1, 1, 0, 0, 0))

