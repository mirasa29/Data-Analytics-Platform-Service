import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
import confluent_kafka

from ingestion_service.src.init_design_backup.kafka_loader import KafkaLoaderError


# Failing - need to mock Minio client properly


def test_load_from_kafka_to_s3_message_count_flush(loader_instance, mock_pipeline_config, mock_kafka_consumer,
                                                   mock_minio_client, mocker):
    """Test that a flush is triggered by message count."""
    loader_instance.consumer = mock_kafka_consumer
    with patch('minio.Minio', return_value=mock_minio_client):
        loader_instance.minio_client = loader_instance.get_minio_client(mock_pipeline_config.s3_landing_config)

    mock_msg_value = b'{"id":1, "invoicedate":"2023-01-01T00:00:00"}'
    mock_msg_list = [
        MagicMock(value=mock_msg_value, partition=0, offset=i, error=lambda: None) for i in range(2)
    ]
    loader_instance.consumer.poll.side_effect = mock_msg_list + [
        MagicMock(error=lambda: confluent_kafka.KafkaError._PARTITION_EOF)]

    mocker.patch.object(loader_instance, '_flush_buffer_to_s3')

    loader_instance.load_from_kafka_to_s3(mock_pipeline_config, num_messages_to_process=3, testing_mode=True)

    assert loader_instance.flush_buffer_to_s3.call_count == 2
    assert loader_instance.consumer.close.assert_called_once()


def test_load_from_kafka_to_s3_interval_flush(loader_instance, mock_pipeline_config, mock_kafka_consumer,
                                             mock_minio_client, mocker):
    """Test that a flush is triggered by time interval."""
    loader_instance.consumer = mock_kafka_consumer
    with patch('minio.Minio', return_value=mock_minio_client):
        loader_instance.minio_client = loader_instance.get_minio_client(mock_pipeline_config.s3_landing_config)

    mock_msg_value = b'{"id":1, "invoicedate":"2023-01-01T00:00:00"}'
    loader_instance.consumer.poll.side_effect = [
        MagicMock(value=mock_msg_value, partition=0, offset=1, error=lambda: None),
        None,
        None,
        MagicMock(error=lambda: confluent_kafka.KafkaError._PARTITION_EOF)
    ]

    mocker.patch.object(loader_instance, '_flush_buffer_to_s3')

    mock_now_patch = patch('src.kafka_loader.datetime')
    mock_datetime = mock_now_patch.start()
    mock_datetime.now.side_effect = [
        datetime(2023, 1, 1, 0, 0, 0),
        datetime(2023, 1, 1, 0, 0, 1),
        datetime(2023, 1, 1, 0, 0, 2),
        datetime(2023, 1, 1, 0, 0, 11),
        datetime(2023, 1, 1, 0, 0, 12)
    ]
    mock_datetime.fromisoformat = datetime.fromisoformat

    loader_instance.flush_message_count = 100
    loader_instance.flush_interval_seconds = 10
    loader_instance.load_from_kafka_to_s3(mock_pipeline_config, num_messages_to_check=3, testing_mode=True)

    assert loader_instance.flush_buffer_to_s3.call_count == 2
    assert loader_instance.consumer.close.assert_called_once()
    mock_now_patch.stop()


def test_load_from_kafka_to_s3_kafka_error(loader_instance, mock_pipeline_config, mock_kafka_consumer,
                                         mock_minio_client):
    """Test that a Kafka error during poll raises KafkaLoaderError."""
    loader_instance.consumer = mock_kafka_consumer
    with patch('minio.Minio', return_value=mock_minio_client):
        loader_instance.minio_client = loader_instance.get_minio_client(mock_pipeline_config.s3_landing_config)

    mock_msg_error = MagicMock()
    mock_msg_error.error.return_value = confluent_kafka.KafkaError(confluent_kafka.KafkaError._FAIL)
    loader_instance.consumer.poll.return_value = mock_msg_error

    with pytest.raises(KafkaLoaderError) as excinfo:
        loader_instance.load_from_kafka_to_s3(mock_pipeline_config, num_messages_to_check=1)
    assert "Kafka error" in str(excinfo.value)
    loader_instance.consumer.close.assert_called_once()


def test_flush_buffer_to_s3_success(loader_instance, mock_pipeline_config, mocker):
    """Test successful flushing of buffer to S3 with correct content and naming."""
    with patch('minio.Minio', return_value=mock_minio_client):
        loader_instance.minio_client = loader_instance.get_minio_client(mock_pipeline_config.s3_landing_config)

    mock_record_value1 = '{"id":1, "invoicedate":"2023-07-25T10:00:00"}'
    mock_record_value2 = '{"id":2, "invoicedate":"2023-07-25T10:01:00"}'
    loader_instance.message_buffer = [
        (mock_record_value1, 0, 100),
        (mock_record_value2, 0, 101)
    ]

    mock_now = datetime(2023, 7, 25, 10, 30, 1)
    mocker.patch('src.kafka_loader.datetime.now', return_value=mock_now)
    mocker.patch('src.kafka_loader.datetime.fromisoformat', side_effect=datetime.fromisoformat)
    mocker.patch('src.kafka_loader.uuid.uuid4', return_value=MagicMock(hex='flush_uuid_hex'))

    loader_instance.flush_buffer_to_s3(mock_pipeline_config.s3_landing_config)

    assert not loader_instance.message_buffer
    assert loader_instance.last_flush_time == mock_now

    expected_content = mock_record_value1 + "\n" + mock_record_value2 + "\n"
    loader_instance.minio_client.put_object.assert_called_once()

    call_args, call_kwargs = loader_instance.minio_client.put_object.call_args
    assert call_args[0] == 'test_bucket'
    assert call_args[1].startswith('bronze/test_app/test_entity/year=2023/month=07/day=25/')
    assert '20230725103001-0-100-101-flush_uuid_hex.jsonl' in call_args[1]
    assert call_args[2].getvalue() == expected_content.encode('utf-8')
    assert call_kwargs['content_type'] == "application/x-jsonlines"
