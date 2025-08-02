import pytest
import json
import os
import tempfile
from datetime import datetime
from unittest.mock import patch, mock_open
from ingestion_service.src.state_manager import StateManager, StateManagerError


def test_load_state_file_not_found(state_manager_instance, caplog):
    """Test loading state when the file does not exist (expected for first run)."""
    assert state_manager_instance.load_state() == {}
    assert "State file" in caplog.text and "not found" in caplog.text


def test_load_state_empty_file_raises_error(state_manager_instance, temp_state_dir):
    """Test loading state from an empty file, which is invalid JSON."""
    test_file = os.path.join(temp_state_dir, "test_state.json")
    with open(test_file, 'w') as f:
        f.write('')
    with pytest.raises(StateManagerError) as excinfo:
        state_manager_instance.load_state()
    assert "Error decoding state file" in str(excinfo.value)


def test_load_state_corrupted_json_raises_error(state_manager_instance, temp_state_dir):
    """Test loading state from a file with invalid JSON content."""
    test_file = os.path.join(temp_state_dir, "test_state.json")
    with open(test_file, 'w') as f:
        f.write('{"last_watermark_value": "invalid_json')
    with pytest.raises(StateManagerError) as excinfo:
        state_manager_instance.load_state()
    assert "Error decoding state file" in str(excinfo.value)


def test_load_state_success_timestamp(state_manager_instance, temp_state_dir):
    """Test successful loading of state with a timestamp watermark."""
    test_file = os.path.join(temp_state_dir, "test_state.json")
    test_timestamp_str = "2023-01-01T12:00:00.000000"
    expected_timestamp_dt = datetime.fromisoformat(test_timestamp_str)

    with open(test_file, 'w') as f:
        json.dump({"last_watermark_value": test_timestamp_str}, f)

    loaded_state = state_manager_instance.load_state()
    assert loaded_state == {"last_watermark_value": expected_timestamp_dt}
    assert isinstance(loaded_state["last_watermark_value"], datetime)


def test_load_state_success_integer(state_manager_instance, temp_state_dir):
    """Test successful loading of state with an integer watermark."""
    test_file = os.path.join(temp_state_dir, "test_state.json")
    test_integer_value = 12345

    with open(test_file, 'w') as f:
        json.dump({"last_watermark_value": test_integer_value}, f)

    loaded_state = state_manager_instance.load_state()
    assert loaded_state == {"last_watermark_value": test_integer_value}
    assert isinstance(loaded_state["last_watermark_value"], int)


def test_save_state_success_timestamp(state_manager_instance):
    """Test successful saving of state with a timestamp watermark using atomic write."""
    test_timestamp_dt = datetime(2023, 1, 2, 10, 30, 0)
    state_to_save = {"last_watermark_value": test_timestamp_dt, "other_key": "value"}

    state_manager_instance.save_state(state_to_save)

    test_file = state_manager_instance.state_file_path
    assert os.path.exists(test_file)
    with open(test_file, 'r') as f:
        saved_data = json.load(f)
    assert saved_data == {"last_watermark_value": "2023-01-02T10:30:00", "other_key": "value"}


def test_save_state_io_error_raises_error(state_manager_instance, mocker):
    """Test saving state when an IOError occurs during file write."""
    mocker.patch('os.fdopen', side_effect=IOError("Permission denied"))
    mocker.patch('os.remove')

    test_state = {"last_watermark_value": datetime.now()}
    with pytest.raises(StateManagerError) as excinfo:
        state_manager_instance.save_state(test_state)
    assert "IOError saving state file" in str(excinfo.value)

    mocker.patch('os.path.exists', return_value=True)
    os.remove.assert_called_once()
