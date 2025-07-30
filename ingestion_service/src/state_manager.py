# ingestion_service/src/state_manager.py
import json
import os
import logging
from datetime import datetime
import tempfile

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class StateManagerError(Exception):
    """Custom exception for StateManager errors."""

    def __init__(self, error: str):
        super().__init__(f"State Manager failure: {error}")
        logging.error(f"State Manager failure: {error}")


class StateManager:
    """Manages the persistent state for data ingestion, specifically watermarks."""

    def __init__(self, state_file_path):
        self.state_file_path = state_file_path
        os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
        logging.info(f"StateManager initialized with state file: {self.state_file_path}")

    def load_state(self):
        """
        Loads the last extracted watermark value from the state file.
        Handles datetime conversion from ISO string to datetime object upon loading.
        Raises StateManagerError on critical load failures.
        """
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    state = json.load(f)

                    # Convert watermark string back to datetime object if it's a timestamp
                    if 'last_watermark_value' in state and isinstance(state['last_watermark_value'], str):
                        try:
                            state['last_watermark_value'] = datetime.fromisoformat(state['last_watermark_value'])
                        except ValueError:
                            # Just log but keep string if parsing fails - downstream might need to handle this
                            logging.error(
                                f"Failed to parse datetime from state file: '{state['last_watermark_value']}'. Keeping as string.")

                    logging.info(f"Loaded state: {state}")
                    return state
            except json.JSONDecodeError as error:
                raise StateManagerError(f"Error decoding state file '{self.state_file_path}': {error}")
            except IOError as error:
                raise StateManagerError(f"IOError loading state file '{self.state_file_path}': {error}")
        logging.info(f"State file '{self.state_file_path}' not found. Returning empty state.")
        return {}  # This is expected for first run.

    def save_state(self, state):
        """
        Saves the current watermark value to the state file using an atomic write pattern.
        Handles datetime conversion to ISO string before saving.
        Raises StateManagerError on critical save failures.
        """
        state_to_save = state.copy()

        if 'last_watermark_value' in state_to_save and isinstance(state_to_save['last_watermark_value'], datetime):
            state_to_save['last_watermark_value'] = state_to_save['last_watermark_value'].isoformat()

        fd, temp_filepath = tempfile.mkstemp(dir=os.path.dirname(self.state_file_path), prefix='tmp_state_')

        try:
            with os.fdopen(fd, 'w') as tmp_f:
                json.dump(state_to_save, tmp_f, indent=4)

            os.replace(temp_filepath, self.state_file_path)
            logging.info(f"Saved state: {state_to_save} to {self.state_file_path}")
        except IOError as error:
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)  # Clean up temp file
            raise StateManagerError(f"IOError saving state file '{self.state_file_path}': {error}")
        except Exception as error:
            if os.path.exists(temp_filepath):
                os.remove(temp_filepath)  # Clean up temp file
            raise StateManagerError(f"Unexpected error saving state file '{self.state_file_path}': {error}")
