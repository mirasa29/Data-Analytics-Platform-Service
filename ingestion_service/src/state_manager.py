import json
import os
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class StateManager:
    """Manages the persistent state for data ingestion, specifically watermarks."""

    def __init__(self, state_file_path):
        self.state_file_path = state_file_path
        # Ensure the directory for the state file exists
        os.makedirs(os.path.dirname(state_file_path), exist_ok=True)
        logging.info(f"StateManager initialized with state file: {self.state_file_path}")

    def load_state(self):
        """
        Loads the last extracted watermark value from the state file.
        Handles datetime conversion from ISO string to datetime object upon loading.
        """
        if os.path.exists(self.state_file_path):
            try:
                with open(self.state_file_path, 'r') as f:
                    state = json.load(f)

                    # Convert watermark string back to datetime object if it's a timestamp
                    if 'last_watermark_value' in state and isinstance(state['last_watermark_value'], str):
                        try:
                            # Assuming ISO format for timestamp watermarks
                            state['last_watermark_value'] = datetime.fromisoformat(state['last_watermark_value'])
                        except ValueError:
                            logging.error(
                                f"Failed to parse datetime from state file: {state['last_watermark_value']}. Keeping as string.")

                    logging.info(f"Loaded state: {state}")
                    return state
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding state file '{self.state_file_path}': {e}. Returning empty state.")
                return {}
            except IOError as e:
                logging.error(f"IOError loading state file '{self.state_file_path}': {e}. Returning empty state.")
                return {}
        logging.info(f"State file '{self.state_file_path}' not found. Returning empty state.")
        return {}

    def save_state(self, state):
        """
        Saves the current watermark value to the state file.
        Handles datetime conversion to ISO string before saving.
        """
        # Ensure datetime objects are converted to ISO string for JSON serialization
        if 'last_watermark_value' in state and isinstance(state['last_watermark_value'], datetime):
            state['last_watermark_value'] = state['last_watermark_value'].isoformat()

        try:
            with open(self.state_file_path, 'w') as f:
                json.dump(state, f, indent=4)
                logging.info(f"Saved state: {state} to {self.state_file_path}")
        except IOError as e:
            logging.error(f"Error saving state file '{self.state_file_path}': {e}")
