import threading
import time
import logging
import os
import yaml  # Added for loading contract to get initial watermark
from postgres_extractor import PostgresExtractor
from kafka_loader import KafkaS3Loader
from state_manager import StateManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def run_ingestion_pipeline(config_path='config/transactions_contract.yaml'):
    """Orchestrates the data extraction and loading process with state management."""
    logging.info("Starting data ingestion pipeline...")

    STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'state', 'transactions_load_state.json')
    state_manager = StateManager(STATE_FILE)

    # 1. Load Last Extracted Value
    current_state = state_manager.load_state()

    # Load contract to get initial watermark if no state exists
    with open(config_path, 'r') as f:
        contract_config = yaml.safe_load(f)
    # UPDATED: Access initial_watermark_value via dataset config
    initial_contract_watermark = contract_config['dataset']['incremental_load']['initial_watermark_value']

    # Use value from state file if present, else use initial from contract
    last_extracted_value = current_state.get('last_watermark_value', initial_contract_watermark)
    logging.info(f"Loaded last extracted watermark value: {last_extracted_value}")

    # Initialize extractor and loader
    extractor = PostgresExtractor(config_path)
    loader = KafkaS3Loader(config_path)

    try:
        # 2. Run Extraction and Production
        logging.info("Running PostgreSQL extraction and Kafka production...")
        new_watermark_value = extractor.extract_and_produce(last_extracted_value)
        logging.info(
            f"PostgreSQL extraction and Kafka production completed. New highest watermark: {new_watermark_value}")

        # 3. Save New Watermark State
        # Only update state if extraction was successful AND new data was processed
        if new_watermark_value and new_watermark_value != last_extracted_value:
            state_manager.save_state({'last_watermark_value': new_watermark_value})
        else:
            logging.info("No new data processed or watermark not updated. State remains unchanged.")

        # Give Kafka a moment to process internally before consumption (adjust as needed)
        time.sleep(5)

        # 4. Run Kafka Consumption and MinIO Loading
        logging.info("Running Kafka consumption and MinIO loading...")
        loader.load_from_kafka_to_s3()
        logging.info("Kafka consumption and MinIO loading completed.")

        logging.info("Data ingestion pipeline finished successfully.")
    except Exception as e:
        logging.error(f"Data ingestion pipeline failed: {e}")
    finally:
        logging.info("Ingestion pipeline execution finished.")


if __name__ == "__main__":
    run_ingestion_pipeline()
