import time
import logging
import os
import yaml
from datetime import datetime
from postgres_extractor import PostgresExtractor, PostgresExtractorError
from kafka_loader import KafkaS3Loader, KafkaLoaderError
from state_manager import StateManager, StateManagerError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MainIngestionJobError(Exception):
    """Custom exception for the main ingestion job orchestration errors."""

    def __init__(self, error: str):
        super().__init__(f"Main Ingestion Job failure: {error}")
        logging.error(f"Main Ingestion Job failure: {error}")


def run_ingestion_pipeline(config_path='config/transactions_contract.yaml'):
    """
    Orchestrates the data extraction, production to Kafka, and loading to S3,
    with state management for incremental loads.
    """
    logging.info("Starting data ingestion pipeline...")

    # Define state file path relative to the project root or a known location
    STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'state', 'transactions_load_state.json')
    state_manager = StateManager(STATE_FILE)

    # Load contract configuration for initial watermark and component initialization
    try:
        with open(config_path, 'r') as f:
            contract_config = yaml.safe_load(f)
    except FileNotFoundError:
        raise MainIngestionJobError(f"Configuration file not found at: {config_path}")
    except yaml.YAMLError as error:
        raise MainIngestionJobError(f"Error parsing configuration file '{config_path}': {error}")
    except Exception as error:
        raise MainIngestionJobError(f"Unexpected error loading config file '{config_path}': {error}")

    # 1. Determine the 'last_extracted_value' for the current run
    last_extracted_value = None
    try:
        current_state = state_manager.load_state()
        # this should be datetime object if timestamp from previous successful run
        last_extracted_value = current_state.get('last_watermark_value')
        logging.info(f"Loaded last extracted watermark from state: {last_extracted_value}")
    except StateManagerError as e:
        logging.warning(f"Could not load state from file. Proceeding with initial watermark from contract. Error: {e}")
        # If state loading fails, default back to initial value from contract

    # If state file was empty, missing, or loading failed, use the initial value from the contract
    if not last_extracted_value:
        initial_contract_watermark_str = contract_config['dataset']['incremental_load']['initial_watermark_value']
        watermark_type = contract_config['dataset']['incremental_load']['watermark_type']

        if watermark_type == 'timestamp':
            try:
                # Convert initial contract watermark string to datetime object
                last_extracted_value = datetime.fromisoformat(initial_contract_watermark_str)
            except ValueError:
                raise MainIngestionJobError(
                    f"Initial watermark '{initial_contract_watermark_str}' in contract is not a valid ISO timestamp for type '{watermark_type}'.")
        elif watermark_type == 'integer':
            try:
                last_extracted_value = int(initial_contract_watermark_str)
            except ValueError:
                raise MainIngestionJobError(
                    f"Initial watermark '{initial_contract_watermark_str}' in contract is not a valid integer for type '{watermark_type}'.")
        else:
            raise MainIngestionJobError(f"Unsupported watermark_type '{watermark_type}' specified in contract.")

        logging.info(f"Using initial watermark from contract: {last_extracted_value}")

    # Initialize pipeline components
    extractor = PostgresExtractor(config_path)
    loader = KafkaS3Loader(config_path)

    # Variable to hold the new highest watermark after extraction
    new_watermark_value = last_extracted_value

    try:
        # 2. Run Data Extraction and Kafka Production
        logging.info("Starting PostgreSQL extraction and Kafka production...")
        # The extractor will return the highest watermark found in this run
        new_watermark_value = extractor.extract_and_produce(last_extracted_value)
        logging.info(
            f"PostgreSQL extraction and Kafka production completed. New highest watermark: {new_watermark_value}")

        # 3. Save New Watermark State
        # Only update state if extraction was successful AND the watermark value has progressed
        if new_watermark_value != last_extracted_value:
            state_manager.save_state({'last_watermark_value': new_watermark_value})
            logging.info(f"State saved with new watermark: {new_watermark_value}")
        else:
            logging.info("No new data processed or watermark did not advance. State remains unchanged.")

        # Kafka a brief buffering before consumption starts - lucky number 8 ;)
        time.sleep(8)

        # 4. Run Kafka Consumption and MinIO Loading
        logging.info("Starting Kafka consumption and MinIO loading...")

        loader.load_from_kafka_to_s3()
        logging.info("Kafka consumption and MinIO loading completed.")

        logging.info("Data ingestion pipeline finished successfully.")

    except (PostgresExtractorError, KafkaLoaderError, StateManagerError) as error:
        raise MainIngestionJobError(f"Pipeline component failed: {error}")
    except Exception as error:
        raise MainIngestionJobError(f"Unexpected error during pipeline execution: {error}")
    finally:
        logging.info("Ingestion pipeline execution finished.")


if __name__ == "__main__":
    try:
        run_ingestion_pipeline()
    except MainIngestionJobError as error:
        logging.critical(f"Pipeline execution halted due to a critical error: {error}")
    except Exception as error:
        logging.critical(f"Pipeline execution halted due to an unhandled exception: {error}")
