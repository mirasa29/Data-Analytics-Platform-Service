import time
import logging
import os
import yaml
from datetime import datetime

from .postgres_extractor import PostgresExtractor, PostgresExtractorError
from .kafka_loader import KafkaS3Loader, KafkaLoaderError
from .state_manager import StateManager, StateManagerError
from ..models.pipeline_config import PipelineConfig, ValidationError

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

    STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'state', 'transactions_load_state.json')
    state_manager = StateManager(STATE_FILE)

    # 1. Load, parse, and validate config using Pydantic
    pipeline_config = None
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        pipeline_config = PipelineConfig.model_validate(config_dict)
    except FileNotFoundError:
        raise MainIngestionJobError(f"Configuration file not found at: {config_path}")
    except yaml.YAMLError as error:
        raise MainIngestionJobError(f"Error parsing configuration file '{config_path}': {error}")
    except ValidationError as error:
        raise MainIngestionJobError(f"Configuration validation failed: {error}")
    except Exception as error:
        raise MainIngestionJobError(f"Unexpected error loading config file '{config_path}': {error}")

    # 2. Determine the 'last_extracted_value' for the current run
    last_extracted_value = None
    try:
        current_state = state_manager.load_state()
        last_extracted_value = current_state.get('last_watermark_value')
        logging.info(f"Loaded last extracted watermark from state: {last_extracted_value}")
    except StateManagerError as error:
        logging.warning(f"Could not load state from file. Proceeding with initial watermark from contract. Error: {error}")

    if last_extracted_value is None:
        initial_watermark_str = pipeline_config.dataset.incremental_load.initial_watermark_value
        watermark_type = pipeline_config.dataset.incremental_load.watermark_type

        if watermark_type == 'timestamp':
            try:
                last_extracted_value = datetime.fromisoformat(initial_watermark_str)
            except ValueError:
                raise MainIngestionJobError(
                    f"Initial watermark '{initial_watermark_str}' in contract is not a valid ISO timestamp for type '{watermark_type}'.")
        elif watermark_type == 'integer':
            try:
                last_extracted_value = int(initial_watermark_str)
            except ValueError:
                raise MainIngestionJobError(
                    f"Initial watermark '{initial_watermark_str}' in contract is not a valid integer for type '{watermark_type}'.")
        else:
            raise MainIngestionJobError(f"Unsupported watermark_type '{watermark_type}' specified in contract.")

        logging.info(f"Using initial watermark from contract: {last_extracted_value}")

    extractor = PostgresExtractor()
    loader = KafkaS3Loader()

    new_watermark_value = last_extracted_value

    try:
        # 3. Run Data Extraction and Kafka Production
        logging.info("Starting PostgreSQL extraction and Kafka production...")
        # Pass pipeline_config and last_extracted_value to the method
        new_watermark_value = extractor.extract_and_produce(pipeline_config, last_extracted_value)
        logging.info(
            f"PostgreSQL extraction and Kafka production completed. New highest watermark: {new_watermark_value}")

        # 4. Save New Watermark State
        if new_watermark_value != last_extracted_value:
            state_manager.save_state({'last_watermark_value': new_watermark_value})
            logging.info(f"State saved with new watermark: {new_watermark_value}")
        else:
            logging.info("No new data processed or watermark did not advance. State remains unchanged.")

        time.sleep(5)

        # 5. Run Kafka Consumption and MinIO Loading
        logging.info("Starting Kafka consumption and MinIO loading...")
        # Pass pipeline_config to the method
        loader.load_from_kafka_to_s3(pipeline_config)
        logging.info("Kafka consumption and MinIO loading completed.")

        logging.info("Data ingestion pipeline finished successfully.")

    except PostgresExtractorError as error:
        raise MainIngestionJobError(f"Extraction/Production failed: {error}")
    except KafkaLoaderError as error:
        raise MainIngestionJobError(f"Kafka Consumption/S3 Loading failed: {error}")
    except StateManagerError as error:
        raise MainIngestionJobError(f"State Management failed: {error}")
    except Exception as error:
        raise MainIngestionJobError(f"An unexpected error occurred: {error}")
    finally:
        logging.info("Ingestion pipeline execution finished.")


if __name__ == "__main__":
    try:
        run_ingestion_pipeline()
    except MainIngestionJobError as error:
        logging.critical(f"Pipeline execution halted due to a critical error: {error}")
    except Exception as error:
        logging.critical(f"Pipeline execution halted due to an unhandled exception: {error}")
