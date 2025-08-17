import time
import logging
import os
import yaml
from datetime import datetime
import threading
from typing import List, Tuple

from src.state_manager import StateManager, StateManagerError
from src.ingestion_worker import IngestionWorker, IngestionWorkerError
from models.pipeline_config import PipelineConfig, ValidationError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class MainIngestionJobError(Exception):
    """Custom exception for the main ingestion job orchestration errors."""

    def __init__(self, error: str):
        super().__init__(f"Main Ingestion Job failure: {error}")
        logging.error(f"Main Ingestion Job failure: {error}")


def load_config(config_path: str) -> PipelineConfig:
    """Loads and validates a single data contract file using Pydantic models."""
    try:
        with open(config_path, 'r') as f:
            config_dict = yaml.safe_load(f)
        return PipelineConfig.model_validate(config_dict)
    except FileNotFoundError:
        raise MainIngestionJobError(f"Configuration file not found at: {config_path}")
    except yaml.YAMLError as error:
        raise MainIngestionJobError(f"Error parsing configuration file '{config_path}': {error}")
    except ValidationError as error:
        raise MainIngestionJobError(f"Configuration validation failed for '{config_path}': {error}")
    except Exception as error:
        raise MainIngestionJobError(f"Unexpected error loading config file '{config_path}': {error}")


def run_ingestion_pipeline(config_dir: str = 'config'):
    """
    The orchestrator that discovers data contracts and launches a worker for each.
    """
    logging.info("Starting ingestion pipeline orchestrator...")

    # List all contract files in the specified directory
    contracts = [os.path.join(config_dir, f) for f in os.listdir(config_dir) if f.endswith('.yaml')]

    if not contracts:
        raise MainIngestionJobError("No data contracts found in the config directory.")

    workers: List[IngestionWorker] = []
    worker_threads: List[threading.Thread] = []

    for contract_path in contracts:
        try:
            pipeline_config = load_config(contract_path)

            # Create a unique state file for each worker based on its entity name
            state_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'state',
                                      f"{pipeline_config.dataset.entity_name}_load_state.json")
            state_manager = StateManager(state_file)

            worker = IngestionWorker(pipeline_config, state_manager)
            worker_thread = threading.Thread(target=worker.run, daemon=True)  # Set as daemon to allow main to exit
            worker_threads.append(worker_thread)
            workers.append(worker)

            logging.info(f"Worker for '{pipeline_config.dataset.entity_name}' initialized.")
        except Exception as error:
            # If a worker fails to initialize, log the error and continue to the next contract
            logging.error(f"Failed to initialize worker for contract '{contract_path}': {error}. Skipping.")

    if not worker_threads:
        raise MainIngestionJobError("No workers were successfully initialized. Exiting.")

    logging.info(f"Starting {len(worker_threads)} ingestion workers...")
    for thread in worker_threads:
        thread.start()

    # The orchestrator will now run in an infinite loop, monitoring the workers
    try:
        while True:
            # Check if any workers have failed (optional for MVP, as exceptions are already logged)
            # You could add logic here to re-start a worker or send an alert
            alive_workers = [thread.is_alive() for thread in worker_threads]
            if not any(alive_workers):
                logging.info("All ingestion workers have stopped. Exiting orchestrator.")
                break
            time.sleep(10)
    except KeyboardInterrupt:
        logging.info("Orchestrator received stop signal. Shutting down...")
    finally:
        logging.info("Orchestrator exiting.")


if __name__ == "__main__":
    try:
        run_ingestion_pipeline()
    except MainIngestionJobError as error:
        logging.critical(f"Pipeline execution halted due to a critical error: {error}")
    except Exception as error:
        logging.critical(f"Pipeline execution halted due to an unhandled exception: {error}")