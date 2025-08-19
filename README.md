# Data Analytics Platform Service MVP

This project is a Data Analytics Platform MVP designed to ingest, process, and analyze data from various sources. It uses Python to extract data from source databases and datasets, streams it through Kafka brokers, and stores it in MinIO object storage across bronze, silver, and gold buckets. Data transformation and aggregation are performed using Python and Apache Spark, with experimentation supported via Jupyter notebooks. DuckDB enables SQL-based analytics on stored data, and results can be visualized in Tableau. The platform is containerized with Docker for easy deployment and scalability.

## Ingestion Service
- Modular Python package structure for data ingestion
- Dockerfile for containerized deployment
- Configurable data contracts using YAML files
- Support for multiple data sources (e.g., JSON files)
- Pipeline configuration management
- Kafka topic viewing and management utilities
- Ingestion worker for orchestrating data extraction and loading
- State management for tracking ingestion progress
- Backup utilities for Kafka and PostgreSQL extraction
- Persistent state tracking via JSON files
- Unit and integration tests for ingestion components
- Test fixtures for reproducible test environments
- Requirements file for dependency management

    ## Requirements

  - psycopg2-binary v2.9.9, 
  - confluent-kafka v2.3.0
  - minio v7.2.16
  - PyYAML v6.0.1
  - pydantic v2.8.2
  - pytest v7.4.0
  - pytest-mock v3.12.0

    note: all dependencies are listed in `requirements.txt`

  ## Installation

    Fresh Install 🧹:
    ```bash
   make install
  ```
  Install Docker 🐳:
  ```bash
  make install-docker
    ```
    Install Docker Compose 🎼:
  ```bash
  make install-docker-compose
  ```
    ## Usage
    
    From root directory, start Docker engine:
    ```bash
    # using makefile
    make run-docker-engine
    # manual docker command
    open -a docker
    ```

    From root directory, run docker compose in your IDE if you have docker plugin installed, or run the following command in your terminal:
    ```bash
    # start using makefile
    make start-docker
    # manual docker compose command
    docker compose up -d
    .
    .
    .
    # stop using makefile
    make stop-docker
    # manual docker compose command
    docker compose down
    ```
  
    Clean up after run:
    ```bash
    # using makefile
    make clean-all
    ```
  

## Transformation Service
In Progress... 😜😜😜

