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

---

## Installation

---