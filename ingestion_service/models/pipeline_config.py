# ingestion_service/models/pipeline_config.py
from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal
import os


# model to represent the fields in the schema
class FieldSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True


# model for the entire schema
class Schema(BaseModel):
    fields: List[FieldSchema]


# model for the source database config
class SourceConfig(BaseModel):
    db_type: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    user: Optional[str] = None
    password: Optional[str] = None
    file_path: Optional[str] = None
    file_type: Optional[str] = None
    delimiter: Optional[str] = None

    @field_validator('db_type')
    @classmethod
    def validate_db_type(cls, value, info):
        if info.data.get('source_type') == 'rdbms':
            if not value:
                raise ValueError("db_type is required for source_type 'rdbms'")
            allowed_types = {"postgresql"}
            if value.lower() not in allowed_types:
                raise ValueError(f"db_type must be one of {allowed_types}, but got '{value}'")
        return value

    @field_validator('file_path')
    @classmethod
    def validate_file_path(cls, value, info):
        if info.data.get('source_type') == 'flat_file':
            if not value:
                raise ValueError("file_path is required for source_type 'flat_file'")
            if not os.path.exists(value):
                # NOTE: This check works in the context of the Docker container.
                raise ValueError(f"file_path '{value}' does not exist")
        return value

# model for the producer section of the contract
class Producer(BaseModel):
    name: str
    source_system: str
    description: str
    source_type: str
    source_config: SourceConfig

    @field_validator('source_type')
    @classmethod
    def validate_source_type(cls, value):
        allowed_types = {"rdbms", "flat_file"}
        if value.lower() not in allowed_types:
            raise ValueError(f"source_type must be one of {allowed_types}, but got '{value}'")
        return value

# model for the incremental load configuration
class IncrementalLoad(BaseModel):
    mode: str = "watermark"
    watermark_column: str
    watermark_type: str
    initial_watermark_value: str

    @field_validator('watermark_type')
    @classmethod
    def validate_watermark_type(cls, value):
        allowed_types = {"timestamp", "integer", "na"}
        if value.lower() not in allowed_types:
            raise ValueError(f"watermark_type must be one of {allowed_types}, but got '{value}'")
        return value

    @field_validator('mode')
    @classmethod
    def validate_mode(cls, value):
        allowed_modes = {"full", "watermark"}
        if value.lower() not in allowed_modes:
            raise ValueError(f"mode must be one of {allowed_modes}, but got '{value}'")
        return value

# model for the dataset section of the contract
class Dataset(BaseModel):
    entity_name: str
    version: str
    primary_keys: List[str]
    fetch_size: int
    incremental_load: IncrementalLoad
    data_schema: Schema

# model for Kafka configuration
class KafkaConfig(BaseModel):
    bootstrap_servers: str
    topic: str

# model for S3 landing configuration
class S3LandingConfig(BaseModel):
    bucket_name: str
    prefix: str
    data_format: str
    access_key: str
    secret_key: str
    endpoint: str
    secure: bool
    flush_message_count: int
    flush_interval_seconds: int
    ingestion_timeout_seconds: Optional[int] = 180

    @field_validator('data_format')
    @classmethod
    def validate_data_format(cls, value):
        allowed_formats = {"json", "jsonl"}
        if value.lower() not in allowed_formats:
            raise ValueError(f"data_format must be one of {allowed_formats}, but got '{value}'")
        return value

    @field_validator('prefix')
    @classmethod
    def validate_prefix(cls, value):
        if not value.endswith('/'):
            raise ValueError("prefix must be a folder path and end with a '/' (e.g., 'path/to/folder/')")
        return value


# model for the entire pipeline configuration
class PipelineConfig(BaseModel):
    producer: Producer
    dataset: Dataset
    kafka_config: KafkaConfig
    s3_landing_config: S3LandingConfig


if __name__ == '__main__':
    # Example of a valid configuration (RDBMS source)
    valid_rdbms_config = {
        'producer': {
            'name': 'online_retail_app',
            'source_system': 'online_retail_app',
            'description': 'Online Retail Application Transactions Data',
            'source_type': 'rdbms',
            'source_config': {'db_type': 'postgresql', 'host': 'localhost', 'port': 5432, 'database': 'sales_db', 'user': 'user', 'password': 'password'}
        },
        'dataset': {
            'entity_name': 'transactions',
            'version': '1.0',
            'primary_keys': ['invoiceno', 'stockcode'],
            'fetch_size': 10000,
            'incremental_load': {
                'mode': 'watermark',
                'watermark_column': 'invoicedate',
                'watermark_type': 'timestamp',
                'initial_watermark_value': '2009-12-01T00:00:00.000000'
            },
            'data_schema': {'fields': [{'name': 'invoiceno', 'type': 'string'}, {'name': 'stockcode', 'type': 'string'}, {'name': 'description', 'type': 'string'}]},
        },
        'kafka_config': {'bootstrap_servers': 'kafka:9092', 'topic': 'raw_transactions'},
        's3_landing_config': {
            'bucket_name': 'data-lake',
            'prefix': 'bronze/online_retail_app/transactions/',
            'data_format': 'jsonl',
            'access_key': 'minioadmin',
            'secret_key': 'minioadmin',
            'endpoint': 'minio:9000',
            'secure': False,
            'flush_message_count': 1000,
            'flush_interval_seconds': 10
        }
    }

    # Example of a valid configuration (flat file source)
    valid_file_config = {
        'producer': {
            'name': 'crm_system',
            'source_system': 'crm_data',
            'description': 'Customer demographic data from a CRM system',
            'source_type': 'flat_file',
            'source_config': {'file_path': '/app/data_sources/customer_demographics.json', 'file_type': 'json'}
        },
        'dataset': {
            'entity_name': 'customer_demographics',
            'version': '1.0',
            'primary_keys': ['customer_id'],
            'fetch_size': 1000,
            'incremental_load': {
                'mode': 'full',
                'watermark_column': 'NA',
                'watermark_type': 'NA',
                'initial_watermark_value': 'NA'
            },
            'data_schema': {'fields': [{'name': 'customer_id', 'type': 'integer'}, {'name': 'city', 'type': 'string'}]},
        },
        'kafka_config': {'bootstrap_servers': 'kafka:9092', 'topic': 'raw_customer_demographics'},
        's3_landing_config': {
            'bucket_name': 'data-lake',
            'prefix': 'bronze/crm_data/customer_demographics/',
            'data_format': 'jsonl',
            'access_key': 'minioadmin',
            'secret_key': 'minioadmin',
            'endpoint': 'minio:9000',
            'secure': False,
            'flush_message_count': 100,
            'flush_interval_seconds': 5
        }
    }

    try:
        # This will pass validation
        pipeline_config_rdbms = PipelineConfig.model_validate(valid_rdbms_config)
        print("RDBMS config passed validation!")

        # This will also pass validation
        pipeline_config_file = PipelineConfig.model_validate(valid_file_config)
        print("File config passed validation!")

        # Example of a validation error
        invalid_config = valid_rdbms_config.copy()
        invalid_config['dataset']['data_schema']['fields'][0]['type'] = 'unsupported_type'
        PipelineConfig.model_validate(invalid_config)
    except ValidationError as e:
        print("Caught expected validation error:")
        print(e)