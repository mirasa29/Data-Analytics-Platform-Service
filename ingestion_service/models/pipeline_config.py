from pydantic import BaseModel, Field, field_validator, ValidationError
from typing import List, Dict, Any, Optional
from datetime import datetime
from decimal import Decimal


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
    db_type: str
    host: str
    port: int
    database: str
    user: str
    password: str

    @field_validator('db_type')
    def validate_db_type(cls, value):
        allowed_types = {"postgresql"}
        if value.lower() not in allowed_types:
            raise ValueError(f"db_type must be one of {allowed_types}, but got '{value}'")
        return value


# model for the producer section of the contract
class Producer(BaseModel):
    name: str
    source_system: str
    description: str
    source_type: str
    source_config: SourceConfig


# model for the incremental load configuration
class IncrementalLoad(BaseModel):
    mode: str = "watermark"
    watermark_column: str
    watermark_type: str
    initial_watermark_value: str

    @field_validator('watermark_type')
    def validate_watermark_type(cls, value):
        allowed_types = {"timestamp", "integer"}
        if value.lower() not in allowed_types:
            raise ValueError(f"watermark_type must be one of {allowed_types}, but got '{value}'")
        return value

    @field_validator('mode')
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

    @field_validator('data_format')
    def validate_data_format(cls, value):
        allowed_formats = {"json", "jsonl"}
        if value.lower() not in allowed_formats:
            raise ValueError(f"data_format must be one of {allowed_formats}, but got '{value}'")
        return value

    @field_validator('prefix')
    def validate_prefix(cls, value):
        if not value.endswith('/'):
            raise ValueError("prefix must be a folder path and end with a '/'")
        return value


# model for the entire pipeline configuration
class PipelineConfig(BaseModel):
    producer: Producer
    dataset: Dataset
    kafka_config: KafkaConfig
    s3_landing_config: S3LandingConfig
    