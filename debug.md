# Create Topic with partitions

### apply parallelism for kafka 
For Postgres:
- run `docker compose run --rm kafka1 kafka-topics.sh --create --topic raw_transactions --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3`

For Flatfile:
- run `docker compose run --rm kafka1 kafka-topics.sh --create --topic raw_customer_demographics --bootstrap-server kafka1:9092 --partitions 3 --replication-factor 3`

# Ingestion Service App

### to run scripts under ingestion_app container, run the code as module to avoid import errors (since models are above the main app directory)
- run `docker compose run --rm ingestion_app /bin/bash` to keep the container running on shell
- run `python -m src.main_ingestion_job`
###### note: notice the `-m` flag which allows you to run a module as a script, treating the directory as a package. and the absence of the `.py` extension.

### to peak from kafka topic via kcat cli on container
- after the postgres_extractor.py runs inside the app container, and while kafka service is up: 
  run `docker run --network sales-data-analytics-pipeline-service_default --rm edenhill/kcat:1.7.1 -b kafka:9092 -t raw_transactions -C -o beginning -e | head`

### to see data delivered to kafka topic via kafka_topic_viewer.py script 
- run `docker compose run --rm ingestion_app /bin/bash` to keep the container running on shell
- run `python3 /app/scripts/kafka_topic_viewer.py` note that this script relies on data contract

