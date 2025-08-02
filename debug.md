# Ingestion Service App

### to run scripts under ingestion_app container, run the code as module to avoid import errors (since models are above the main app directory)
- run `python -m src.postgres_extractor` or `python -m src.kafka_topic_viewer`

###### note: notice the `-m` flag which allows you to run a module as a script, treating the directory as a package. and the absence of the `.py` extension.

# postgres_extractor.py:

### to peak on data on the fly
- convert logging level from INFOR to DEBUG
- add `logging.debug(f"Extracted Record Sample: {json.dumps(record, cls=CustomJsonEncoder)[:200]}...") # Log first 200 chars` at line 167 before the check for watermark value

### to peak from kafka topic via kcat cli on container
- after the postgres_extractor.py runs inside the app container, and while kafka service is up: 
  run `docker run --network sales-data-analytics-pipeline-service_default --rm edenhill/kcat:1.7.1 -b kafka:9092 -t raw_transactions -C -o beginning -e | head`

### to see data delivered to kafka topic via kafka_topic_viewer.py script 
- run `docker compose run --rm ingestion_app /bin/bash` to keep the container running on shell
- run `python3 /app/scripts/kafka_topic_viewer.py` note that this script relies on data contract

