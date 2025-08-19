# Project root: /Users/mirasa/PycharmProjects/Data-Analytics-Platform-Service

workdir := $(shell pwd)
KAFKA_LOGS := ./data/kafka/kraft_logs
MINIO_DATA := ./data/minio/data-lake

.PHONY: install format install-docker install-docker-compose run-docker-engine start-docker stop-docker restart-docker docker-ingest-bash docker-logs clean-kafka clean-minio clean-all restart-docker clean-kafka-restart clean-minio-restart clean-all-restart

## Fresh install of the project
install:
	@echo "🧹 Cleaning up previous installation and virtual environment..."
	@rm -rf .venv
	@python3 -m venv .venv
	@.venv/bin/pip install --upgrade pip
	@.venv/bin/pip install -r requirements.txt
	@echo "✅ Fresh install complete."

## format all code files
format:
	@echo "🛠️ Formatting code files..."
	.venv/bin/autopep8 --in-place .
	.venv/bin/isort .
	@echo "✅ Code formatting complete."

## install docker
install-docker:
	@echo "🔧 Installing Docker..."
	@brew install docker
	@echo "✅ Docker installation complete."

## install docker-compose
install-docker-compose:
	@echo "🔧 Installing Docker Compose..."
	@brew install docker-compose
	@echo "✅ Docker Compose installation complete."

## Run Docker engine
run-docker-engine:
	@echo "🚀 Starting Docker engine..."
	@open -a Docker
	@sleep 5  # Wait for Docker to start
	@docker info > /dev/null 2>&1 || (echo "❌ Docker engine is not running. Please start Docker." && exit 1)
	@echo "✅ Docker engine is running."

## Stop docker-compose services
stop-docker:
	@echo "⏹️ Stopping docker-compose services..."
	@docker compose down

## Start docker-compose services
start-docker:
	@echo "🚀 Starting docker-compose services..."
	@docker compose up -d --build

## Restart docker-compose services
restart-docker: stop-docker start-docker

## show docker app logs
docker-logs:
	@echo "📜 Showing docker-compose logs..."
	@docker compose logs -f

## log into running docker ingestion app container
docker-ingest-bash:
	@echo "🔍 Logging into the running docker container..."
	@docker compose run --rm ingestion_app /bin/bash

## Clean Kafka topic data but preserve cluster metadata
clean-kafka:
	@echo "🧹 Cleaning Kafka topic data in all kafka log folders..."
	@for dir in ./data/kafka/kraft_logs ./data/kafka2/kraft_logs ./data/kafka3/kraft_logs; do \
		if [ -d $$dir ]; then \
			find $$dir -mindepth 1 -maxdepth 1 \
				! -name "__cluster_metadata*" \
				-exec rm -rf {} +; \
			echo "✅ Kafka data cleaned in $$dir (cluster metadata preserved)."; \
		else \
			echo "⚠️ Directory $$dir does not exist."; \
		fi \
	done

## Clean MinIO persisted data
clean-minio:
	@echo "🧹 Cleaning MinIO data in $(MINIO_DATA)..."
	@rm -rf $(MINIO_DATA)/*
	@echo "✅ MinIO data cleaned."

## Clean both Kafka and MinIO data
clean-all: clean-kafka clean-minio
	@echo "🎯 All data cleaned."

## Clean Kafka data with docker-compose restart
clean-kafka-restart: stop-docker clean-kafka start-docker

## Clean MinIO data with docker-compose restart
clean-minio-restart: stop-docker clean-minio start-docker

## Clean all data with docker-compose restart
clean-all-restart: stop-docker clean-all start-docker