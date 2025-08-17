FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# done separately so that code changes don't force a re-install of requirements
COPY src ./src
COPY models ./models
COPY config ./config
COPY state ./state
COPY tests ./tests
COPY data_sources ./data_sources

# Expose the port the app runs on (if applicable)
EXPOSE 8000

# This command will be the default entrypoint when the container starts
#CMD ["python", "-m", "src.main_ingestion_job"]
#CMD ["python", "-m", "src.postgres_extractor"]
CMD ["python", "-m", "src.main_ingestion_job"]

