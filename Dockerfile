FROM python:3.9-slim-buster

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ./ingestion_service .

# Expose the port the app runs on (if applicable)
EXPOSE 8000

# This command will be the default entrypoint when the container starts
CMD ["python", "src/main_ingestion_job.py"]
