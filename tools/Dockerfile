FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY replay_archive_to_kafka.py .

CMD ["python", "replay_archive_to_kafka.py"]
