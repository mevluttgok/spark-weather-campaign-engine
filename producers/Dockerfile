FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    requests==2.31.0 \
    tenacity==8.2.3

COPY weather_producer.py .

# PRODUCER_TYPE=weather ile çalışır
ENTRYPOINT ["python", "weather_producer.py"]
