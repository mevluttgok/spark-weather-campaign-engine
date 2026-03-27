#!/bin/bash
# Spark Weather Streaming Entrypoint

set -e

SPARK_HOME="/opt/spark"
JOBS_DIR="/opt/spark_jobs"
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.kafka:kafka-clients:3.5.1"

mkdir -p "${DATA_LAKE_PATH:-/opt/data_lake}/weather/raw"
mkdir -p "${DATA_LAKE_PATH:-/opt/data_lake}/weather/windowed"
mkdir -p "${CHECKPOINT_PATH:-/opt/checkpoints}/weather"

echo "⚡ Weather Streaming Job başlatılıyor..."
${SPARK_HOME}/bin/spark-submit \
    --master "${SPARK_MASTER_URL:-local[2]}" \
    --packages "${PACKAGES}" \
    --conf "spark.driver.memory=512m" \
    --conf "spark.executor.memory=512m" \
    "${JOBS_DIR}/weather_streaming.py"
