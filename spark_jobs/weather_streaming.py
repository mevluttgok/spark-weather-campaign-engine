"""
Weather Streaming Job (Revize)
-------------------------------
Kafka 'weather-events' → Spark Structured Streaming

İşlemler:
  1. JSON parse + validasyon
  2. Hava sınıflandırma kolonu ekleme (Spark UDF)
  3. Console sink: anlık şehir-sıcaklık tablosu
  4. Parquet sink: ham kayıtlar (data_lake/weather/raw/)
  5. Parquet sink: 5dk window aggregation (data_lake/weather/windowed/)
  6. Kafka sink: weather-classified topic (campaign engine için)
  7. Kafka sink: aşırı hava koşulları → alerts topic
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_json, struct, window,
    avg, min as spark_min, max as spark_max,
    count, lit, udf,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
)

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATA_LAKE       = os.getenv("DATA_LAKE_PATH", "/opt/data_lake")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/checkpoints")

# ─── Şema ────────────────────────────────────────────────────────────────────
WEATHER_SCHEMA = StructType([
    StructField("city",               StringType()),
    StructField("latitude",           DoubleType()),
    StructField("longitude",          DoubleType()),
    StructField("temperature_c",      DoubleType()),
    StructField("feels_like_c",       DoubleType()),
    StructField("humidity_pct",       DoubleType()),
    StructField("wind_speed_kmh",     DoubleType()),
    StructField("wind_direction_deg", DoubleType()),
    StructField("pressure_hpa",       DoubleType()),
    StructField("precipitation_mm",   DoubleType()),
    StructField("cloud_cover_pct",    DoubleType()),
    StructField("weather_code",       IntegerType()),
    StructField("weather_desc",       StringType()),
    StructField("observation_time",   StringType()),
    StructField("ingested_at",        StringType()),
])

# ─── Hava Sınıflandırma UDF ──────────────────────────────────────────────────
RAIN_CODES  = set(range(51, 83))
SNOW_CODES  = set(range(71, 78))
STORM_CODES = {95, 96, 99}


def classify_weather(temp: float, code: int, precip: float) -> str:
    if code is None or temp is None:
        return "NORMAL"
    if code in STORM_CODES or (code in SNOW_CODES and temp < 2):
        return "SNOWSTORM"
    if code in SNOW_CODES:
        return "COLD_SNAP"
    if code in RAIN_CODES or (precip or 0) >= 1.0:
        return "RAINY"
    if temp < 5:
        return "COLD_SNAP"
    if temp >= 28:
        return "HOT_SUNNY"
    if code in {2, 3}:
        return "CLOUDY"
    return "NORMAL"


classify_udf = udf(classify_weather, StringType())


def create_spark() -> SparkSession:
    packages = ",".join([
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        "org.apache.kafka:kafka-clients:3.5.1",
    ])
    return (
        SparkSession.builder
        .appName("WeatherStreamingRevised")
        .config("spark.jars.packages", packages)
        .getOrCreate()
    )


def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    print("⚡ Weather Streaming Job (Revize) başlatıldı")

    # ── Raw stream ────────────────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", "weather-events")
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse + Sınıflandırma ──────────────────────────────────────────────────
    parsed = (
        raw
        .select(from_json(col("value").cast("string"), WEATHER_SCHEMA).alias("d"), col("timestamp"))
        .select("d.*", col("timestamp").alias("event_time"))
        .withColumn(
            "weather_category",
            classify_udf(col("temperature_c"), col("weather_code"), col("precipitation_mm"))
        )
    )

    # ═══════════════════════════════ SINK 1: Console ════════════════════════════
    q_console = (
        parsed
        .select("city", "temperature_c", "feels_like_c", "humidity_pct",
                "weather_desc", "weather_category", "event_time")
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", False)
        .trigger(processingTime="30 seconds")
        .queryName("weather_console")
        .start()
    )

    # ═══════════════════════════════ SINK 2: Parquet raw ════════════════════════
    q_parquet = (
        parsed
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{DATA_LAKE}/weather/raw")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/weather/raw")
        .partitionBy("city")
        .trigger(processingTime="60 seconds")
        .queryName("weather_parquet")
        .start()
    )

    # ═══════════════════════════════ SINK 3: Window aggregation ════════════════
    windowed = (
        parsed
        .withWatermark("event_time", "5 minutes")
        .groupBy(window("event_time", "5 minutes"), "city", "weather_category")
        .agg(
            avg("temperature_c").alias("avg_temp_c"),
            spark_min("temperature_c").alias("min_temp_c"),
            spark_max("temperature_c").alias("max_temp_c"),
            avg("humidity_pct").alias("avg_humidity"),
            avg("wind_speed_kmh").alias("avg_wind_kmh"),
            avg("precipitation_mm").alias("avg_precip_mm"),
            count("*").alias("observation_count"),
        )
        .select(
            col("city"), col("weather_category"),
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("avg_temp_c"), col("min_temp_c"), col("max_temp_c"),
            col("avg_humidity"), col("avg_wind_kmh"),
            col("avg_precip_mm"), col("observation_count"),
        )
    )

    q_windowed = (
        windowed
        .writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", f"{DATA_LAKE}/weather/windowed")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/weather/windowed")
        .trigger(processingTime="60 seconds")
        .queryName("weather_windowed")
        .start()
    )

    # ═══════════════════════════════ SINK 4: weather-classified (Campaign Engine için) ═══
    classified_kafka = (
        parsed
        .select(
            to_json(struct(
                col("city"), col("latitude"), col("longitude"),
                col("temperature_c"), col("feels_like_c"),
                col("humidity_pct"), col("wind_speed_kmh"),
                col("precipitation_mm"), col("weather_code"),
                col("weather_desc"), col("weather_category"),
                col("observation_time"), col("ingested_at"),
            )).alias("value")
        )
    )

    q_classified = (
        classified_kafka
        .writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", "weather-classified")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/weather/classified")
        .trigger(processingTime="30 seconds")
        .queryName("weather_classified_kafka")
        .start()
    )

    # ═══════════════════════════════ SINK 5: Alerts ════════════════════════════
    extreme = (
        parsed
        .filter((col("temperature_c") > 35) | (col("temperature_c") < -5))
        .select(
            to_json(struct(
                lit("extreme_weather").alias("alert_type"),
                col("city"), col("temperature_c"),
                col("weather_desc"), col("weather_category"),
                col("event_time"),
            )).alias("value")
        )
    )

    q_alerts = (
        extreme
        .writeStream
        .outputMode("append")
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("topic", "alerts")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/weather/alerts")
        .trigger(processingTime="30 seconds")
        .queryName("weather_alerts")
        .start()
    )

    print("✅ Tüm Weather query'leri başlatıldı (Console + Parquet + Kafka Classified + Alerts)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
