"""
Weather Producer (Standalone — Campaign Project)
------------------------------------------------
Open-Meteo API → Kafka 'weather-events' topic
8 Türk şehri için her 60 saniyede bir hava verisi.
API Key gerektirmez — tamamen ücretsiz.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [WeatherProducer] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("WeatherProducer")

KAFKA_SERVERS    = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
POLL_INTERVAL    = int(os.getenv("POLL_INTERVAL", "60"))
OPEN_METEO_URL   = "https://api.open-meteo.com/v1/forecast"

CITIES = [
    {"city": "Istanbul",  "lat": 41.0082, "lon": 28.9784},
    {"city": "Ankara",    "lat": 39.9334, "lon": 32.8597},
    {"city": "Izmir",     "lat": 38.4192, "lon": 27.1287},
    {"city": "Antalya",   "lat": 36.8969, "lon": 30.7133},
    {"city": "Bursa",     "lat": 40.1826, "lon": 29.0665},
    {"city": "Adana",     "lat": 37.0000, "lon": 35.3213},
    {"city": "Trabzon",   "lat": 41.0015, "lon": 39.7178},
    {"city": "Erzurum",   "lat": 39.9055, "lon": 41.2658},
]

WEATHER_CODES = {
    0: "Açık", 1: "Az Bulutlu", 2: "Parçalı Bulutlu", 3: "Çok Bulutlu",
    45: "Sisli", 48: "Kırağılı Sis",
    51: "Hafif Çiseleme", 53: "Orta Çiseleme", 55: "Yoğun Çiseleme",
    61: "Hafif Yağmur", 63: "Orta Yağmur", 65: "Şiddetli Yağmur",
    71: "Hafif Kar", 73: "Orta Kar", 75: "Yoğun Kar",
    80: "Sağanak", 81: "Kuvvetli Sağanak", 82: "Şiddetli Sağanak",
    95: "Fırtına", 96: "Dolu ile Fırtına", 99: "Şiddetli Dolu",
}


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=3, max=30))
def create_producer() -> KafkaProducer:
    logger.info(f"Kafka'ya bağlanılıyor: {KAFKA_SERVERS}")
    p = KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
        retries=5,
    )
    logger.info("✅ Kafka bağlantısı kuruldu")
    return p


def fetch_weather() -> list[dict]:
    records = []
    for city_info in CITIES:
        try:
            resp = requests.get(OPEN_METEO_URL, params={
                "latitude": city_info["lat"],
                "longitude": city_info["lon"],
                "daily": [
                    "weather_code", "temperature_2m_max", "precipitation_sum", "wind_speed_10m_max"
                ],
                "timezone": "auto",
                "wind_speed_unit": "kmh",
                "forecast_days": 3,
            }, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            daily = data.get("daily", {})
            
            # Index 0 is today, 1 is tomorrow, 2 is the day after tomorrow
            for day_idx in [1, 2]:
                try:
                    date_str = daily.get("time", [])[day_idx]
                    code = daily.get("weather_code", [])[day_idx]
                    records.append({
                        "city":               city_info["city"],
                        "latitude":           city_info["lat"],
                        "longitude":          city_info["lon"],
                        "temperature_c":      daily.get("temperature_2m_max", [])[day_idx],
                        "feels_like_c":       daily.get("temperature_2m_max", [])[day_idx], # Apparent not available in daily, use max
                        "humidity_pct":       50.0, # Default for now
                        "wind_speed_kmh":     daily.get("wind_speed_10m_max", [])[day_idx],
                        "precipitation_mm":   daily.get("precipitation_sum", [])[day_idx],
                        "weather_code":       code,
                        "weather_desc":       WEATHER_CODES.get(code, "Bilinmiyor"),
                        "observation_time":   datetime.now(timezone.utc).isoformat(),
                        "target_date":        date_str,
                        "ingested_at":        datetime.now(timezone.utc).isoformat(),
                        "producer":           "WeatherProducer",
                    })
                except IndexError:
                    pass
        except Exception as e:
            logger.error(f"{city_info['city']} verisi alınamadı: {e}")
    return records


def run():
    producer = create_producer()
    logger.info(f"🚀 WeatherProducer başlatıldı. Poll: {POLL_INTERVAL}sn")
    while True:
        records = fetch_weather()
        sent = 0
        for rec in records:
            try:
                # Key allows grouping by city and target_date in Kafka if log compaction was used
                kafka_key = f"{rec['city']}_{rec['target_date']}"
                future = producer.send("weather-events", value=rec, key=kafka_key)
                future.get(timeout=10)
                sent += 1
            except KafkaError as e:
                logger.error(f"Kafka gönderme hatası: {e}")
        producer.flush()
        logger.info(f"📤 {sent}/{len(records)} şehir verisi gönderildi → weather-events")
        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    run()
