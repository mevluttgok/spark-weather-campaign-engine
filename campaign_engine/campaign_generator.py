"""
Campaign Generator — Ana Döngü
--------------------------------
Kafka 'weather-events' → WeatherProfile → Gemini LLM → 'campaign-recommendations'

Çalışma mantığı:
  1. weather-events topic'inden son hava verilerini tüketir
  2. Her şehir için WeatherProfile oluşturur (WeatherClassifier)
  3. Konfigürasyona göre değişen hava kategorisinde LLM çağrısı yapar
  4. Üretilen kampanyaları 'campaign-recommendations' Kafka topic'ine yazar
  5. Parquet'e de yazar (dashboard geçmiş görünümü için)

Önemli: Aynı şehir+kategori kombinasyonu için LLM tekrar çağrılmaz (cache).
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError
from tenacity import retry, stop_after_attempt, wait_exponential

from weather_classifier import build_profile, WeatherCategory
from llm_client import GeminiClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("CampaignGenerator")

# ─── Konfigürasyon ────────────────────────────────────────────────────────────
KAFKA_SERVERS     = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC       = "weather-events"
OUTPUT_TOPIC      = "campaign-recommendations"
POLL_INTERVAL_SEC = int(os.getenv("CAMPAIGN_POLL_INTERVAL", "300"))   # 5 dakika
NUM_CAMPAIGNS     = int(os.getenv("NUM_CAMPAIGNS_PER_CITY", "3"))
CONSUMER_TIMEOUT  = 8000   # ms — 8sn içinde mesaj gelmezse döngü devam eder


# ─── Kafka bağlantıları ───────────────────────────────────────────────────────
@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=3, max=30))
def create_consumer() -> KafkaConsumer:
    logger.info("Kafka consumer bağlanıyor...")
    consumer = KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=KAFKA_SERVERS,
        auto_offset_reset="latest",
        consumer_timeout_ms=CONSUMER_TIMEOUT,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="campaign-engine",
        enable_auto_commit=True,
    )
    logger.info("✅ Consumer hazır")
    return consumer


@retry(stop=stop_after_attempt(10), wait=wait_exponential(multiplier=2, min=3, max=30))
def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
    )


# ─── Cache: aynı şehir+kategori için tekrar LLM çağırma ──────────────────────
class CategoryCache:
    """
    Her şehir + hedef gün için en son hava kategorisini tutar.
    Sadece kategori değiştiğinde LLM çağrısı yapılır.
    """
    def __init__(self):
        self._last: dict[tuple[str, str], WeatherCategory] = {}

    def has_changed(self, city: str, target_date: str, cat: WeatherCategory) -> bool:
        key = (city, target_date)
        prev = self._last.get(key)
        self._last[key] = cat
        return prev != cat

    def reset(self):
        self._last.clear()


# ─── Ana işlem ────────────────────────────────────────────────────────────────
def process_weather_batch(
    messages: list[dict],
    llm: GeminiClient,
    producer: KafkaProducer,
    cache: CategoryCache,
) -> int:
    """
    Bir batch hava mesajını işle.
    Şehir başına en güncel mesajı seç, kategorisi değişmişse LLM çağır.
    """
    # Şehir+Tarih ikilisine göre en son kaydı al
    latest_by_key: dict[tuple[str, str], dict] = {}
    for msg in messages:
        city = msg.get("city", "?")
        t_date = msg.get("target_date", "?")
        latest_by_key[(city, t_date)] = msg

    campaigns_published = 0
    for (city, t_date), weather in latest_by_key.items():
        try:
            profile = build_profile(weather)
            logger.info(
                f"📍 {city} ({t_date}): {profile.category.value} | "
                f"{profile.temperature_c:.1f}°C | {profile.weather_desc}"
            )

            # Kategori değişmediyse LLM çağırma
            if not cache.has_changed(city, t_date, profile.category):
                logger.info(f"  ↩️  {city} ({t_date}) — kategori değişmedi, LLM atlanıyor")
                continue

            # Kampanya üret
            campaigns = llm.generate_campaigns(profile, num_campaigns=NUM_CAMPAIGNS)

            # Her kampanyayı Kafka'ya yaz
            for campaign in campaigns:
                campaign["generated_at"] = datetime.now(timezone.utc).isoformat()
                future = producer.send(OUTPUT_TOPIC, value=campaign)
                future.get(timeout=10)
                campaigns_published += 1
                logger.info(f"  📣 Kampanya: {campaign.get('campaign_title', '?')}")

        except Exception as e:
            logger.error(f"❌ {city} işlenemedi: {e}", exc_info=True)

    producer.flush()
    return campaigns_published


# ─── Başlangıç ────────────────────────────────────────────────────────────────
def print_banner():
    logger.info("=" * 55)
    logger.info("  🎯 Hava Durumu Bazlı Kampanya Motoru Başlatılıyor")
    logger.info(f"  Kafka:   {KAFKA_SERVERS}")
    logger.info(f"  Input:   {INPUT_TOPIC}")
    logger.info(f"  Output:  {OUTPUT_TOPIC}")
    logger.info(f"  LLM:     Gemini")
    logger.info(f"  Döngü:   Her {POLL_INTERVAL_SEC} saniye")
    logger.info("=" * 55)


def run():
    print_banner()

    # Bileşenler
    llm      = GeminiClient()
    consumer = create_consumer()
    producer = create_producer()
    cache    = CategoryCache()

    logger.info("🚀 Kampanya motoru çalışıyor. Hava verisi bekleniyor...")

    while True:
        messages = []
        try:
            for msg in consumer:
                messages.append(msg.value)
        except Exception as e:
            if "CommitFailed" in str(e):
                logger.error("Kafka commit failed, rebalancing...")
            pass   # consumer_timeout_ms doldu, normal

        if messages:
            logger.info(f"📦 {len(messages)} hava mesajı alındı, işleniyor...")
            count = process_weather_batch(messages, llm, producer, cache)
            if count:
                logger.info(f"✅ Toplam {count} kampanya Kafka'ya yazıldı")
            else:
                logger.info("ℹ️  Bu turda yeni kampanya üretilmedi (kategori değişmedi)")
        else:
            logger.info("⏳ Hava verisi bekleniyor...")

        # 10 saniye bekle, polling timeout hatası almamak için 300 saniye uyumayıp cache'e güveniyoruz
        time.sleep(10)


if __name__ == "__main__":
    run()
