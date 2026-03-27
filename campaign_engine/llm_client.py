"""
LLM Client — Google Gemini
---------------------------
Hava durumu profiline göre kampanya tavsiyeleri üretir.
Gemini 1.5 Flash modeli (ücretsiz tier) kullanır.
"""

import json
import logging
import os
import re
from typing import Optional

import google.generativeai as genai

from weather_classifier import WeatherProfile

logger = logging.getLogger(__name__)

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
MODEL_NAME     = os.getenv("GEMINI_MODEL", "gemini-1.5-flash")


CATEGORY_ICONS = {
    "RAINY":     "🌧️",
    "HOT_SUNNY": "☀️",
    "COLD_SNAP": "🥶",
    "SNOWSTORM": "❄️",
    "CLOUDY":    "🌥️",
    "NORMAL":    "🌤️",
}

SYSTEM_PROMPT = """Sen deneyimli bir perakende pazarlama uzmanısın.
Görevin: verilen şehir ve hava durumu bilgisine göre o gün için satış arttırabilecek
ürün kampanyaları önermek.

Kurallar:
- Öneriler gerçekçi, uygulanabilir ve hava koşumuyla doğrudan ilişkili olmalı.
- Her kampanya için net bir hedef kitle ve mesaj yaz.
- Türkçe yaz.
- SADECE aşağıdaki JSON formatını döndür, başka hiçbir şey yazma.
"""

CAMPAIGN_SCHEMA = """
[
  {
    "campaign_title": "string",
    "target_segment": "string (kim için: aile, genç, ev hanımı...)",
    "target_products": ["ürün1", "ürün2", "ürün3"],
    "campaign_message": "string (kısa, çarpıcı slogan veya mesaj)",
    "channel": "online | store | both",
    "urgency": "immediate | today | this_week | normal",
    "discount_suggestion": "string (örn: %15 indirim, 2 al 1 öde, ücretsiz teslimat)",
    "reasoning": "string (neden bu kampanya bu havada işe yarar)"
  }
]
"""


def build_prompt(profile: WeatherProfile, num_campaigns: int = 3) -> str:
    icon = CATEGORY_ICONS.get(profile.category.value, "🌡️")
    hints = ", ".join(profile.product_hints[:8])

    prompt = f"""
{icon} Hava Durumu Analizi:
- Şehir: {profile.city}
- Tarih: {profile.date} ({profile.weekday})
- Hava Durumu: {profile.weather_desc} ({profile.category.value})
- Sıcaklık: {profile.temperature_c:.1f}°C (hissedilen: {profile.feels_like_c or '?'}°C)
- Nem: {profile.humidity_pct or '?'}%
- Yağış: {profile.precipitation_mm or 0:.1f} mm
- Rüzgar: {profile.wind_speed_kmh or 0:.0f} km/h

Bu havada satışı artması beklenen ürün kategorileri: {hints}

Lütfen bu şehir ve hava koşulları için **{num_campaigns} adet** ürün kampanyası öner.
Her kampanya farklı bir ürün kategorisine odaklanmalı.

Döndüreceğin format (SADECE JSON, başka hiçbir şey yazma):
{CAMPAIGN_SCHEMA}
"""
    return prompt.strip()


def parse_campaigns(raw_text: str) -> list[dict]:
    """LLM çıktısından JSON listesini parse et."""
    # Markdown kod bloğu varsa temizle
    cleaned = re.sub(r"```(?:json)?", "", raw_text).replace("```", "").strip()
    # İlk [ ile son ] arasını al
    start = cleaned.find("[")
    end   = cleaned.rfind("]") + 1
    if start == -1 or end == 0:
        raise ValueError(f"JSON listesi bulunamadı. Ham metin: {raw_text[:300]}")
    return json.loads(cleaned[start:end])


class GeminiClient:
    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("GEMINI_API_KEY ortam değişkeni boş! .env dosyasını kontrol edin.")
        genai.configure(api_key=GEMINI_API_KEY)
        self.model = genai.GenerativeModel(
            model_name=MODEL_NAME,
            system_instruction=SYSTEM_PROMPT,
            generation_config={
                "temperature":       0.7,
                "top_p":             0.9,
                "max_output_tokens": 2048,
            },
        )
        logger.info(f"✅ Gemini istemci hazır — model: {MODEL_NAME}")

    def generate_campaigns(
        self,
        profile: WeatherProfile,
        num_campaigns: int = 3,
    ) -> list[dict]:
        """Bir hava profili için LLM kampanya listesi üret."""
        prompt = build_prompt(profile, num_campaigns)
        logger.info(f"🤖 Gemini çağrısı: {profile.city} / {profile.category.value}")

        try:
            response = self.model.generate_content(prompt)
            raw = response.text
            campaigns = parse_campaigns(raw)

            # Profil metadatasını her kampanyaya ekle
            for c in campaigns:
                c["city"]          = profile.city
                c["date"]          = profile.date
                c["weekday"]       = profile.weekday
                c["weather_category"] = profile.category.value
                c["temperature_c"] = profile.temperature_c
                c["weather_desc"]  = profile.weather_desc

            logger.info(f"✅ {len(campaigns)} kampanya üretildi: {profile.city}")
            return campaigns

        except Exception as e:
            logger.error(f"Gemini hatası: {e}", exc_info=True)
            # Fallback: kural tabanlı kampanya
            return _fallback_campaigns(profile)


def _fallback_campaigns(profile: WeatherProfile) -> list[dict]:
    """API çağrısı başarısız olursa kural tabanlı fallback."""
    ctx = {
        "campaign_title":    f"{profile.city} — {profile.weather_desc} Kampanyası",
        "target_segment":    "Genel müşteri",
        "target_products":   profile.product_hints[:3],
        "campaign_message":  f"{profile.weekday} {profile.weather_desc} havasında özel fırsatlar!",
        "channel":           profile.channel_hint,
        "urgency":           profile.urgency,
        "discount_suggestion": "%10 indirim",
        "reasoning":         f"{profile.weather_desc} nedeniyle bu ürünlere talep artar.",
        "city":              profile.city,
        "date":              profile.date,
        "weekday":           profile.weekday,
        "weather_category":  profile.category.value,
        "temperature_c":     profile.temperature_c,
        "weather_desc":      profile.weather_desc,
        "is_fallback":       True,
    }
    return [ctx]
