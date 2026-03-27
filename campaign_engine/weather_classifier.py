"""
Weather Classifier
------------------
Hava durumu verisini işlek kampanya kategorilerine dönüştürür.

Kategoriler:
  RAINY       → Yağmurlu / Kapalı hava
  HOT_SUNNY   → Sıcak ve güneşli
  COLD_SNAP   → Ani soğuma
  SNOWSTORM   → Kar / Fırtına
  CLOUDY      → Kapalı ama kuru
  NORMAL      → Normal hava
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional


# WMO Weather Interpretation Codes
# https://open-meteo.com/en/docs#weather_variable
RAIN_CODES   = set(range(51, 83))   # drizzle + rain + showers
SNOW_CODES   = set(range(71, 78))   # snow
STORM_CODES  = {95, 96, 99}         # thunderstorm
FOG_CODES    = {45, 48}             # fog
CLOUDY_CODES = {2, 3}               # partly/overcast


class WeatherCategory(str, Enum):
    RAINY     = "RAINY"
    HOT_SUNNY = "HOT_SUNNY"
    COLD_SNAP = "COLD_SNAP"
    SNOWSTORM = "SNOWSTORM"
    CLOUDY    = "CLOUDY"
    NORMAL    = "NORMAL"


@dataclass
class WeatherProfile:
    city:          str
    date:          str
    weekday:       str
    category:      WeatherCategory
    temperature_c: float
    feels_like_c:  Optional[float]
    humidity_pct:  Optional[float]
    wind_speed_kmh: Optional[float]
    precipitation_mm: Optional[float]
    weather_desc:  str
    weather_code:  int
    # Kampanya bağlamı
    product_hints: list[str] = field(default_factory=list)
    channel_hint:  str = "both"        # online | store | both
    urgency:       str = "normal"       # immediate | today | this_week | normal


# ─── Ürün önerileri ve bağlam ───────────────────────────────────────────────

CATEGORY_CONTEXT = {
    WeatherCategory.RAINY: {
        "product_hints": [
            "şemsiye", "yağmurluk", "su geçirmez bot",
            "sıcak içecek (çay, çikolata)", "ev atıştırmalıkları",
            "battaniye", "mum", "online sipariş teslimatı",
            "film/dizi platform üyeliği",
        ],
        "channel_hint": "online",
        "urgency": "immediate",
        "desc": "Yağmurlu ve kapalı hava insanların evde kalmasına ve online alışveriş yapmasına yol açar.",
    },
    WeatherCategory.HOT_SUNNY: {
        "product_hints": [
            "güneş kremi", "dondurma", "soğuk içecek",
            "vantilatör", "klima", "yüzme malzemeleri",
            "piknik seti", "mangal ekipmanı", "bahçe mobilyası",
            "spor kıyafeti", "güneş gözlüğü",
        ],
        "channel_hint": "both",
        "urgency": "today",
        "desc": "Sıcak ve güneşli hava açık hava aktivitelerini ve serinleme ürünlerini öne çıkarır.",
    },
    WeatherCategory.COLD_SNAP: {
        "product_hints": [
            "kışlık mont", "bot", "atkı-bere-eldiven seti",
            "elektrikli ısıtıcı", "termal içlik",
            "sıcak içecek", "çorba malzemeleri", "battaniye",
        ],
        "channel_hint": "both",
        "urgency": "today",
        "desc": "Ani soğuma kışlık giyim ve ısınma ürünlerine yönelik acil talep yaratır.",
    },
    WeatherCategory.SNOWSTORM: {
        "product_hints": [
            "ekmek ve temel gıda stoku", "konserve", "su",
            "pil ve fener", "tuz ve kürek", "sıcak içecek",
            "acil durum kiti", "ısıtıcı",
        ],
        "channel_hint": "online",
        "urgency": "immediate",
        "desc": "Kar ve fırtına beklentisi ani stoklama davranışı ve online siparişe yönelim yaratır.",
    },
    WeatherCategory.CLOUDY: {
        "product_hints": [
            "iç mekan aktivite ürünleri", "kitap", "puzzle",
            "sıcak içecek", "hafif atıştırmalık",
        ],
        "channel_hint": "both",
        "urgency": "normal",
        "desc": "Kapalı hava iç mekan aktivitelerini teşvik eder.",
    },
    WeatherCategory.NORMAL: {
        "product_hints": [
            "genel promosyon ürünleri", "mevsim geçiş ürünleri",
        ],
        "channel_hint": "both",
        "urgency": "normal",
        "desc": "Normal hava koşulları genel alışveriş örüntüsünü destekler.",
    },
}

WEEKDAY_TR = {
    0: "Pazartesi", 1: "Salı", 2: "Çarşamba",
    3: "Perşembe", 4: "Cuma", 5: "Cumartesi", 6: "Pazar",
}


def classify(
    temperature_c: float,
    weather_code: int,
    precipitation_mm: float = 0.0,
    wind_speed_kmh: float = 0.0,
) -> WeatherCategory:
    """Hava verisini kategoriye dönüştür."""
    if weather_code in STORM_CODES or (weather_code in SNOW_CODES and temperature_c < 2):
        return WeatherCategory.SNOWSTORM
    if weather_code in SNOW_CODES:
        return WeatherCategory.COLD_SNAP
    if weather_code in RAIN_CODES or precipitation_mm >= 1.0:
        return WeatherCategory.RAINY
    if temperature_c < 5:
        return WeatherCategory.COLD_SNAP
    if temperature_c >= 28:
        return WeatherCategory.HOT_SUNNY
    if weather_code in CLOUDY_CODES or weather_code in FOG_CODES:
        return WeatherCategory.CLOUDY
    return WeatherCategory.NORMAL


def build_profile(weather_event: dict) -> WeatherProfile:
    """Kafka mesajından WeatherProfile oluştur."""
    from datetime import datetime

    temp    = weather_event.get("temperature_c") or 15.0
    code    = weather_event.get("weather_code") or 0
    precip  = weather_event.get("precipitation_mm") or 0.0
    wind    = weather_event.get("wind_speed_kmh") or 0.0
    ingested = weather_event.get("ingested_at") or datetime.utcnow().isoformat()

    cat = classify(temp, code, precip, wind)
    ctx = CATEGORY_CONTEXT[cat]

    # Tarih ve gün bilgisi
    try:
        dt = datetime.fromisoformat(ingested.replace("Z", "+00:00"))
    except Exception:
        dt = datetime.utcnow()
    date_str = dt.strftime("%d %B %Y")
    weekday  = WEEKDAY_TR[dt.weekday()]

    return WeatherProfile(
        city=weather_event.get("city", "Bilinmiyor"),
        date=date_str,
        weekday=weekday,
        category=cat,
        temperature_c=temp,
        feels_like_c=weather_event.get("feels_like_c"),
        humidity_pct=weather_event.get("humidity_pct"),
        wind_speed_kmh=wind,
        precipitation_mm=precip,
        weather_desc=weather_event.get("weather_desc", ""),
        weather_code=code,
        product_hints=ctx["product_hints"],
        channel_hint=ctx["channel_hint"],
        urgency=ctx["urgency"],
    )
