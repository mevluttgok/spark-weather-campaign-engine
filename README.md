# 🎯 Hava Durumu Bazlı LLM Kampanya Motoru

Büyük veri işleme (Apache Spark & Kafka) ile Üretken Yapay Zeka'yı (Google Gemini LLM) birleştiren gerçek zamanlı uçtan uca veri mühendisliği ve pazarlama otomasyonu projesi.

## Özellikler
- **Gerçek Zamanlı Veri Akışı:** Open-Meteo API'sinden anlık Türkiye hava durumu verisi çekilir (her 60 saniyede bir 8 şehir).
- **Stream Processing (Spark):** Apache Spark Structured Streaming ile akan veri temizlenir, 5 dakikalık sliding window'lar (pencereler) ile agregasyona sokulur ve acil durum uyarıları üretilir.
- **Microservice Mimari:** Veri `weather-events` topic'inden akar.
- **LLM Campaign Engine:** Şehirlerin hava durumu kategorisi (yağmurlu, güneşli, don) değiştiğinde Google Gemini 1.5 Flash AI modeli otomatik çağrılarak, hava koşuluna özel satışı artıracak JSON formatında "Pazarlama Kampanyaları" oluşturur.
- **Dashboard:** Streamlit ile modern, açık temalı, canlı izleme ekranı. Kampanya kartları, anlık hava bilgisi, uyarı akışı ve Türkiye haritası üzerinden sıcaklık dağılımı.

## Proje Bileşenleri
```text
spark/
├── docker-compose.yml
├── Makefile
├── producers/               # Open-Meteo -> Kafka Producer
├── spark_jobs/              # Spark Streaming (Scala/Python)
├── campaign_engine/         # Gemini LLM Bağlantısı & Sınıflandırıcı
├── dashboard/               # Streamlit Arayüzü (app.py)
└── .env                     # Gizli anahtarlarınız (Gemini API Key)
```

## Kurulum ve Çalıştırma

1. **Gereksinimler:** Docker ve Docker Compose kurulu olmalıdır.
2. **API Key Ayarı:** Proje dizininde `.env` dosyasını oluşturun veya düzenleyin:
   ```env
   GEMINI_API_KEY=sizin_gemini_api_key_buraya
   ```
3. **Başlatma:**
   ```bash
   make up
   ```
   *İlk kurulumda Docker imajları indirileceği için sistemin tamamen ayağa kalkması 3-5 dakika sürebilir.*

## Erişim URL'leri
- **Dashboard (Gerçek Zamanlı UI):** [http://localhost:8501](http://localhost:8501)
- **Kafka UI:** [http://localhost:8080](http://localhost:8080)
- **Spark Master UI:** [http://localhost:9090](http://localhost:9090)

## Teknik Yığın (Tech Stack)
- **Veri Toplama:** Python (requests, tenacity)
- **Mesaj Kuyruğu:** Apache Kafka, Zookeeper
- **İşleme:** Apache Spark 3.5 (PySpark Structured Streaming)
- **AI Model:** Google Gemini API (gemini-1.5-flash-latest)
- **Depolama:** Parquet (Local Data Lake)
- **Görselleştirme:** Streamlit, Plotly
- **Orkestrasyon:** Docker Compose
