.PHONY: up down restart build clean status logs logs-weather logs-spark logs-campaign topics consume-weather consume-campaigns

# ─── Temel ───────────────────────────────────────────────────────────────────
up:
	@echo "🚀 Tüm servisler başlatılıyor..."
	docker compose up -d --build
	@echo "⏳ 35sn bekleniyor (Kafka hazır olana kadar)..."
	@sleep 35
	@make status

down:
	docker compose down

restart:
	docker compose restart

build:
	docker compose build --no-cache

clean:
	@echo "🧹 Temizleniyor..."
	docker compose down -v
	rm -rf ./data_lake ./checkpoints
	@echo "✅ Temiz!"

# ─── Durum ───────────────────────────────────────────────────────────────────
status:
	@echo ""
	@echo "📊 Servis Durumları:"
	@echo "────────────────────────────────────────────────"
	@docker compose ps
	@echo ""
	@echo "🌐 URL'ler:"
	@echo "  🟠 Kafka UI      → http://localhost:8080"
	@echo "  ⚡ Spark Master  → http://localhost:9090"
	@echo "  🎯 Dashboard     → http://localhost:8501"
	@echo ""

# ─── Loglar ──────────────────────────────────────────────────────────────────
logs:
	docker compose logs -f --tail=50

logs-weather:
	docker compose logs -f producer-weather --tail=50

logs-spark:
	docker compose logs -f spark-streaming --tail=100

logs-campaign:
	docker compose logs -f campaign-engine --tail=100

logs-dashboard:
	docker compose logs -f dashboard --tail=50

# ─── Kafka ───────────────────────────────────────────────────────────────────
topics:
	@docker exec spark-kafka kafka-topics --bootstrap-server localhost:9092 --list

consume-weather:
	@echo "👀 weather-events (Ctrl+C ile çık):"
	docker exec spark-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 --topic weather-events --from-beginning

consume-campaigns:
	@echo "🎯 campaign-recommendations (Ctrl+C ile çık):"
	docker exec spark-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 --topic campaign-recommendations --from-beginning

consume-classified:
	@echo "🔍 weather-classified (Ctrl+C ile çık):"
	docker exec spark-kafka kafka-console-consumer \
		--bootstrap-server localhost:9092 --topic weather-classified --from-beginning

# ─── Debug ───────────────────────────────────────────────────────────────────
shell-kafka:
	docker exec -it spark-kafka bash

shell-campaign:
	docker exec -it spark-campaign-engine bash

data-lake:
	@echo "📦 Data Lake içeriği:"
	@find ./data_lake -name "*.parquet" 2>/dev/null | head -30 || echo "Henüz veri yok"

# ─── API Key Kontrolü ────────────────────────────────────────────────────────
check-env:
	@echo "🔑 GEMINI_API_KEY kontrolü:"
	@grep GEMINI_API_KEY .env | head -1
