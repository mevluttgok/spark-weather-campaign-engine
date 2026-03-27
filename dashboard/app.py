"""
Streamlit Real-Time Campaign & Weather Dashboard
-------------------------------------------------
Sekmeler:
  🎯 Kampanya Tavsiyeleri — LLM tarafından üretilen kampanya kartları
  🌤️ Hava Durumu         — Şehir bazlı anlık hava ve grafikler
  🚨 Alert Merkezi       — Aşırı hava koşulu uyarıları
"""

import json
import os
import time
import glob
from datetime import datetime

import streamlit as st
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer

KAFKA_SERVERS   = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
DATA_LAKE       = os.getenv("DATA_LAKE_PATH", "/opt/data_lake")
REFRESH_SEC     = 10

# ─── Sayfa ayarları ──────────────────────────────────────────────────────────
st.set_page_config(
    page_title="🎯 Hava Bazlı Kampanya Motoru",
    page_icon="⛅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown("""
<style>
body, .main { background: #f8f9fa; color: #212529; }
.campaign-card {
    background: linear-gradient(135deg, #ffffff, #f1f3f5);
    border-radius: 14px;
    padding: 20px 24px;
    margin-bottom: 16px;
    border-left: 5px solid #0d6efd;
    box-shadow: 0 4px 12px rgba(0,0,0,0.05);
}
.campaign-card h3 { margin: 0 0 8px; color: #0d6efd; font-size: 1.1rem; }
.campaign-card .meta { font-size: 12px; color: #6c757d; margin-bottom: 10px; }
.campaign-card .products span {
    display: inline-block; background: #e9ecef; border: 1px solid #dee2e6;
    border-radius: 20px; padding: 3px 10px; margin: 2px; font-size: 12px;
    color: #495057;
}
.channel-badge {
    display: inline-block; border-radius: 20px; padding: 2px 10px;
    font-size: 11px; font-weight: 600; margin-left: 6px;
}
.online  { background:#cfe2ff; color:#084298; }
.store   { background:#d1e7dd; color:#0f5132; }
.both    { background:#fff3cd; color:#664d03; }
.weather-card {
    background: linear-gradient(135deg, #ffffff, #f1f3f5);
    border-radius: 12px; padding: 16px;
    text-align: center; border-top: 4px solid #198754;
    margin-bottom: 8px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.05);
    color: #212529;
}
.weather-card .temp { font-size: 2rem; font-weight: 700; }
.weather-card .city { font-size: 0.85rem; color: #6c757d; }
.alert-box {
    background: #f8d7da; border-left: 4px solid #dc3545;
    border-radius: 8px; padding: 12px 16px; margin-bottom: 8px;
    color: #842029;
}
</style>
""", unsafe_allow_html=True)


# ─── Helpers ──────────────────────────────────────────────────────────────────
CATEGORY_ICONS = {
    "RAINY":     "🌧️", "HOT_SUNNY": "☀️", "COLD_SNAP": "🥶",
    "SNOWSTORM": "❄️", "CLOUDY":    "🌥️", "NORMAL":    "🌤️",
}
CHANNEL_LABELS = {"online": "🌐 Online", "store": "🏪 Mağaza", "both": "🔀 Her İkisi"}

def kafka_read(topic: str, n: int = 50, timeout_ms: int = 4000) -> list[dict]:
    try:
        c = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_SERVERS,
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout_ms,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=None,
        )
        msgs = []
        for m in c:
            msgs.append(m.value)
            if len(msgs) >= n:
                break
        c.close()
        return msgs
    except Exception:
        return []


@st.cache_data(ttl=REFRESH_SEC)
def get_campaigns() -> list[dict]:
    return kafka_read("campaign-recommendations", n=100)


@st.cache_data(ttl=REFRESH_SEC)
def get_weather() -> pd.DataFrame:
    files = glob.glob(f"{DATA_LAKE}/weather/raw/**/*.parquet", recursive=True)
    if not files:
        return pd.DataFrame()
    try:
        dfs = [pd.read_parquet(f) for f in files[-30:]]
        return pd.concat(dfs, ignore_index=True)
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=REFRESH_SEC)
def get_alerts() -> list[dict]:
    return kafka_read("alerts", n=30)


def urgency_color(u: str) -> str:
    return {"immediate": "#f85149", "today": "#e3b341", "this_week": "#3fb950", "normal": "#58a6ff"}.get(u, "#8b949e")

def channel_badge(ch: str) -> str:
    cls = ch if ch in ("online", "store", "both") else "both"
    lbl = CHANNEL_LABELS.get(ch, ch)
    return f'<span class="channel-badge {cls}">{lbl}</span>'


# ─── Header ──────────────────────────────────────────────────────────────────
st.title("🎯 Hava Durumu Bazlı Kampanya Motoru")
st.caption(
    f"⚡ Powered by Open-Meteo + Apache Kafka + Spark + Gemini LLM &nbsp;|&nbsp; "
    f"Son güncelleme: **{datetime.now().strftime('%H:%M:%S')}** &nbsp;|&nbsp; "
    f"Yenileme: {REFRESH_SEC}sn"
)

campaigns = get_campaigns()
weather_df = get_weather()
alerts     = get_alerts()

# KPI bar
k1, k2, k3, k4 = st.columns(4)
k1.metric("🎯 Aktif Kampanya",    len(campaigns))
k2.metric("🌍 İzlenen Şehir",    weather_df["city"].nunique() if not weather_df.empty else 0)
k3.metric("🚨 Aktif Alert",      len(alerts))
k4.metric("🤖 LLM Modeli",       os.getenv("GEMINI_MODEL", "gemini-1.5-flash"))

st.divider()

# ─── Sekmeler ─────────────────────────────────────────────────────────────────
tab_camp, tab_weather, tab_alerts = st.tabs([
    "🎯 Kampanya Tavsiyeleri",
    "🌤️ Hava Durumu",
    "🚨 Alert Merkezi",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1: Kampanya Tavsiyeleri
# ══════════════════════════════════════════════════════════════════════════════
with tab_camp:
    if not campaigns:
        st.info(
            "⏳ Henüz kampanya üretilmedi. \n\n"
            "**Olası nedenler:**\n"
            "- `GEMINI_API_KEY` `.env` dosyasında tanımlanmamış\n"
            "- Hava verisi Kafka'ya henüz ulaşmadı (weather producer çalışıyor mu?)\n"
            "- Campaign engine servisi başlatılıyor (ilk çalışmada ~2-3 dk bekleyin)"
        )
    else:
        # Filtreler
        cities_in_data = sorted({c.get("city", "?") for c in campaigns})
        cats_in_data   = sorted({c.get("weather_category", "?") for c in campaigns})

        col_f1, col_f2, col_f3 = st.columns([2, 2, 3])
        with col_f1:
            sel_city = st.selectbox("🏙️ Şehir", ["Tümü"] + cities_in_data)
        with col_f2:
            sel_cat = st.selectbox("🌡️ Hava Kategorisi", ["Tümü"] + cats_in_data)
        with col_f3:
            sel_urgency = st.multiselect(
                "⚡ Öncelik",
                ["immediate", "today", "this_week", "normal"],
                default=["immediate", "today", "this_week", "normal"],
            )

        filtered = [
            c for c in campaigns
            if (sel_city == "Tümü" or c.get("city") == sel_city)
            and (sel_cat == "Tümü" or c.get("weather_category") == sel_cat)
            and c.get("urgency", "normal") in sel_urgency
        ]

        st.caption(f"**{len(filtered)}** kampanya gösteriliyor")

        # Kampanya kartları — 2 kolon
        cols = st.columns(2)
        for i, camp in enumerate(reversed(filtered[-20:])):
            cat   = camp.get("weather_category", "NORMAL")
            icon  = CATEGORY_ICONS.get(cat, "🌡️")
            urgency = camp.get("urgency", "normal")
            u_col   = urgency_color(urgency)
            products= camp.get("target_products", [])
            ch      = camp.get("channel", "both")

            prod_html = "".join(f"<span>{p}</span>" for p in products)
            badge_html = channel_badge(ch)

            with cols[i % 2]:
                is_fallback = camp.get("is_fallback", False)
                fallback_warning = " <small>⚙️ Kural tabanlı</small>" if is_fallback else ""

                st.markdown(f"""
<div class="campaign-card">
  <h3>{icon} {camp.get('campaign_title', 'Kampanya')}{fallback_warning}</h3>
  <div class="meta">
    📍 {camp.get('city', '?')} &nbsp;|&nbsp;
    📅 {camp.get('weekday', '')} {camp.get('date', '')} &nbsp;|&nbsp;
    🌡️ {camp.get('temperature_c', '?')}°C · {camp.get('weather_desc', '')} &nbsp;|&nbsp;
    {badge_html} &nbsp;|&nbsp;
    <span style="color:{u_col}; font-weight:600;">
      {'🔴 Acil' if urgency=='immediate' else '🟡 Bugün' if urgency=='today' else '🟢 Bu Hafta' if urgency=='this_week' else 'Normal'}
    </span>
  </div>
  <p style="font-style:italic; margin:8px 0;">"{camp.get('campaign_message', '')}"</p>
  <div class="products">{prod_html}</div>
  <p style="font-size:12px; color:#8b949e; margin-top:10px;">
    🏷️ Hedef: {camp.get('target_segment', '?')} &nbsp;|&nbsp;
    💡 {camp.get('discount_suggestion', '')}
  </p>
  <details><summary style="font-size:11px; color:#8b949e; cursor:pointer;">🔍 Analiz</summary>
    <p style="font-size:11px; color:#8b949e;">{camp.get('reasoning', '')}</p>
  </details>
</div>
""", unsafe_allow_html=True)

        # Özet grafik
        if len(campaigns) >= 3:
            st.subheader("📊 Kampanya Dağılımı")
            cg1, cg2 = st.columns(2)
            with cg1:
                city_counts = pd.DataFrame(campaigns)["city"].value_counts().reset_index()
                city_counts.columns = ["Şehir", "Kampanya Sayısı"]
                fig = px.bar(city_counts, x="Şehir", y="Kampanya Sayısı",
                             title="Şehir Başına Kampanya", template="plotly_white",
                             color="Kampanya Sayısı", color_continuous_scale="Blues")
                st.plotly_chart(fig, use_container_width=True)
            with cg2:
                cat_counts = pd.DataFrame(campaigns)["weather_category"].value_counts().reset_index()
                cat_counts.columns = ["Kategori", "Adet"]
                fig2 = px.pie(cat_counts, values="Adet", names="Kategori",
                              title="Hava Kategorisi Dağılımı", template="plotly_white",
                              color_discrete_sequence=px.colors.qualitative.Set3)
                st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2: Hava Durumu
# ══════════════════════════════════════════════════════════════════════════════
with tab_weather:
    st.subheader("🌤️ Şehir Bazlı Anlık Hava Durumu")

    if weather_df.empty:
        st.info("⏳ Hava verisi bekleniyor... Weather producer çalışıyor mu?")
    else:
        latest = (
            weather_df
            .sort_values("event_time", ascending=False)
            .groupby("city")
            .first()
            .reset_index()
        )

        # Şehir kartları
        city_cols = st.columns(min(len(latest), 4))
        for i, row in latest.iterrows():
            cat  = row.get("weather_category", "NORMAL")
            icon = CATEGORY_ICONS.get(cat, "🌡️")
            with city_cols[i % 4]:
                st.markdown(f"""
<div class="weather-card">
  <div class="city">📍 {row['city']}</div>
  <div class="temp">{icon} {row['temperature_c']:.1f}°C</div>
  <div style="font-size:13px; margin:4px 0;">{row.get('weather_desc', '')}</div>
  <div style="font-size:11px; color:#8b949e;">
    💧 {row.get('humidity_pct', 0):.0f}% &nbsp;|&nbsp;
    💨 {row.get('wind_speed_kmh', 0):.0f} km/h
  </div>
  <div style="font-size:11px; color:#58a6ff; margin-top:4px;">{cat}</div>
</div>
""", unsafe_allow_html=True)

        # Grafikler
        wc1, wc2 = st.columns(2)
        with wc1:
            fig = px.bar(
                latest.sort_values("temperature_c"),
                x="city", y="temperature_c",
                color="temperature_c",
                color_continuous_scale="RdYlBu_r",
                title="Şehir Bazlı Sıcaklık (°C)",
                template="plotly_white",
            )
            st.plotly_chart(fig, use_container_width=True)
        with wc2:
            if "latitude" in latest.columns:
                fig2 = px.scatter_mapbox(
                    latest,
                    lat="latitude", lon="longitude",
                    color="temperature_c",
                    size="humidity_pct",
                    hover_name="city",
                    hover_data=["temperature_c", "weather_desc", "weather_category"],
                    color_continuous_scale="RdYlBu_r",
                    zoom=5,
                    mapbox_style="carto-positron",
                    title="Türkiye Hava Haritası",
                )
                st.plotly_chart(fig2, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3: Alert Merkezi
# ══════════════════════════════════════════════════════════════════════════════
with tab_alerts:
    st.subheader("🚨 Aşırı Hava Koşulu Alertleri")

    if not alerts:
        st.success("✅ Aktif alarm yok")
    else:
        for a in reversed(alerts):
            st.markdown(f"""
<div class="alert-box">
  🌡️ <strong>{a.get('city', '?')}</strong> — {a.get('temperature_c', '?')}°C
  · {a.get('weather_desc', '')}
  · Kategori: <code>{a.get('weather_category', '?')}</code>
</div>
""", unsafe_allow_html=True)

# ─── Otomatik yenileme ────────────────────────────────────────────────────────
time.sleep(REFRESH_SEC)
st.rerun()
