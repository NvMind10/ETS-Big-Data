# 📁 Project Structure & Component Overview

## Directory Tree
```
kelompok-newspulse/
│
├── 📄 README.md                          ← START HERE! Dokumentasi lengkap
├── 📄 API_KEYS_SETUP.md                  ← Setup panduan untuk API keys
├── 📄 requirements.txt                   ← Python dependencies
├── 📄 .gitignore                         ← Git ignore rules
│
├── 🐳 docker-compose-kafka.yml           ← Kafka setup (Zookeeper + Broker)
├── 🐳 docker-compose-hadoop.yml          ← Hadoop setup (Namenode + 2 Datanode)
├── 📄 hadoop.env                         ← Hadoop environment variables
│
├── 🚀 startup.sh                         ← Quick startup script
├── 🔍 check_status.py                    ← Health check script
│
├── 📁 kafka/
│   ├── producer_api.py                   ← ★★★ Producer: GNews/NewsAPI → Kafka
│   ├── producer_rss.py                   ← ★★★ Producer: RSS Feeds → Kafka
│   └── consumer_to_hdfs.py               ← ★★★ Consumer: Kafka → HDFS
│
├── 📁 spark/
│   ├── analysis.py                       ← ★★★ Spark: HDFS → 3 Analisis
│   └── analysis.ipynb                    ← Optional: Jupyter notebook version
│
├── 📁 dashboard/
│   ├── app.py                            ← ★★★ Flask: API + Web Server
│   ├── templates/
│   │   └── index.html                    ← ★★★ UI: 3 Panel Dashboard
│   ├── static/
│   │   └── style.css                     ← Styling & responsiveness
│   └── 📁 data/                          ← (Generated at runtime)
│       ├── spark_results.json            ← Spark output (analisis)
│       ├── live_api.json                 ← Live API data
│       └── live_rss.json                 ← Live RSS data
│
├── 📁 data_buffer/                       ← (Generated at runtime)
│   └── news_*.json                       ← Consumer buffer files
│
└── .git/                                 ← Git repository
```

---

## 🔧 Core Components

### 1. KAFKA PRODUCERS
**File:** `kafka/producer_api.py` & `kafka/producer_rss.py`

**Fungsi:**
- Fetch berita dari eksternal sumber (API/RSS)
- Transform ke format JSON yang konsisten
- Send ke Kafka topics

**Producers:**
```
GNews API / NewsAPI     RSS Feeds (Kompas, Tempo)
        │                        │
        └────────┬───────────────┘
                 ▼
        ┌─────────────────────┐
        │  Producer API.py    │ → Topic: news-api
        │  Producer RSS.py    │ → Topic: news-rss
        └─────────────────────┘
```

**Key Features:**
- ✅ Periodic polling (10 menit untuk API, 5 menit untuk RSS)
- ✅ Deduplication (track URL yang sudah dikirim)
- ✅ Error handling & retry logic
- ✅ Idempotent producers (tidak duplikat meskipun crash)
- ✅ Logging lengkap

**Data Format:**
```json
{
  "title": "Presiden luncurkan program ekonomi",
  "url": "https://...",
  "summary": "Berita singkat...",
  "source": "Kompas",
  "published_at": "2026-04-27T10:30:00Z",
  "timestamp": "2026-04-27T10:35:22Z"
}
```

---

### 2. KAFKA CONSUMER
**File:** `kafka/consumer_to_hdfs.py`

**Fungsi:**
- Baca dari 2 Kafka topics (news-api, news-rss)
- Buffer messages dalam memory
- Flush ke HDFS setiap 5 menit

**Consumer Flow:**
```
Kafka Topics
├── news-api
└── news-rss
    │
    ▼
Consumer to HDFS.py
├── Read dari Kafka (2 topic parallel)
├── Buffer messages
├── Setiap 5 menit:
│   ├── Save API messages → /data/news/api/
│   ├── Save RSS messages → /data/news/rss/
│   └── Update dashboard data
└── Close gracefully on signal
```

**Key Features:**
- ✅ Parallel consumption dari 2 topics
- ✅ Buffering untuk efisiensi I/O
- ✅ Periodic flush dengan timestamp
- ✅ Dashboard sync (live data)
- ✅ HDFS integration (hdfs dfs -put)

**File Output (HDFS):**
```
/data/news/api/news_api_2026-04-27_10-35-22.json
/data/news/rss/news_rss_2026-04-27_10-40-15.json
```

**File Output (Dashboard):**
```
dashboard/data/live_api.json     ← Latest 50 API messages
dashboard/data/live_rss.json     ← Latest 50 RSS messages
```

---

### 3. HADOOP HDFS
**Files:** `docker-compose-hadoop.yml`, `hadoop.env`

**Infrastruktur:**
```
┌──────────────────────────────┐
│      Hadoop Cluster          │
├──────────────────────────────┤
│  Namenode (port 9870)        │ ← Master
│  Datanode 1 (port 9864)      │ ← Slave 1
│  Datanode 2 (port 9865)      │ ← Slave 2
└──────────────────────────────┘
```

**Key Features:**
- ✅ 3-node cluster (1 namenode + 2 datanode)
- ✅ Replication factor = 1 (untuk development)
- ✅ Web UI di http://localhost:9870
- ✅ Persistent volumes (data tersimpan)

**Directory Structure:**
```
/data/news/
├── api/          ← JSON files dari GNews/NewsAPI
├── rss/          ← JSON files dari RSS feeds
└── hasil/        ← Output dari Spark analysis
```

---

### 4. APACHE SPARK
**File:** `spark/analysis.py`

**Fungsi:**
- Baca semua file JSON dari HDFS
- Jalankan 3 analisis data
- Output hasil ke JSON + HDFS

**Pipeline Analisis:**

```
HDFS Files (api + rss)
│
▼
Spark Read JSON
│
├─► Analisis 1: Top Words
│   ├── Split title jadi words
│   ├── Filter stopwords
│   ├── Count frekuensi
│   └── Top 20 words
│
├─► Analisis 2: Source Distribution
│   ├── Group by source
│   ├── Count artikel per source
│   └── Calculate percentage
│
└─► Analisis 3: Volume per Jam
    ├── Extract hour from timestamp
    ├── Group by jam
    └── Count artikel per jam
```

**Output Format:**
```json
{
  "timestamp": "2026-04-27T10:40:00Z",
  "analyses": {
    "top_words": [
      {"word": "presiden", "count": 12},
      {"word": "pemerintah", "count": 10}
    ],
    "source_distribution": [
      {"source": "Kompas", "jumlah_artikel": 45, "persentase": 54.22},
      {"source": "Tempo", "jumlah_artikel": 38, "persentase": 45.78}
    ],
    "volume_by_hour": [
      {"jam": 9, "jumlah_artikel": 8},
      {"jam": 10, "jumlah_artikel": 15}
    ]
  }
}
```

**Key Features:**
- ✅ DataFrame API + Spark SQL
- ✅ Distributed processing
- ✅ 3 analisis berbeda
- ✅ Error handling untuk missing data
- ✅ Output ke file lokal + HDFS

---

### 5. FLASK DASHBOARD
**Files:** `dashboard/app.py`, `dashboard/templates/index.html`, `dashboard/static/style.css`

**Fungsi:**
- Serve web UI di http://localhost:5000
- Load hasil analisis Spark
- Load live data dari Kafka
- Merge + display di 3 panel

**Dashboard Panels:**

```
┌─────────────────────────────────────────┐
│   📊 Panel 1: Top 15 Trending Words     │
│   ├─ Tabel word + frekuensi             │
│   └─ Bar visualization                  │
├─────────────────────────────────────────┤
│   📈 Panel 2: Distribution Per Sumber   │
│   ├─ Bar chart (Chart.js)               │
│   └─ Tabel sumber + jumlah + %          │
├─────────────────────────────────────────┤
│   📰 Panel 3: Recent News Feed          │
│   ├─ List berita terbaru (20 items)     │
│   ├─ Source badge, timestamp, snippet   │
│   └─ Link ke artikel asli               │
└─────────────────────────────────────────┘
```

**Features:**
- ✅ Auto-refresh setiap 30 detik
- ✅ Real-time status indicator
- ✅ Responsive design (mobile-friendly)
- ✅ Chart.js untuk visualisasi
- ✅ Beautiful UI dengan gradient background

**Endpoints:**
```
GET  /                    → Serve dashboard HTML
GET  /api/data            → Return JSON (semua data)
GET  /api/health          → Health check status
```

---

## 🔄 Data Flow (End-to-End)

```
┌─────────────────────────────────────────────────────────────┐
│                    COMPLETE PIPELINE                        │
└─────────────────────────────────────────────────────────────┘

MINUTE 0-10:
  [GNews API] ──┐
                 ├─→ [Kafka Topic: news-api]     (50 messages)
  [NewsAPI]  ──┘

MINUTE 0-5:
  [RSS Feeds] ──→ [Kafka Topic: news-rss]        (30 messages)

MINUTE 0-5 (Parallel):
  [Kafka Topics] ──→ [Consumer] ──→ Buffer in memory

MINUTE 5:
  [Buffer] ──→ [HDFS] ──→ /data/news/api/news_api_*.json
  [Buffer] ──→ [HDFS] ──→ /data/news/rss/news_rss_*.json
  [Buffer] ──→ [Local] ──→ dashboard/data/live_api.json
             ──→ [Local] ──→ dashboard/data/live_rss.json

MINUTE 5-30:
  [HDFS Files] ──→ [Spark] ──→ 3 Analisis
                               ├─ Top words
                               ├─ Source distribution
                               └─ Hourly volume

MINUTE 30:
  [Spark Output] ──→ [Local] ──→ dashboard/data/spark_results.json
  [Spark Output] ──→ [HDFS] ──→ /data/news/hasil/analysis_*.json

ALWAYS (Live):
  [Dashboard] ──→ Fetch /api/data ──→ Merge Spark + Live Data
                                   ──→ Display UI (auto-refresh 30s)

BROWSER:
  http://localhost:5000 ──→ 3 Panels with real-time data
```

---

## 📊 Data Volume Expectations

### Per 30 Menit:
```
Kafka Input:
- news-api:  ~30 messages (GNews polling @10min interval)
- news-rss:  ~30 messages (RSS polling @5min interval)
Total:       ~60 messages

HDFS Storage (24 hours):
- api/: 144 files × ~5KB = 720 KB
- rss/: 288 files × ~4KB = 1.1 MB
Total: ~2 MB (very small)

Kafka Lag (steady state):
- Should be near 0 (all messages consumed)
```

---

## 🔐 Security & Best Practices

### API Keys:
- ❌ Never hardcode API keys
- ✅ Use environment variables
- ✅ Documented in API_KEYS_SETUP.md

### Data Handling:
- ✅ Deduplication di producer
- ✅ Error handling & retry
- ✅ Idempotent operations

### Deployment:
- ✅ Docker for infrastructure
- ✅ Consistent environments
- ✅ Easy to reproduce

---

## 🧪 Testing Checklist

```
UNIT TESTS:
[ ] Producer API - dapat fetch dari API
[ ] Producer RSS - dapat parse feed
[ ] Consumer - dapat read dari Kafka
[ ] Spark - dapat read dari HDFS
[ ] Dashboard - endpoint respond correctly

INTEGRATION TESTS:
[ ] Kafka → HDFS message end-to-end
[ ] Spark → Dashboard data pipeline
[ ] Complete pipeline dari API → Dashboard

PERFORMANCE TESTS:
[ ] Throughput: 60 messages/5min (OK)
[ ] Latency: <5 min dari API → Dashboard (OK)
[ ] Storage: <2MB per 24 jam (OK)
```

---

## 📝 Code Quality

### Logging:
- ✅ INFO, WARNING, ERROR levels
- ✅ Timestamp + context
- ✅ Helpful for debugging

### Documentation:
- ✅ Docstrings untuk functions
- ✅ Comments untuk logic
- ✅ README + API_KEYS_SETUP.md

### Error Handling:
- ✅ Try-except blocks
- ✅ Graceful degradation
- ✅ Clear error messages

---

## 📖 How to Read Code

**Start dengan:**
1. README.md - Overview & setup
2. API_KEYS_SETUP.md - API configuration
3. docker-compose files - Infrastructure

**Then dive into:**
1. kafka/producer_api.py - Producer logic
2. kafka/consumer_to_hdfs.py - Consumer logic
3. spark/analysis.py - Analytics logic
4. dashboard/app.py - Web API logic

**Each file has:**
- Docstrings explaining purpose
- Comments explaining complex logic
- Logging untuk tracking execution

---

## 🚀 Next Steps

1. ✅ Setup API keys (API_KEYS_SETUP.md)
2. ✅ Run startup script
3. ✅ Monitor with check_status.py
4. ✅ Wait 5-10 min untuk data accumulate
5. ✅ View dashboard at http://localhost:5000
6. ✅ Check logs untuk troubleshooting

---

**Project Complete! Ready for Demo 🎉**
