# NewsPulse - Big Data Pipeline End-to-End
## ETS Praktik Big Data dan Data Lakehouse - Topik 5

---

## 📋 Informasi Proyek

| Aspek | Detail |
|-------|--------|
| **Topik** | NewsPulse: Analisis Tren Berita Nasional |
| **Pertanyaan Bisnis** | Topik apa yang paling hangat hari ini di berbagai media, dan jam berapa biasanya berita dominan muncul? |
| **Durasi Pengerjaan** | 2 minggu |
| **Deliverable** | GitHub Repository + Demo Live 10 menit |

---

## 👥 Anggota Kelompok

> **INSTRUKSI:** Update dengan nama anggota kelompok Anda dan kontribusi masing-masing

| Nama | NRP |
|------|-----|
| Abiyyu Raihan Putra Wikanto | 5027241042 |
| Daniswara Fausta Novanto | 5027241050 |
| Muhammad Khairul Yahya | 5027241092 |
| Muhammad Fachry Salahhudin Rusamsyi | 5027241031 |
| Muhammad Huda Rabbani | 5027241098 |

---

## 🏗️ Arsitektur Sistem

```
╔═══════════════════════════════════════════════════════════════════╗
║              NEWSPULSE BIG DATA PIPELINE ARCHITECTURE             ║
╠═══════════════════════════════════════════════════════════════════╣
║                                                                   ║
║  [GNews API]          [NewsAPI]          [RSS Feeds]              ║
║  (Berita)             (Alternatif)       (Kompas + Tempo)         ║
║       │                    │                   │                  ║
║       ▼                    ▼                   ▼                  ║
║  ┌─────────┐          ┌─────────┐         ┌─────────┐             ║
║  │Producer │          │Producer │         │Producer │             ║
║  │  API    │          │ NewsAPI │         │  RSS    │             ║
║  └────┬────┘          └────┬────┘         └────┬────┘             ║
║       │                    │                   │                  ║
║       └────────┬───────────┴───────────────────┘                  ║
║                │                                                   ║
║                ▼                                                   ║
║  ╔═════════════════════════════════════════╗                      ║
║  ║      APACHE KAFKA (Ingestion)           ║                      ║
║  ║   Topic: news-api   Topic: news-rss     ║                      ║
║  ╚═════════════╤══════════════════════════╝                       ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌──────────────┐                                           ║
║         │ Consumer to  │ ← membaca dari Kafka                      ║
║         │ HDFS.py      │ ← buffer & flush ke HDFS                  ║
║         └──────┬───────┘                                           ║
║                │                                                   ║
║                ▼                                                   ║
║  ╔═════════════════════════════════════════╗                      ║
║  ║      HADOOP HDFS (Storage)              ║                      ║
║  ║   /data/news/api/   /data/news/rss/     ║                      ║
║  ╚═════════════╤══════════════════════════╝                       ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌──────────────┐                                           ║
║         │Apache Spark  │ ← baca dari HDFS, 3 analisis              ║
║         │analysis.py   │ ← hasil ke HDFS + JSON dashboard          ║
║         └──────┬───────┘                                           ║
║                │                                                   ║
║                ▼                                                   ║
║         ┌──────────────────┐                                       ║
║         │  Dashboard Flask │                                       ║
║         │   :5000          │                                       ║
║         │  (3 panel data)  │                                       ║
║         └──────────────────┘                                       ║
║                                                                    ║
╚════════════════════════════════════════════════════════════════════╝
```

**Flow Data:**
1. **Producers** → mengambil berita dari API/RSS → kirim ke Kafka topics
2. **Consumer** → baca dari Kafka → buffer messages → flush ke HDFS (setiap 5 menit)
3. **Spark** → baca semua file JSON dari HDFS → analisis 3 topik → output JSON
4. **Dashboard** → fetch hasil Spark + live data → tampilkan di web UI

---

## 📂 Struktur Repository

```
kelompok-newspulse/
├── README.md                      ← Dokumentasi (file ini)
├── requirements.txt               ← Python dependencies
├── docker-compose-kafka.yml       ← Setup Kafka dengan Docker
├── docker-compose-hadoop.yml      ← Setup Hadoop/HDFS dengan Docker
├── hadoop.env                     ← Konfigurasi Hadoop environment
│
├── kafka/
│   ├── producer_api.py            ← Producer GNews/NewsAPI
│   ├── producer_rss.py            ← Producer RSS Feeds
│   └── consumer_to_hdfs.py        ← Consumer: Kafka → HDFS
│
├── spark/
│   ├── analysis.py                ← Spark analysis (3 analisis)
│   └── analysis.ipynb             ← Optional: Jupyter notebook version
│
├── dashboard/
│   ├── app.py                     ← Flask web app
│   ├── templates/
│   │   └── index.html             ← Dashboard UI
│   ├── static/
│   │   └── style.css              ← Styling
│   └── data/                       ← Data directory (untuk JSON hasil)
│       ├── spark_results.json     ← Output Spark analysis
│       ├── live_api.json          ← Live data dari API
│       └── live_rss.json          ← Live data dari RSS
│
└── .gitignore                     ← Exclude data & cache
```

---

## 🚀 Cara Menjalankan Sistem

### 1️⃣ **Setup Awal**

#### Prerequisite:
- **Docker & Docker Compose** sudah terinstall
- **Python 3.8+** dengan pip
- **git** untuk version control
- **HDFS CLI tools** (untuk verifikasi, optional)

#### Clone & Setup:
```bash
# Clone repository
git clone https://github.com/[username]/kelompok-newspulse-ets-bigdata.git
cd kelompok-newspulse-ets-bigdata

# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# atau
venv\\Scripts\\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

---

### 2️⃣ **Setup Infrastructure (Docker)**

#### Start Kafka:
```bash
# Terminal 1
docker-compose -f docker-compose-kafka.yml up -d

# Verify Kafka running
docker ps | grep kafka

# Create topics (jika belum ada)
docker exec -it kafka-broker kafka-topics.sh \\
  --create --topic news-api \\
  --bootstrap-server localhost:9092 \\
  --partitions 3 --replication-factor 1

docker exec -it kafka-broker kafka-topics.sh \\
  --create --topic news-rss \\
  --bootstrap-server localhost:9092 \\
  --partitions 3 --replication-factor 1

# Verify topics
docker exec -it kafka-broker kafka-topics.sh \\
  --list --bootstrap-server localhost:9092
```

#### Start Hadoop/HDFS:
```bash
# Terminal 2 (buka terminal baru)
docker-compose -f docker-compose-hadoop.yml up -d

# Verify HDFS running
docker ps | grep namenode
docker ps | grep datanode

# Wait 10-20 seconds untuk HDFS initialization
sleep 15

# Create HDFS directories
docker exec -it namenode hdfs dfs -mkdir -p /data/news/api
docker exec -it namenode hdfs dfs -mkdir -p /data/news/rss
docker exec -it namenode hdfs dfs -mkdir -p /data/news/hasil

# Verify directories
docker exec -it namenode hdfs dfs -ls -R /data/news/
```

---

### 3️⃣ **Setup API Keys** (PENTING!)

#### Option A: GNews API (Recommended - Gratis)
```bash
# 1. Daftar di https://gnews.io
# 2. Copy API key
# 3. Edit kafka/producer_api.py
#    - Set: GNEWS_API_KEY = 'your_key_here'
#    - Set: USE_NEWSAPI = False
```

#### Option B: NewsAPI (Gratis alternatif)
```bash
# 1. Daftar di https://newsapi.org
# 2. Copy API key
# 3. Edit kafka/producer_api.py
#    - Set: NEWSAPI_KEY = 'your_key_here'
#    - Set: USE_NEWSAPI = True
```

#### Option C: Simulator Mode (Jika API tidak tersedia)
```bash
# Edit producer_api.py untuk menggunakan data simulator
# (lihat bagian komentar di code)
# Sistem akan tetap berjalan dengan data dummy yang realistis
```

---

### 4️⃣ **Jalankan Producers**

#### Terminal 3: Producer API
```bash
cd kafka
python producer_api.py

# Expected output:
# 2026-04-27 10:30:45 - __main__ - INFO - === Producer API - NewsPulse ===
# 2026-04-27 10:30:45 - __main__ - INFO - Kafka Broker: localhost:9092
# 2026-04-27 10:30:45 - __main__ - INFO - Topic: news-api
# 2026-04-27 10:30:48 - __main__ - INFO - Berhasil fetch 10 artikel dari GNews
# 2026-04-27 10:30:49 - __main__ - INFO - Sent: [economy] Rupiah melemah terhadap dolar...
# ...
```

#### Terminal 4: Producer RSS
```bash
cd kafka
python producer_rss.py

# Expected output:
# 2026-04-27 10:30:50 - __main__ - INFO - === Producer RSS - NewsPulse ===
# 2026-04-27 10:30:50 - __main__ - INFO - Kafka Broker: localhost:9092
# 2026-04-27 10:30:50 - __main__ - INFO - Topic: news-rss
# 2026-04-27 10:30:53 - __main__ - INFO - RSS Sent: [Kompas Nasional] Pemerintah...
# ...
```

---

### 5️⃣ **Jalankan Consumer**

#### Terminal 5: Consumer to HDFS
```bash
cd kafka
python consumer_to_hdfs.py

# Expected output:
# 2026-04-27 10:30:55 - __main__ - INFO - === Consumer to HDFS - NewsPulse ===
# 2026-04-27 10:30:55 - __main__ - INFO - Kafka Broker: localhost:9092
# 2026-04-27 10:30:55 - __main__ - INFO - Topics: ['news-api', 'news-rss']
# 2026-04-27 10:30:55 - __main__ - INFO - HDFS Namenode: hdfs://namenode:8020
# 2026-04-27 10:31:00 - __main__ - INFO - Consumer started, listening untuk messages...
# 2026-04-27 10:31:02 - __main__ - INFO - API message buffered: Rupiah melemah...
# 2026-04-27 10:31:05 - __main__ - INFO - Flushing 5 API messages to HDFS...
# ...
```

### ✅ Checkpoint Minggu 1:
```bash
# Verify Kafka topics punya data
docker exec -it kafka-broker kafka-console-consumer.sh \\
  --topic news-api --from-beginning --max-messages 5 \\
  --bootstrap-server localhost:9092

# Verify HDFS punya file
docker exec -it namenode hdfs dfs -ls -h /data/news/api/
docker exec -it namenode hdfs dfs -ls -h /data/news/rss/

# Cek ukuran file
docker exec -it namenode hdfs dfs -du -h /data/news/
```

---

### 6️⃣ **Jalankan Spark Analysis**

#### Terminal 6: Spark Analysis (jalankan setelah consumer flush data)
```bash
cd spark
python analysis.py

# Expected output:
# 2026-04-27 10:35:00 - __main__ - INFO - === NewsPulse Spark Analysis ===
# 2026-04-27 10:35:01 - __main__ - INFO - Spark Session initialized successfully
# 2026-04-27 10:35:05 - __main__ - INFO - Reading news data from HDFS...
# 2026-04-27 10:35:08 - __main__ - INFO - Loaded 45 records from API path
# 2026-04-27 10:35:10 - __main__ - INFO - Loaded 38 records from RSS path
# 2026-04-27 10:35:10 - __main__ - INFO - Combined dataframe has 83 records
# 2026-04-27 10:35:15 - __main__ - INFO - === Analisis 1: Top Words dalam Judul Berita ===
# 2026-04-27 10:35:16 - __main__ - INFO - Top 20 Words:
#   1. 'presiden' : 12 occurrences
#   2. 'pemerintah' : 10 occurrences
#   3. 'ekonomi' : 8 occurrences
# ...
# 2026-04-27 10:35:20 - __main__ - INFO - Results saved to: ./dashboard/data/spark_results.json
```

---

### 7️⃣ **Jalankan Dashboard**

#### Terminal 7: Flask Dashboard
```bash
cd dashboard
python app.py

# Expected output:
# 2026-04-27 10:40:00 - __main__ - INFO - Starting NewsPulse Dashboard
# 2026-04-27 10:40:00 - __main__ - INFO - Data directory: ./dashboard/data
# 2026-04-27 10:40:00 - __main__ - INFO - Dashboard running on http://localhost:5000
# 2026-04-27 10:40:02 - werkzeug - INFO - Running on http://127.0.0.1:5000
```

#### Akses Dashboard:
```
Browser: http://localhost:5000
```

**Dashboard Panels:**
- ✅ **Panel 1**: Top 15 Kata Trending (tabel + bar visualization)
- ✅ **Panel 2**: Distribusi Berita Per Sumber (bar chart + tabel)
- ✅ **Panel 3**: Berita Terbaru (feed dari API & RSS, auto-refresh 30 detik)

---

## 📊 3 Analisis yang Dilakukan (Spark)

### Analisis 1: Top Words dalam Judul Berita
**Tujuan:** Identifikasi kata-kata yang paling sering muncul di judul → topik trending

**Cara Kerja:**
- Split setiap judul artikel menjadi words
- Filter stopwords (dan, yang, di, ke, dari, untuk, dst.)
- Filter kata dengan panjang < 3 karakter
- Count frekuensi setiap kata
- Ambil top 20 kata

**Output:** Tabel kata + frekuensi

---

### Analisis 2: Distribusi Berita Per Sumber
**Tujuan:** Lihat media mana yang paling produktif publish berita

**Cara Kerja:**
- Group berita by `source` (Kompas, Tempo, GNews, dst.)
- Count jumlah artikel per sumber
- Calculate persentase

**Output:** Tabel sumber + jumlah + persentase + bar chart

---

### Analisis 3: Volume Publikasi Per Jam
**Tujuan:** Temukan pola jam publikasi berita (kapan berita dominan muncul?)

**Cara Kerja:**
- Extract `hour` dari `timestamp`
- Group by jam (0-23)
- Count jumlah artikel per jam
- Visualisasi untuk lihat pola

**Output:** Tabel jam + volume + bar chart

---

## 🧪 Testing & Verification

### Verify Kafka:
```bash
# Check topics
docker exec -it kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092

# Check consumer groups
docker exec -it kafka-broker kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list

# Check consumer group details
docker exec -it kafka-broker kafka-consumer-groups.sh \\
  --bootstrap-server localhost:9092 \\
  --group news-consumer-group \\
  --describe
```

### Verify HDFS:
```bash
# Check HDFS Web UI
# Browser: http://localhost:9870

# List directories
docker exec -it namenode hdfs dfs -ls -R /data/news/

# Check file count
docker exec -it namenode hdfs dfs -find /data/news -type f | wc -l

# Check total size
docker exec -it namenode hdfs dfs -du -s -h /data/news
```

### Verify Spark Results:
```bash
# Check if spark_results.json ada
ls -la dashboard/data/spark_results.json

# View content
cat dashboard/data/spark_results.json
```

### Verify Dashboard:
```bash
# Check if live data files ada
ls -la dashboard/data/

# Check Flask app
curl http://localhost:5000

# Check API endpoint
curl http://localhost:5000/api/data

# Check health
curl http://localhost:5000/api/health
```

---

## ⚠️ Troubleshooting

### **Producer tidak bisa connect ke Kafka**
```bash
# Problem: ECONNREFUSED pada localhost:9092
# Solution:
docker ps | grep kafka-broker
# Jika tidak ada, jalankan docker-compose up -d terlebih dahulu

# Atau check Kafka logs:
docker logs kafka-broker
```

### **Consumer tidak terima message**
```bash
# Problem: Consumer berjalan tapi tidak ada message
# Solution:
# 1. Pastikan producers sudah running
# 2. Check topic punya data:
docker exec -it kafka-broker kafka-console-consumer.sh \\
  --topic news-api --from-beginning --max-messages 1 \\
  --bootstrap-server localhost:9092

# 3. Check consumer group:
docker exec -it kafka-broker kafka-consumer-groups.sh \\
  --bootstrap-server localhost:9092 \\
  --group news-consumer-group \\
  --describe
```

### **HDFS tidak bisa write file**
```bash
# Problem: Permission denied atau file not found
# Solution:
# 1. Ensure path exists:
docker exec -it namenode hdfs dfs -mkdir -p /data/news/api

# 2. Check path permissions:
docker exec -it namenode hdfs dfs -ls -ld /data/news/

# 3. Try put file manually:
echo "test" > /tmp/test.txt
docker exec -it namenode hdfs dfs -put /tmp/test.txt /data/news/test.txt
```

### **Spark tidak bisa baca dari HDFS**
```bash
# Problem: "No such file or directory" saat Spark read
# Solution:
# 1. Verify data ada di HDFS:
docker exec -it namenode hdfs dfs -ls /data/news/api/

# 2. Jika folder kosong, tunggu sampai consumer flush data (5 menit)

# 3. Check Spark logs:
# Lihat error message di terminal saat jalankan analysis.py
```

### **Dashboard tidak menampilkan data**
```bash
# Problem: Dashboard blank atau "no data"
# Solution:
# 1. Check JSON files ada:
ls -la dashboard/data/

# 2. Check content file:
cat dashboard/data/spark_results.json

# 3. Check Flask logs di terminal

# 4. Open browser DevTools (F12) → Console untuk lihat errors
```

---

## 📈 Expected Output & Performance

### Data Volume (setelah berjalan 30 menit):
```
Kafka Topics:
  - news-api: ~30 messages
  - news-rss: ~20 messages

HDFS Storage:
  - /data/news/api/: 6-7 files, ~500 KB
  - /data/news/rss/: 5-6 files, ~400 KB

Spark Analysis:
  - Top 20 words identified
  - 5-10 sumber berita terdaftar
  - 24 jam distribution (puncak di jam tertentu)

Dashboard:
  - 15 trending words
  - Source distribution chart
  - 20 latest news items
```

---

## 🎯 Tantangan & Solusi

### **Tantangan 1: API Rate Limit**
**Problem:** GNews/NewsAPI memberikan limit requests
**Solusi:** 
- Gunakan interval 10 menit untuk API polling
- Gunakan RSS feed sebagai backup (unlimited)
- Buat simulator data jika API tidak accessible

### **Tantangan 2: Network Latency (Docker)**
**Problem:** Container tidak bisa communicate
**Solusi:**
- Pastikan network bridge `news-network` digunakan
- Use container names (kafka-broker, namenode) bukan localhost
- Check `docker network ls` dan `docker network inspect news-network`

### **Tantangan 3: Duplikat Data di Kafka**
**Problem:** Berita yang sama dikirim berkali-kali
**Solusi:**
- Producer sudah implement deduplication logic
- Gunakan `url` sebagai unique identifier

### **Tantangan 4: HDFS Disk Space**
**Problem:** Storage penuh setelah berjalan lama
**Solusi:**
- Implement data retention policy (delete files >7 hari)
- Compress file JSON sebelum simpan
- Use HDFS quota jika perlu

---

## 🔍 Monitoring & Logs

### Check Status Semua Komponen:
```bash
# Terminal baru
chmod +x check_status.sh  # (jika ada script)

# Atau manual check:
echo "=== KAFKA ===" && docker ps | grep kafka
echo "=== HADOOP ===" && docker ps | grep -E "namenode|datanode"

# Check Kafka topics
docker exec kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092

# Check Kafka consumer lag
docker exec kafka-broker kafka-consumer-groups.sh \\
  --bootstrap-server localhost:9092 \\
  --group news-consumer-group \\
  --describe
```

### View Live Logs:
```bash
# Kafka broker logs
docker logs -f kafka-broker

# Hadoop namenode logs
docker logs -f namenode

# Producer logs (dari terminal)
# Lihat output di terminal where producer running

# Consumer logs (dari terminal)
# Lihat output di terminal where consumer running

# Spark logs
# Lihat output di terminal where spark analysis running

# Flask logs
# Lihat output di terminal where flask running
```

---

## 📚 Referensi & Resources

- **Apache Kafka Documentation:** https://kafka.apache.org/documentation/
- **Hadoop HDFS Guide:** https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html
- **Apache Spark Documentation:** https://spark.apache.org/docs/latest/
- **GNews API:** https://gnews.io/docs
- **NewsAPI:** https://newsapi.org/docs
- **Flask Documentation:** https://flask.palletsprojects.com/

---

## 🎓 Pembelajaran Key

**Technical Skills Gained:**
- ✅ Setup distributed system dengan Docker Compose
- ✅ Event streaming dengan Apache Kafka
- ✅ Distributed storage dengan Hadoop HDFS
- ✅ Batch processing dengan Apache Spark
- ✅ Data visualization dengan Flask + Chart.js

**Problem-Solving Skills:**
- ✅ Debugging distributed system
- ✅ Performance optimization
- ✅ Error handling & recovery
- ✅ Monitoring & alerting

---

## ✅ Checklist Demo

Sebelum demo 10 menit, pastikan:

```
INFRASTRUCTURE:
[ ] Kafka running (3 topics: news-api, news-rss)
[ ] HDFS running (namenode + 2 datanode)
[ ] Network connectivity antara container OK

DATA PIPELINE:
[ ] Producer API berjalan, send ~5-10 messages per poll
[ ] Producer RSS berjalan, send ~5-10 articles per poll
[ ] Consumer berjalan, flush ke HDFS every 5 minutes
[ ] Verify file ada di HDFS (/data/news/api/ dan /data/news/rss/)

ANALYTICS:
[ ] Spark analysis.py berjalan tanpa error
[ ] 3 analisis selesai: top words, source distribution, hourly volume
[ ] Hasil tersimpan ke dashboard/data/spark_results.json

DASHBOARD:
[ ] Flask running di :5000
[ ] Semua 3 panel menampilkan data (bukan placeholder)
[ ] Auto-refresh berfungsi (30 detik)
[ ] Chart terlihat clear dan readable

REPOSITORY:
[ ] Semua file di-push ke GitHub
[ ] README.md lengkap dengan screenshot
[ ] .gitignore exclude data/ dan __pycache__/
```

---

## 📝 Demo Flow (10 Menit)

```
Menit 0-1: Intro & Arsitektur
  - Jelaskan business problem (NewsPulse)
  - Tunjukkan arsitektur diagram

Menit 1-3: Infrastructure Demo
  - Tunjukkan Kafka topics running
  - Tunjukkan messages in Kafka
  - Tunjukkan files in HDFS

Menit 3-6: Data Pipeline Demo
  - Jalankan/show producer berjalan
  - Jalankan/show consumer flushing
  - Tunjukkan data flow end-to-end

Menit 6-8: Analytics Demo
  - Jalankan Spark analysis
  - Tunjukkan 3 analisis results
  - Jelaskan insights

Menit 8-10: Dashboard & Q&A
  - Tunjukkan dashboard di browser
  - Tunjukkan auto-refresh berfungsi
  - Answer questions dari penguji
```

---

## 📞 Kontak & Support

Untuk pertanyaan atau issue:
1. Check troubleshooting section di README ini
2. Review logs dari masing-masing komponen
3. Diskusi dengan team members
4. Tanya ke dosen jika masalah teknis fundamental

---

**Dibuat untuk ETS Praktik Big Data dan Data Lakehouse**
**Tanggal: [YYYY-MM-DD]**
**Terakhir diupdate: [YYYY-MM-DD]**
