# ✅ Pre-Demo Setup Checklist

Gunakan checklist ini untuk memastikan semua komponen sudah siap sebelum demo 10 menit.

---

## 📋 Phase 1: Environment & Prerequisites

- [ ] Python 3.8+ installed (`python --version`)
- [ ] Docker installed (`docker --version`)
- [ ] Docker Compose installed (`docker-compose --version`)
- [ ] Git installed (`git --version`)
- [ ] Virtual environment created (`python -m venv venv`)
- [ ] Virtual environment activated (`source venv/bin/activate`)
- [ ] Requirements installed (`pip install -r requirements.txt`)

**⏱️ Estimated Time: 10-15 minutes**

---

## 🐳 Phase 2: Infrastructure Setup

### Kafka Setup
```bash
# Run in Terminal 1
docker-compose -f docker-compose-kafka.yml up -d
sleep 5  # Wait for startup
```
- [ ] Kafka container running (`docker ps | grep kafka`)
- [ ] Zookeeper container running (`docker ps | grep zookeeper`)
- [ ] Topics created (check with: `docker exec kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092`)
  - [ ] `news-api` topic exists
  - [ ] `news-rss` topic exists

### Hadoop/HDFS Setup
```bash
# Run in Terminal 2 (new terminal)
docker-compose -f docker-compose-hadoop.yml up -d
sleep 20  # Wait for HDFS initialization
```
- [ ] Namenode container running (`docker ps | grep namenode`)
- [ ] Datanode1 container running (`docker ps | grep datanode1`)
- [ ] Datanode2 container running (`docker ps | grep datanode2`)
- [ ] HDFS directories created:
  ```bash
  docker exec namenode hdfs dfs -ls -R /data/news/
  ```
  - [ ] `/data/news/api/` exists
  - [ ] `/data/news/rss/` exists
  - [ ] `/data/news/hasil/` exists
- [ ] HDFS Web UI accessible: http://localhost:9870

**⏱️ Estimated Time: 5-10 minutes (includes startup latency)**

---

## 🔑 Phase 3: API Keys Configuration

### Setup API Key (Choose One)

#### Option A: GNews
```bash
# Edit kafka/producer_api.py
# Line: GNEWS_API_KEY = 'YOUR_GNEWS_API_KEY'
# Change to: GNEWS_API_KEY = 'your_actual_key'
# Line: USE_NEWSAPI = False
```
- [ ] API key obtained from https://gnews.io
- [ ] API key set in `kafka/producer_api.py`
- [ ] `USE_NEWSAPI = False`
- [ ] Tested with curl:
  ```bash
  curl "https://gnews.io/api/v4/top-headlines?country=id&lang=id&max=3&token=YOUR_KEY"
  ```

#### Option B: NewsAPI
```bash
# Edit kafka/producer_api.py
# Line: NEWSAPI_KEY = 'YOUR_NEWSAPI_KEY'
# Change to: NEWSAPI_KEY = 'your_actual_key'
# Line: USE_NEWSAPI = True
```
- [ ] API key obtained from https://newsapi.org
- [ ] API key set in `kafka/producer_api.py`
- [ ] `USE_NEWSAPI = True`
- [ ] Tested with curl

#### Option C: Simulator (If APIs unavailable)
- [ ] Modified `producer_api.py` to use simulator
- [ ] Documented in README why simulator used

**⏱️ Estimated Time: 5-10 minutes**

---

## 🚀 Phase 4: Start Data Producers

### Terminal 3: Producer API
```bash
cd kafka
python producer_api.py
```
**Expected Output:**
```
INFO - === Producer API - NewsPulse ===
INFO - Kafka Broker: localhost:9092
INFO - Topic: news-api
INFO - Poll Interval: 600 seconds
INFO - Berhasil fetch X artikel dari [API]
INFO - Sent: [category] Article title...
```
- [ ] Producer started without errors
- [ ] Successfully fetching articles from API
- [ ] Messages showing "Sent: " in logs
- [ ] Check producer with: `docker exec kafka-broker kafka-console-consumer.sh --topic news-api --max-messages 3 --bootstrap-server localhost:9092`

### Terminal 4: Producer RSS
```bash
cd kafka
python producer_rss.py
```
**Expected Output:**
```
INFO - === Producer RSS - NewsPulse ===
INFO - Kafka Broker: localhost:9092
INFO - Topic: news-rss
INFO - Poll Interval: 300 seconds
INFO - Berhasil parse X entries dari [source]
INFO - RSS Sent: [source] Article title...
```
- [ ] Producer started without errors
- [ ] Successfully parsing RSS feeds
- [ ] Messages showing "RSS Sent: " in logs
- [ ] Check producer with: `docker exec kafka-broker kafka-console-consumer.sh --topic news-rss --max-messages 3 --bootstrap-server localhost:9092`

**⏱️ Estimated Time: 2-3 minutes**

---

## 📥 Phase 5: Start Consumer (Kafka → HDFS)

### Terminal 5: Consumer to HDFS
```bash
cd kafka
python consumer_to_hdfs.py
```
**Expected Output:**
```
INFO - === Consumer to HDFS - NewsPulse ===
INFO - Kafka Broker: localhost:9092
INFO - Topics: ['news-api', 'news-rss']
INFO - HDFS Namenode: hdfs://namenode:8020
INFO - Consumer started, listening untuk messages...
INFO - API message buffered: Article title...
INFO - Flushing X API messages to HDFS...
INFO - Updated dashboard data: live_api.json (X messages)
```
- [ ] Consumer started without errors
- [ ] Messages being buffered from both topics
- [ ] Auto-flush happening (setiap 5 menit)
- [ ] Check HDFS files:
  ```bash
  docker exec namenode hdfs dfs -ls -h /data/news/api/
  docker exec namenode hdfs dfs -ls -h /data/news/rss/
  ```
  - [ ] Files appearing in `/data/news/api/`
  - [ ] Files appearing in `/data/news/rss/`

**⏱️ Estimated Time: 1-2 minutes**
**⏳ Wait Time: 5+ minutes for first flush to HDFS**

---

## 📊 Phase 6: Run Spark Analysis

### Wait for Data
```bash
# Wait 5-10 minutes for consumer to flush data to HDFS
# Check HDFS:
docker exec namenode hdfs dfs -du -h /data/news/api/
docker exec namenode hdfs dfs -du -h /data/news/rss/
```
- [ ] HDFS has files from both topics
- [ ] Files are not empty (size > 0)

### Terminal 6: Spark Analysis
```bash
cd spark
python analysis.py
```
**Expected Output:**
```
INFO - === NewsPulse Spark Analysis ===
INFO - Spark Session initialized successfully
INFO - Reading news data from HDFS...
INFO - Loaded X records from API path
INFO - Loaded X records from RSS path
INFO - Combined dataframe has X records
INFO - === Analisis 1: Top Words dalam Judul Berita ===
INFO - Top 20 Words:
  1. 'word1' : X occurrences
  2. 'word2' : X occurrences
...
INFO - === Analisis 2: Distribusi Berita Per Sumber ===
INFO - Distribution by Source:
  Sumber1: X articles (XX%)
  Sumber2: X articles (XX%)
...
INFO - === Analisis 3: Volume Publikasi Per Jam ===
INFO - Volume per Jam (0-23):
  09:00 - 10:00 : X | ████
...
INFO - Results saved to: ./dashboard/data/spark_results.json
```
- [ ] Spark session initialized successfully
- [ ] Data loaded from HDFS
- [ ] All 3 analyses completed
- [ ] No errors in console
- [ ] Results file created:
  ```bash
  ls -la dashboard/data/spark_results.json
  ```

**⏱️ Estimated Time: 2-3 minutes**

---

## 🌐 Phase 7: Start Dashboard

### Terminal 7: Flask App
```bash
cd dashboard
python app.py
```
**Expected Output:**
```
INFO - Starting NewsPulse Dashboard
INFO - Data directory: ./dashboard/data
INFO - Dashboard running on http://localhost:5000
WARNING - Running on http://127.0.0.1:5000 Press CTRL+C to quit
```
- [ ] Flask app started without errors
- [ ] Running on http://localhost:5000
- [ ] No port conflicts

### Browser: Access Dashboard
```
http://localhost:5000
```
**Expected Visual:**
- [ ] Header shows "📰 NewsPulse - Dashboard Analisis Berita Nasional"
- [ ] Status indicator visible (green dot = connected)
- [ ] 3 panels visible:
  - [ ] Panel 1: Top 15 Kata Trending (tabel + visualization)
    - [ ] Populated dengan actual words (bukan placeholder)
    - [ ] Frequency counter shows realistic numbers
  - [ ] Panel 2: Distribusi Berita Per Sumber (chart + tabel)
    - [ ] Bar chart rendering correctly
    - [ ] Multiple sources listed
    - [ ] Percentages shown
  - [ ] Panel 3: Recent News Feed
    - [ ] Article cards showing
    - [ ] Titles, sources, timestamps visible
    - [ ] Links clickable

**⏱️ Estimated Time: 1-2 minutes**

---

## 🔄 Phase 8: Verify Auto-Refresh

### Test Auto-Refresh (30-second interval)
- [ ] Open browser DevTools (F12)
- [ ] Go to Console tab
- [ ] Wait 30 seconds
- [ ] Observe:
  - [ ] API call to `/api/data` happening
  - [ ] No errors in console
  - [ ] Timestamps updating
  - [ ] Data refreshing (if new messages in Kafka)

### Test Health Endpoints
```bash
# Terminal: Check API endpoints
curl http://localhost:5000/api/health
curl http://localhost:5000/api/data | python -m json.tool
```
- [ ] `/api/health` returns status OK
- [ ] `/api/data` returns JSON with analyses data
- [ ] All panels data present in JSON response

**⏱️ Estimated Time: 2-3 minutes**

---

## 📁 Phase 9: Verify All Files

### Repository Structure
```bash
# Check all required files exist
ls -la *.md *.py *.yml *.env
ls -la kafka/*.py
ls -la spark/analysis.py
ls -la dashboard/app.py
ls -la dashboard/templates/index.html
ls -la dashboard/static/style.css
```
- [ ] README.md (comprehensive)
- [ ] API_KEYS_SETUP.md (guide)
- [ ] PROJECT_STRUCTURE.md (overview)
- [ ] requirements.txt (dependencies)
- [ ] docker-compose-kafka.yml
- [ ] docker-compose-hadoop.yml
- [ ] hadoop.env
- [ ] startup.sh
- [ ] check_status.py
- [ ] kafka/producer_api.py
- [ ] kafka/producer_rss.py
- [ ] kafka/consumer_to_hdfs.py
- [ ] spark/analysis.py
- [ ] dashboard/app.py
- [ ] dashboard/templates/index.html
- [ ] dashboard/static/style.css
- [ ] .gitignore

### Data Files (Should exist after running)
```bash
ls -la dashboard/data/
```
- [ ] spark_results.json (exists, >100 bytes)
- [ ] live_api.json (exists, >100 bytes)
- [ ] live_rss.json (exists, >100 bytes)

**⏱️ Estimated Time: 1-2 minutes**

---

## 🔍 Phase 10: Troubleshooting & Edge Cases

### If Kafka not working:
```bash
# Check Kafka logs
docker logs kafka-broker

# Check if port 9092 is accessible
telnet localhost 9092

# Reset Kafka (if needed - WARNING: loses data)
docker-compose -f docker-compose-kafka.yml down
docker volume prune
docker-compose -f docker-compose-kafka.yml up -d
```
- [ ] Kafka issue resolved or documented

### If HDFS not working:
```bash
# Check Namenode status
docker logs namenode

# Check if Namenode web UI accessible
curl http://localhost:9870

# Check HDFS safe mode
docker exec namenode hdfs dfsadmin -safemode leave
```
- [ ] HDFS issue resolved or documented

### If Producer not sending:
```bash
# Check if API accessible
curl "https://gnews.io/api/v4/top-headlines?country=id&lang=id&max=1&token=YOUR_KEY"

# Check Kafka broker accessibility
docker exec kafka-broker bash -c 'echo "test" | kafka-console-producer.sh --topic test --broker-list localhost:9092'
```
- [ ] Producer issue resolved or documented

### If Spark not reading:
```bash
# Check HDFS files
docker exec namenode hdfs dfs -ls /data/news/api/

# Check file content
docker exec namenode hdfs dfs -cat /data/news/api/news_api_*.json | head -c 200
```
- [ ] Spark issue resolved or documented

### If Dashboard not updating:
```bash
# Check if JSON files exist and updated
ls -la dashboard/data/
tail dashboard/data/spark_results.json

# Check Flask logs in terminal
# Look for errors in /api/data endpoint
```
- [ ] Dashboard issue resolved or documented

---

## 🎬 Phase 11: Demo Preparation (Last 30 minutes before)

### Screenshot Verification
```bash
# Take screenshots of:
# 1. HDFS Web UI (http://localhost:9870)
#    - Screenshot showing /data/news directories
# 2. Kafka topics & data
#    - Screenshot showing messages in kafka-console-consumer
# 3. Dashboard (http://localhost:5000)
#    - Screenshot showing all 3 panels
```
- [ ] Screenshot 1: HDFS Web UI saved
- [ ] Screenshot 2: Kafka data saved
- [ ] Screenshot 3: Dashboard saved

### Demo Script Ready
```bash
# Prepare talking points:
# 1. What is NewsPulse? (20 seconds)
# 2. Show architecture diagram (30 seconds)
# 3. Run through each component (3 minutes)
# 4. Show dashboard (1 minute)
# 5. Q&A (3 minutes)
```
- [ ] Script memorized or written
- [ ] Laptop display settings OK (resolution)
- [ ] Internet connection stable
- [ ] All terminals positioned for visibility

### Code Documentation
```bash
# Ensure code has comments explaining:
# - Each major function
# - Complex logic
# - Data transformations
```
- [ ] Code readable and commented
- [ ] README explains each component
- [ ] Troubleshooting guide complete

---

## ✅ FINAL VERIFICATION CHECKLIST

```
INFRASTRUCTURE:
[ ] Kafka running (broker + zookeeper)
[ ] HDFS running (namenode + 2 datanode)
[ ] Network connectivity OK

DATA PIPELINE:
[ ] Producer API sending messages
[ ] Producer RSS sending articles
[ ] Consumer reading from Kafka
[ ] Data flushing to HDFS
[ ] Both topics have files in HDFS

ANALYTICS:
[ ] Spark reading from HDFS successfully
[ ] All 3 analyses completed
[ ] Results file populated with data

DASHBOARD:
[ ] Flask running at localhost:5000
[ ] All 3 panels displaying data
[ ] Auto-refresh working (30 sec interval)
[ ] No JavaScript errors (check DevTools)

REPOSITORY:
[ ] All files committed to git
[ ] .gitignore properly configured
[ ] README complete
[ ] No API keys hardcoded
[ ] Data folders not committed

DOCUMENTATION:
[ ] README.md complete
[ ] API_KEYS_SETUP.md done
[ ] PROJECT_STRUCTURE.md done
[ ] Code comments in place
[ ] Screenshots taken
```

---

## 🎯 Time Breakdown

| Phase | Tasks | Est. Time | Cumulative |
|-------|-------|-----------|-----------|
| 1 | Setup Env | 10-15 min | 10-15 min |
| 2 | Infrastructure | 5-10 min | 15-25 min |
| 3 | API Keys | 5-10 min | 20-35 min |
| 4 | Producers | 2-3 min | 22-38 min |
| 5 | Consumer | **+5-10 min wait** | 27-48 min |
| 6 | Spark | 2-3 min | 29-51 min |
| 7 | Dashboard | 1-2 min | 30-53 min |
| 8-10 | Verification | 5-8 min | 35-61 min |
| 11 | Demo Prep | 20-30 min | 55-91 min |
| | **TOTAL** | **~90 minutes** | |

---

## 🏁 Ready for Demo!

Once all checkmarks are ✅, your system is **READY FOR DEMO**.

**Questions? See:**
- README.md - Overview & setup
- API_KEYS_SETUP.md - API configuration
- PROJECT_STRUCTURE.md - Component details
- Troubleshooting sections above - Common issues

---

**Good luck with your ETS demo! 🚀**

Last Updated: 2026-04-27
