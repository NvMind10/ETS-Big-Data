# Setup API Keys - NewsPulse

Panduan lengkap untuk setup API keys yang dibutuhkan oleh Producer API.

---

## 🔑 Option A: GNews API (Recommended)

### Kelebihan:
- ✅ Gratis 100 requests/hari untuk free tier
- ✅ Response time cepat
- ✅ Data coverage luas (berita nasional Indonesia)
- ✅ Dokumentasi lengkap

### Langkah Setup:

#### 1. Daftar di GNews
- Kunjungi: https://gnews.io
- Klik "Sign Up"
- Isi email dan password
- Verify email Anda

#### 2. Get API Key
- Login ke https://gnews.io/dashboard
- Copy API Key Anda (sudah auto-generated)
- API Key terlihat seperti: `3a1b2c3d4e5f...`

#### 3. Setup di Project
- Edit file: `kafka/producer_api.py`
- Cari baris: `GNEWS_API_KEY = 'YOUR_GNEWS_API_KEY'`
- Ganti dengan: `GNEWS_API_KEY = 'your_actual_key_here'`
- Pastikan: `USE_NEWSAPI = False`

#### 4. Test API
```bash
# Test dengan curl
curl "https://gnews.io/api/v4/top-headlines?country=id&lang=id&max=3&token=YOUR_KEY"
```

### Rate Limiting:
- Free tier: 100 requests/24 jam (cukup untuk project ini)
- Interval polling di code: 10 menit = 144 requests/24 jam (aman)

---

## 🔑 Option B: NewsAPI

### Kelebihan:
- ✅ Gratis 100 requests/hari
- ✅ Data lebih terstruktur
- ✅ Support banyak negara termasuk Indonesia

### Langkah Setup:

#### 1. Daftar di NewsAPI
- Kunjungi: https://newsapi.org
- Klik "Register"
- Isi email dan password
- Verify email

#### 2. Get API Key
- Login ke https://newsapi.org/account
- Copy API Key Anda

#### 3. Setup di Project
- Edit file: `kafka/producer_api.py`
- Cari baris: `NEWSAPI_KEY = 'YOUR_NEWSAPI_KEY'`
- Ganti dengan: `NEWSAPI_KEY = 'your_actual_key_here'`
- Ubah: `USE_NEWSAPI = True`

#### 4. Test API
```bash
# Test dengan curl
curl "https://newsapi.org/v2/top-headlines?country=id&apiKey=YOUR_KEY"
```

---

## 🔑 Option C: Simulator Mode (Jika API tidak bisa diakses)

### Kapan Gunakan:
- ❌ API tidak accessible (blokir ISP, etc.)
- ❌ API quota sudah habis
- ❌ Testing tanpa API dependency

### Cara Implementasi:

Buat fungsi `simulate_articles()` di `kafka/producer_api.py`:

```python
def fetch_articles_simulated():
    """
    Generate synthetic news data untuk testing
    """
    import random
    
    news_templates = [
        {"title": "Rupiah melemah terhadap dolar AS", "source": "GNews"},
        {"title": "Pemerintah luncurkan program kesehatan gratis", "source": "GNews"},
        {"title": "Presiden menghadiri rapat kabinet", "source": "Tempo"},
        {"title": "Ekonomi digital tumbuh 25% tahun ini", "source": "Kompas"},
        {"title": "Olahraga sepak bola Indonesia raih prestasi", "source": "GNews"},
    ]
    
    descriptions = [
        "Berita terbaru tentang perkembangan ekonomi nasional",
        "Informasi terkini dari pemerintah pusat",
        "Laporan perkembangan sektor industri",
    ]
    
    articles = []
    for i in range(random.randint(5, 10)):
        article = {
            'title': random.choice(news_templates)['title'],
            'description': random.choice(descriptions),
            'url': f'https://example.com/article-{i}',
            'image': 'https://example.com/image.jpg',
            'source': {'name': random.choice(['GNews', 'Tempo', 'Kompas'])},
            'publishedAt': datetime.utcnow().isoformat()
        }
        articles.append(article)
    
    return articles
```

### Keuntungan Simulator:
- ✅ Tidak perlu API key
- ✅ Bisa test kapan saja tanpa rate limit
- ✅ Data deterministik untuk testing

### Kekurangan:
- ❌ Data tidak real (dummy)
- ❌ Tidak ada value bisnis

---

## 🔐 Security Best Practices

### ⚠️ PENTING: Jangan Commit API Keys!

❌ **JANGAN:**
```python
GNEWS_API_KEY = 'abc123def456'  # HARDCODED! Bahaya!
```

✅ **LAKUKAN:**
```python
# Option 1: Environment variable
import os
GNEWS_API_KEY = os.getenv('GNEWS_API_KEY', 'YOUR_GNEWS_API_KEY')

# Option 2: .env file
from dotenv import load_dotenv
load_dotenv()
GNEWS_API_KEY = os.getenv('GNEWS_API_KEY')

# Option 3: Config file (jangan commit)
# data/config.json
```

### Setup Environment Variable:

#### Linux/Mac:
```bash
# Add ke ~/.bashrc atau ~/.zshrc
export GNEWS_API_KEY='your_key_here'
export NEWSAPI_KEY='your_key_here'

# Reload shell
source ~/.bashrc  # atau source ~/.zshrc
```

#### Windows (PowerShell):
```powershell
# Permanent
[Environment]::SetEnvironmentVariable("GNEWS_API_KEY", "your_key_here", "User")
[Environment]::SetEnvironmentVariable("NEWSAPI_KEY", "your_key_here", "User")

# Temporary (untuk session ini saja)
$env:GNEWS_API_KEY = "your_key_here"
$env:NEWSAPI_KEY = "your_key_here"
```

#### Windows (Command Prompt):
```cmd
setx GNEWS_API_KEY your_key_here
setx NEWSAPI_KEY your_key_here
```

---

## ✅ Verification Checklist

Sebelum menjalankan producer, pastikan:

```
API Setup:
[ ] API key sudah diterima dan tercopy dengan benar
[ ] Tidak ada typo di API key
[ ] API key tidak expired
[ ] API key tidak di-commit ke git

Configuration:
[ ] producer_api.py sudah diupdate dengan API key
[ ] USE_NEWSAPI = False (jika pakai GNews)
[ ] USE_NEWSAPI = True (jika pakai NewsAPI)
[ ] POLL_INTERVAL = 600 (10 menit, aman dari rate limit)

Testing:
[ ] curl test berhasil menerima response
[ ] Response format sesuai (JSON valid)
[ ] Tidak ada error message di response
```

---

## 🔧 Troubleshooting

### Problem: "401 Unauthorized"
```
Penyebab: API key salah/expired
Solusi:
1. Double-check API key (copy paste lagi dari website)
2. Pastikan tidak ada space atau typo
3. Jika sudah 1 bulan, API key mungkin expired - generate baru
```

### Problem: "429 Too Many Requests"
```
Penyebab: Rate limit exceeded
Solusi:
1. Tunggu 24 jam untuk reset quota
2. Upgrade plan (jika ada paid tier)
3. Gunakan RSS feed sebagai backup (unlimited)
4. Gunakan simulator mode
```

### Problem: "Connection Timeout"
```
Penyebab: Network issue, API server down
Solusi:
1. Check internet connection
2. Coba ping api.gnews.io
3. Jika blocked ISP, gunakan VPN atau simulator
4. Check status API di status.gnews.io
```

### Problem: "Invalid parameter"
```
Penyebab: Query parameter tidak valid
Solusi:
1. Check dokumentasi API terbaru
2. Verifikasi parameter format (lang, country, dll)
3. Check response dari curl manual test
```

---

## 📊 API Comparison

| Aspek | GNews | NewsAPI |
|-------|-------|---------|
| **Setup Difficulty** | Easy | Easy |
| **Free Tier** | 100/hari | 100/hari |
| **Indonesian Coverage** | Excellent | Good |
| **Response Speed** | Fast | Medium |
| **Documentation** | Good | Excellent |
| **Recommendation** | ✅ Primary | Backup |

---

## 📝 Contoh RSS Feed (Backup)

Jika kedua API tidak bisa diakses, RSS feed tidak perlu API key:

```
Kompas Nasional:
https://rss.kompas.com/feed/kompas.com/nasional

Tempo Nasional:
https://rss.tempo.co/nasional

Kompas Bisnis:
https://rss.kompas.com/feed/kompas.com/money

Tempo Bisnis:
https://rss.tempo.co/bisnis
```

RSS polling tidak ada rate limit, jadi bisa di-set interval lebih sering (5 menit).

---

## 🎓 Learning Resources

- [GNews API Docs](https://gnews.io/docs)
- [NewsAPI Docs](https://newsapi.org/docs)
- [Feedparser Docs](https://pythonhosted.org/feedparser/)
- [Requests Library](https://requests.readthedocs.io/)

---

**Last Updated: 2026-04-27**
