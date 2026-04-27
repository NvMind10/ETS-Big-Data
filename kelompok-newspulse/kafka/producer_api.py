#!/usr/bin/env python3
"""
Producer API - Mengambil berita dari GNews API
Topik: NewsPulse - Analisis Tren Berita Nasional
"""

import json
import time
import logging
from datetime import datetime, timedelta
from kafka import KafkaProducer
import requests

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'news-api'
GNEWS_API_KEY = 'YOUR_GNEWS_API_KEY'  # Daftar di gnews.io untuk mendapat key gratis
GNEWS_API_URL = 'https://gnews.io/api/v4/top-headlines'
POLL_INTERVAL = 600  # 10 menit

# Alternatif jika tidak bisa akses GNews: NewsAPI
NEWSAPI_KEY = 'YOUR_NEWSAPI_KEY'  # Daftar di newsapi.org
NEWSAPI_URL = 'https://newsapi.org/v2/top-headlines'
USE_NEWSAPI = False  # Set True jika menggunakan NewsAPI


def get_producer():
    """Inisialisasi Kafka Producer"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False, default=str).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if isinstance(k, str) else k,
            enable_idempotence=True,
            acks='all',
            retries=3
        )
        logger.info("Kafka Producer berhasil diinisialisasi")
        return producer
    except Exception as e:
        logger.error(f"Gagal menginisialisasi Kafka Producer: {e}")
        return None


def fetch_from_gnews():
    """
    Fetch berita dari GNews API
    Endpoint: https://gnews.io/api/v4/top-headlines?country=id&lang=id&max=10&token=[key]
    """
    try:
        params = {
            'country': 'id',
            'lang': 'id',
            'max': 10,
            'token': GNEWS_API_KEY,
            'sortby': 'publishedAt'
        }
        
        response = requests.get(GNEWS_API_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        articles = data.get('articles', [])
        
        logger.info(f"Berhasil fetch {len(articles)} artikel dari GNews")
        return articles
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal fetch dari GNews: {e}")
        return None


def fetch_from_newsapi():
    """
    Fetch berita dari NewsAPI.org (alternatif GNews)
    Endpoint: https://newsapi.org/v2/top-headlines?country=id&apiKey=[key]
    """
    try:
        params = {
            'country': 'id',
            'apiKey': NEWSAPI_KEY,
            'pageSize': 10,
            'sortBy': 'publishedAt'
        }
        
        response = requests.get(NEWSAPI_URL, params=params, timeout=10)
        response.raise_for_status()
        
        data = response.json()
        articles = data.get('articles', [])
        
        logger.info(f"Berhasil fetch {len(articles)} artikel dari NewsAPI")
        return articles
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Gagal fetch dari NewsAPI: {e}")
        return None


def parse_article(article, source='gnews'):
    """
    Parse artikel dari API ke format JSON yang konsisten
    """
    try:
        if source == 'gnews':
            event = {
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'image': article.get('image', ''),
                'source': article.get('source', {}).get('name', 'Unknown'),
                'published_at': article.get('publishedAt', ''),
                'timestamp': datetime.utcnow().isoformat(),
                'api_source': 'gnews'
            }
        elif source == 'newsapi':
            event = {
                'title': article.get('title', ''),
                'description': article.get('description', ''),
                'url': article.get('url', ''),
                'image': article.get('urlToImage', ''),
                'source': article.get('source', {}).get('name', 'Unknown'),
                'published_at': article.get('publishedAt', ''),
                'timestamp': datetime.utcnow().isoformat(),
                'api_source': 'newsapi'
            }
        else:
            return None
        
        return event
    except Exception as e:
        logger.error(f"Error parsing article: {e}")
        return None


def extract_category(title):
    """
    Ekstrak kategori dari judul artikel (untuk menjadi key Kafka)
    Kategorisasi sederhana berdasarkan kata kunci
    """
    title_lower = title.lower()
    
    keywords = {
        'politics': ['pilpres', 'pemilihan', 'pemerintah', 'dpr', 'presiden', 'menteri'],
        'economy': ['ekonomi', 'bisnis', 'pasar', 'saham', 'rupiah', 'inflasi', 'bank'],
        'sports': ['olahraga', 'sepak bola', 'bulu tangkis', 'tenis', 'balap'],
        'technology': ['teknologi', 'aplikasi', 'startup', 'ai', 'gadget', 'digital'],
        'entertainment': ['hiburan', 'film', 'musik', 'selebriti', 'artis'],
        'health': ['kesehatan', 'medis', 'dokter', 'rumah sakit', 'virus', 'vaksin'],
        'education': ['pendidikan', 'sekolah', 'universitas', 'mahasiswa', 'kampus'],
        'general': []
    }
    
    for category, words in keywords.items():
        if any(word in title_lower for word in words):
            return category
    
    return 'general'


def produce_articles(producer):
    """
    Produce artikel ke Kafka topic secara periodik
    """
    last_urls = set()  # Track URL yang sudah dikirim untuk menghindari duplikat
    
    while True:
        try:
            # Fetch dari API (gunakan GNews atau NewsAPI)
            if USE_NEWSAPI:
                articles = fetch_from_newsapi()
            else:
                articles = fetch_from_gnews()
            
            if articles is None:
                logger.warning("Fetch gagal, akan coba lagi di interval berikutnya")
                time.sleep(POLL_INTERVAL)
                continue
            
            # Process dan send ke Kafka
            sent_count = 0
            for article in articles:
                url = article.get('url') or article.get('link', '')
                
                # Hindari duplikat
                if url in last_urls:
                    logger.debug(f"Skip duplikat: {url}")
                    continue
                
                last_urls.add(url)
                
                # Parse artikel ke format standar
                api_source = 'newsapi' if USE_NEWSAPI else 'gnews'
                event = parse_article(article, source=api_source)
                
                if event is None:
                    continue
                
                # Extract kategori untuk dijadikan key
                category = extract_category(event['title'])
                
                # Send ke Kafka
                try:
                    producer.send(
                        KAFKA_TOPIC,
                        key=category,
                        value=event
                    )
                    sent_count += 1
                    logger.info(f"Sent: [{category}] {event['title'][:80]}")
                except Exception as e:
                    logger.error(f"Error sending to Kafka: {e}")
            
            logger.info(f"Cycle selesai: {sent_count} artikel dikirim ke topic {KAFKA_TOPIC}")
            
            # Tahan beberapa URL terakhir (max 1000) untuk menghindari memory leak
            if len(last_urls) > 1000:
                last_urls = set(list(last_urls)[-500:])
            
            # Tunggu sampai interval berikutnya
            logger.info(f"Waiting {POLL_INTERVAL} seconds sampai poll berikutnya...")
            time.sleep(POLL_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("Producer dihentikan oleh user")
            break
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            time.sleep(POLL_INTERVAL)


def main():
    logger.info("=== Producer API - NewsPulse ===")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Poll Interval: {POLL_INTERVAL} seconds")
    
    if not GNEWS_API_KEY and not USE_NEWSAPI:
        logger.warning("⚠️  API KEY belum diset! Gunakan simulator mode atau set API key terlebih dahulu")
        logger.warning("Untuk GNews: set GNEWS_API_KEY (daftar di gnews.io)")
        logger.warning("Atau set USE_NEWSAPI=True dan NEWSAPI_KEY (daftar di newsapi.org)")
    
    producer = get_producer()
    
    if producer is None:
        logger.error("Tidak bisa terhubung ke Kafka. Pastikan Kafka sudah running!")
        return
    
    try:
        produce_articles(producer)
    finally:
        producer.close()
        logger.info("Producer ditutup")


if __name__ == '__main__':
    main()
