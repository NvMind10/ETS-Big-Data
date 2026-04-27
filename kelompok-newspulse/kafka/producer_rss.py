#!/usr/bin/env python3
"""
Producer RSS - Mengambil berita dari RSS Feed Kompas dan Tempo
Topik: NewsPulse - Analisis Tren Berita Nasional
"""

import json
import time
import logging
import hashlib
from datetime import datetime
from kafka import KafkaProducer
import feedparser
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'news-rss'
POLL_INTERVAL = 300  # 5 menit

RSS_FEEDS = {
    'Kompas Nasional': 'https://rss.kompas.com/feed/kompas.com/nasional',
    'Tempo Nasional': 'https://rss.tempo.co/nasional',
}

# Backup feeds jika salah satu tidak bisa diakses
RSS_FEEDS_BACKUP = {
    'Kompas Ekonomi': 'https://rss.kompas.com/feed/kompas.com/money',
    'Tempo Bisnis': 'https://rss.tempo.co/bisnis',
}


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


def parse_rss_feed(feed_url, source_name):
    """
    Parse RSS feed dan return list of entries
    """
    try:
        feed = feedparser.parse(feed_url)
        
        if feed.bozo:
            logger.warning(f"Feed {source_name} memiliki parsing issue: {feed.bozo_exception}")
        
        entries = feed.entries
        logger.info(f"Berhasil parse {len(entries)} entries dari {source_name}")
        return entries
        
    except Exception as e:
        logger.error(f"Error parsing RSS feed {source_name}: {e}")
        return []


def generate_url_hash(url):
    """Generate hash 8 karakter dari URL untuk dijadikan key Kafka"""
    return hashlib.md5(url.encode()).hexdigest()[:8]


def parse_entry(entry, source_name):
    """
    Parse RSS entry ke format JSON yang konsisten
    """
    try:
        # Extract fields dari entry
        title = entry.get('title', '')
        link = entry.get('link', '')
        summary = entry.get('summary', '')
        published = entry.get('published', '')
        
        # Parse published time
        published_time = None
        if published:
            try:
                # RSS feed biasanya dalam format RFC 2822
                from email.utils import parsedate_to_datetime
                published_time = parsedate_to_datetime(published).isoformat()
            except:
                published_time = published
        
        event = {
            'title': title,
            'url': link,
            'summary': summary,
            'source': source_name,
            'published_at': published_time or datetime.utcnow().isoformat(),
            'timestamp': datetime.utcnow().isoformat(),
            'rss_source': 'rss'
        }
        
        return event
    except Exception as e:
        logger.error(f"Error parsing entry: {e}")
        return None


def extract_category_from_url(url):
    """
    Ekstrak kategori dari URL untuk dijadikan key Kafka
    """
    url_lower = url.lower()
    
    if 'nasional' in url_lower or 'politik' in url_lower:
        return 'politics'
    elif 'bisnis' in url_lower or 'ekonomi' in url_lower or 'money' in url_lower:
        return 'economy'
    elif 'olahraga' in url_lower:
        return 'sports'
    elif 'teknologi' in url_lower or 'digital' in url_lower:
        return 'technology'
    elif 'hiburan' in url_lower or 'entertainment' in url_lower:
        return 'entertainment'
    elif 'kesehatan' in url_lower or 'health' in url_lower:
        return 'health'
    else:
        return 'general'


def produce_rss_articles(producer, feed_urls):
    """
    Produce RSS artikel ke Kafka topic secara periodik
    """
    processed_urls = set()  # Track URL yang sudah diproses
    
    while True:
        try:
            total_sent = 0
            
            for source_name, feed_url in feed_urls.items():
                try:
                    # Parse RSS feed
                    entries = parse_rss_feed(feed_url, source_name)
                    
                    if not entries:
                        continue
                    
                    # Process setiap entry
                    for entry in entries:
                        url = entry.get('link', '') or entry.get('url', '')
                        
                        if not url:
                            continue
                        
                        # Hindari duplikat
                        if url in processed_urls:
                            logger.debug(f"Skip duplikat RSS: {url}")
                            continue
                        
                        processed_urls.add(url)
                        
                        # Parse entry
                        event = parse_entry(entry, source_name)
                        
                        if event is None:
                            continue
                        
                        # Generate key berdasarkan hash URL
                        key = generate_url_hash(url)
                        
                        # Send ke Kafka
                        try:
                            producer.send(
                                KAFKA_TOPIC,
                                key=key,
                                value=event
                            )
                            total_sent += 1
                            logger.info(f"RSS Sent: [{source_name}] {event['title'][:70]}")
                        except Exception as e:
                            logger.error(f"Error sending RSS to Kafka: {e}")
                
                except Exception as e:
                    logger.error(f"Error processing feed {source_name}: {e}")
            
            logger.info(f"RSS cycle selesai: {total_sent} artikel dikirim ke topic {KAFKA_TOPIC}")
            
            # Tahan beberapa URL terakhir untuk menghindari memory leak
            if len(processed_urls) > 2000:
                processed_urls = set(list(processed_urls)[-1000:])
            
            # Tunggu sampai interval berikutnya
            logger.info(f"Waiting {POLL_INTERVAL} seconds sampai poll berikutnya...")
            time.sleep(POLL_INTERVAL)
            
        except KeyboardInterrupt:
            logger.info("RSS Producer dihentikan oleh user")
            break
        except Exception as e:
            logger.error(f"Unexpected error in RSS producer: {e}")
            time.sleep(POLL_INTERVAL)


def main():
    logger.info("=== Producer RSS - NewsPulse ===")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Poll Interval: {POLL_INTERVAL} seconds")
    logger.info(f"RSS Feeds:")
    for name, url in RSS_FEEDS.items():
        logger.info(f"  - {name}: {url}")
    
    producer = get_producer()
    
    if producer is None:
        logger.error("Tidak bisa terhubung ke Kafka. Pastikan Kafka sudah running!")
        return
    
    try:
        produce_rss_articles(producer, RSS_FEEDS)
    finally:
        producer.close()
        logger.info("RSS Producer ditutup")


if __name__ == '__main__':
    main()
