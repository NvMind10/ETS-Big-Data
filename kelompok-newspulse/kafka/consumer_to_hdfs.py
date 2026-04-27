#!/usr/bin/env python3
"""
Consumer to HDFS - Membaca dari Kafka dan menyimpan ke HDFS
Topik: NewsPulse - Analisis Tren Berita Nasional
"""

import json
import time
import logging
import os
import subprocess
from datetime import datetime
from kafka import KafkaConsumer
import threading

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPICS = ['news-api', 'news-rss']
HDFS_NAMENODE = 'hdfs://namenode:8020'
HDFS_BASE_PATH = '/data/news'
LOCAL_BUFFER_DIR = './data_buffer'
BUFFER_FLUSH_INTERVAL = 300  # 5 menit - flush buffer ke HDFS

# Dashboard data directory
DASHBOARD_DATA_DIR = './dashboard/data'


def ensure_hdfs_paths():
    """Pastikan path HDFS sudah ada"""
    try:
        paths = [
            f'{HDFS_BASE_PATH}/api',
            f'{HDFS_BASE_PATH}/rss'
        ]
        
        for path in paths:
            # Check if path exists
            cmd = f'hdfs dfs -ls {path}'
            result = subprocess.run(cmd, shell=True, capture_output=True, timeout=5)
            
            if result.returncode != 0:
                # Path doesn't exist, create it
                create_cmd = f'hdfs dfs -mkdir -p {path}'
                subprocess.run(create_cmd, shell=True, timeout=5)
                logger.info(f"Created HDFS path: {path}")
            else:
                logger.info(f"HDFS path exists: {path}")
    
    except Exception as e:
        logger.warning(f"Error ensuring HDFS paths: {e}")


def ensure_local_dirs():
    """Pastikan direktori lokal sudah ada"""
    try:
        os.makedirs(LOCAL_BUFFER_DIR, exist_ok=True)
        os.makedirs(DASHBOARD_DATA_DIR, exist_ok=True)
        logger.info("Local directories ready")
    except Exception as e:
        logger.error(f"Error creating local directories: {e}")


def get_consumer():
    """Inisialisasi Kafka Consumer untuk kedua topics"""
    try:
        consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='news-consumer-group',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=30000  # 30 detik timeout
        )
        logger.info(f"Kafka Consumer berhasil diinisialisasi untuk topics: {KAFKA_TOPICS}")
        return consumer
    except Exception as e:
        logger.error(f"Gagal menginisialisasi Kafka Consumer: {e}")
        return None


def get_current_timestamp_filename():
    """Generate nama file berdasarkan timestamp"""
    return datetime.now().strftime('%Y-%m-%d_%H-%M-%S')


def push_to_hdfs(local_file, hdfs_path):
    """
    Kirim file dari lokal ke HDFS menggunakan hdfs dfs -put
    """
    try:
        if not os.path.exists(local_file):
            logger.warning(f"Local file tidak ada: {local_file}")
            return False
        
        # Get file size
        file_size = os.path.getsize(local_file)
        if file_size == 0:
            logger.debug(f"File kosong, skip: {local_file}")
            return False
        
        cmd = f'hdfs dfs -put {local_file} {hdfs_path}'
        result = subprocess.run(cmd, shell=True, capture_output=True, timeout=10)
        
        if result.returncode == 0:
            logger.info(f"Uploaded to HDFS: {hdfs_path}")
            return True
        else:
            error_msg = result.stderr.decode('utf-8', errors='ignore')
            logger.error(f"HDFS upload failed: {error_msg}")
            return False
    
    except subprocess.TimeoutExpired:
        logger.error(f"HDFS upload timeout: {local_file}")
        return False
    except Exception as e:
        logger.error(f"Error uploading to HDFS: {e}")
        return False


def save_to_local_and_hdfs(messages, message_type):
    """
    Simpan batch messages ke file lokal dan push ke HDFS
    message_type: 'api' atau 'rss'
    """
    try:
        if not messages:
            return
        
        timestamp = get_current_timestamp_filename()
        local_filename = f'{LOCAL_BUFFER_DIR}/news_{message_type}_{timestamp}.json'
        
        # Write to local file
        with open(local_filename, 'w', encoding='utf-8') as f:
            for msg in messages:
                json.dump(msg, f, ensure_ascii=False, default=str)
                f.write('\n')
        
        logger.info(f"Buffered {len(messages)} messages to: {local_filename}")
        
        # Upload to HDFS
        hdfs_path = f'{HDFS_BASE_PATH}/{message_type}/news_{message_type}_{timestamp}.json'
        if push_to_hdfs(local_filename, hdfs_path):
            # Remove local file setelah berhasil upload
            try:
                os.remove(local_filename)
                logger.debug(f"Deleted local file: {local_filename}")
            except:
                pass
        
    except Exception as e:
        logger.error(f"Error saving messages: {e}")


def save_latest_for_dashboard(all_messages, message_type):
    """
    Simpan messages terbaru untuk dashboard
    message_type: 'api' atau 'rss'
    """
    try:
        if not all_messages:
            return
        
        # Keep only latest 50 messages
        latest_messages = all_messages[-50:]
        
        output_file = f'{DASHBOARD_DATA_DIR}/live_{message_type}.json'
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(latest_messages, f, ensure_ascii=False, indent=2, default=str)
        
        logger.debug(f"Updated dashboard data: {output_file} ({len(latest_messages)} messages)")
    
    except Exception as e:
        logger.error(f"Error saving dashboard data: {e}")


def consume_and_save():
    """
    Consumer utama - membaca dari Kafka dan simpan ke HDFS
    """
    consumer = get_consumer()
    
    if consumer is None:
        logger.error("Tidak bisa terhubung ke Kafka!")
        return
    
    # Buffer untuk kedua topics
    api_buffer = []
    rss_buffer = []
    all_api_messages = []
    all_rss_messages = []
    
    last_flush_time = time.time()
    
    try:
        logger.info("Consumer started, listening untuk messages...")
        
        for message in consumer:
            try:
                topic = message.topic
                value = message.value
                
                # Add timestamp jika belum ada
                if 'consumer_timestamp' not in value:
                    value['consumer_timestamp'] = datetime.utcnow().isoformat()
                
                # Routing ke buffer yang sesuai
                if topic == 'news-api':
                    api_buffer.append(value)
                    all_api_messages.append(value)
                    logger.debug(f"API message buffered: {value.get('title', '')[:60]}")
                
                elif topic == 'news-rss':
                    rss_buffer.append(value)
                    all_rss_messages.append(value)
                    logger.debug(f"RSS message buffered: {value.get('title', '')[:60]}")
                
                # Flush ke HDFS setiap BUFFER_FLUSH_INTERVAL detik
                current_time = time.time()
                if current_time - last_flush_time >= BUFFER_FLUSH_INTERVAL:
                    
                    # Save API messages
                    if api_buffer:
                        logger.info(f"Flushing {len(api_buffer)} API messages to HDFS...")
                        save_to_local_and_hdfs(api_buffer, 'api')
                        api_buffer = []
                    
                    # Save RSS messages
                    if rss_buffer:
                        logger.info(f"Flushing {len(rss_buffer)} RSS messages to HDFS...")
                        save_to_local_and_hdfs(rss_buffer, 'rss')
                        rss_buffer = []
                    
                    # Update dashboard live data
                    save_latest_for_dashboard(all_api_messages, 'api')
                    save_latest_for_dashboard(all_rss_messages, 'rss')
                    
                    last_flush_time = current_time
                    logger.info("Buffer flushed successfully")
                
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding message: {e}")
                continue
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                continue
    
    except KeyboardInterrupt:
        logger.info("Consumer dihentikan oleh user")
        
        # Final flush
        if api_buffer:
            logger.info(f"Final flush {len(api_buffer)} API messages...")
            save_to_local_and_hdfs(api_buffer, 'api')
        
        if rss_buffer:
            logger.info(f"Final flush {len(rss_buffer)} RSS messages...")
            save_to_local_and_hdfs(rss_buffer, 'rss')
        
        save_latest_for_dashboard(all_api_messages, 'api')
        save_latest_for_dashboard(all_rss_messages, 'rss')
    
    finally:
        consumer.close()
        logger.info("Consumer ditutup")


def main():
    logger.info("=== Consumer to HDFS - NewsPulse ===")
    logger.info(f"Kafka Broker: {KAFKA_BROKER}")
    logger.info(f"Topics: {KAFKA_TOPICS}")
    logger.info(f"HDFS Namenode: {HDFS_NAMENODE}")
    logger.info(f"HDFS Base Path: {HDFS_BASE_PATH}")
    logger.info(f"Buffer Flush Interval: {BUFFER_FLUSH_INTERVAL} seconds")
    
    # Setup
    ensure_hdfs_paths()
    ensure_local_dirs()
    
    # Start consuming
    consume_and_save()


if __name__ == '__main__':
    main()
