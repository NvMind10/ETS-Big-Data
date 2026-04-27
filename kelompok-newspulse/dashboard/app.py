#!/usr/bin/env python3
"""
Dashboard Flask - NewsPulse
Menampilkan hasil Spark analysis dan data live dari Kafka
"""

import json
import logging
from flask import Flask, render_template, jsonify
from datetime import datetime
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Konfigurasi
DATA_DIR = './dashboard/data'
SPARK_RESULTS_FILE = f'{DATA_DIR}/spark_results.json'
LIVE_API_FILE = f'{DATA_DIR}/live_api.json'
LIVE_RSS_FILE = f'{DATA_DIR}/live_rss.json'


def load_json_file(filepath):
    """Load JSON file, return empty dict jika tidak ada"""
    try:
        if not os.path.exists(filepath):
            logger.warning(f"File tidak ada: {filepath}")
            return {}
        
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading {filepath}: {e}")
        return {}


def get_spark_results():
    """Load hasil analisis Spark"""
    return load_json_file(SPARK_RESULTS_FILE)


def get_live_api_data():
    """Load data live API"""
    return load_json_file(LIVE_API_FILE)


def get_live_rss_data():
    """Load data live RSS"""
    return load_json_file(LIVE_RSS_FILE)


@app.route('/')
def index():
    """Halaman utama dashboard"""
    return render_template('index.html')


@app.route('/api/data')
def get_data():
    """API endpoint yang menggabungkan semua data"""
    try:
        spark_results = get_spark_results()
        live_api = get_live_api_data()
        live_rss = get_live_rss_data()
        
        # Prepare trending words
        top_words = []
        if 'analyses' in spark_results and 'top_words' in spark_results['analyses']:
            top_words = spark_results['analyses']['top_words'][:15]
        
        # Prepare source distribution
        source_dist = []
        if 'analyses' in spark_results and 'source_distribution' in spark_results['analyses']:
            source_dist = spark_results['analyses']['source_distribution']
        
        # Prepare recent news (gabung API dan RSS, ambil 20 terbaru)
        recent_news = []
        
        if isinstance(live_api, list):
            recent_news.extend(live_api)
        elif isinstance(live_api, dict) and 'articles' in live_api:
            recent_news.extend(live_api['articles'])
        
        if isinstance(live_rss, list):
            recent_news.extend(live_rss)
        elif isinstance(live_rss, dict) and 'articles' in live_rss:
            recent_news.extend(live_rss['articles'])
        
        # Sort by timestamp terbaru dan ambil 20
        recent_news = sorted(
            recent_news,
            key=lambda x: x.get('timestamp', ''),
            reverse=True
        )[:20]
        
        return jsonify({
            'success': True,
            'timestamp': datetime.utcnow().isoformat(),
            'trending_words': top_words,
            'source_distribution': source_dist,
            'recent_news': recent_news,
            'spark_analysis_time': spark_results.get('timestamp', 'N/A')
        })
    
    except Exception as e:
        logger.error(f"Error in /api/data: {e}")
        return jsonify({
            'success': False,
            'error': str(e),
            'trending_words': [],
            'source_distribution': [],
            'recent_news': []
        }), 500


@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'timestamp': datetime.utcnow().isoformat(),
        'spark_results_exists': os.path.exists(SPARK_RESULTS_FILE),
        'live_api_exists': os.path.exists(LIVE_API_FILE),
        'live_rss_exists': os.path.exists(LIVE_RSS_FILE)
    })


@app.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Not found'}), 404


@app.errorhandler(500)
def internal_error(error):
    logger.error(f"Internal error: {error}")
    return jsonify({'error': 'Internal server error'}), 500


if __name__ == '__main__':
    logger.info("Starting NewsPulse Dashboard")
    logger.info(f"Data directory: {DATA_DIR}")
    logger.info("Dashboard running on http://localhost:5000")
    
    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Run Flask app
    app.run(debug=True, host='0.0.0.0', port=5000)
