#!/usr/bin/env python3
"""
Spark Analysis - NewsPulse
3 Analisis Wajib:
1. Kata paling sering muncul di judul berita
2. Distribusi berita per sumber
3. Volume publikasi per jam
"""

import json
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, split, explode, lower, length, count, 
    hour, to_timestamp, desc, row_number, collect_list
)
from pyspark.sql.window import Window
import pandas as pd

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Konfigurasi
HDFS_NAMENODE = 'hdfs://namenode:8020'
HDFS_API_PATH = f'{HDFS_NAMENODE}/data/news/api'
HDFS_RSS_PATH = f'{HDFS_NAMENODE}/data/news/rss'
HDFS_OUTPUT_PATH = f'{HDFS_NAMENODE}/data/news/hasil'
LOCAL_OUTPUT_PATH = './dashboard/data/spark_results.json'

# Stopwords sederhana dalam bahasa Indonesia dan Inggris
STOPWORDS = {
    'dan', 'yang', 'di', 'ke', 'dari', 'untuk', 'dengan', 'dalam',
    'adalah', 'ini', 'itu', 'ada', 'tidak', 'akan', 'telah', 'dapat',
    'atau', 'juga', 'lebih', 'pada', 'saat', 'saat', 'hari', 'jam',
    'the', 'a', 'an', 'as', 'at', 'by', 'for', 'of', 'or', 'to',
    'in', 'on', 'it', 'be', 'is', 'are', 'was', 'were', 'been',
}


def init_spark():
    """Inisialisasi Spark Session"""
    try:
        spark = SparkSession.builder \\
            .appName("NewsPulse-Analysis") \\
            .config("spark.hadoop.fs.defaultFS", HDFS_NAMENODE) \\
            .config("spark.driver.memory", "2g") \\
            .config("spark.executor.memory", "2g") \\
            .getOrCreate()
        
        logger.info("Spark Session initialized successfully")
        return spark
    except Exception as e:
        logger.error(f"Error initializing Spark: {e}")
        return None


def read_news_data(spark):
    """
    Baca data berita dari HDFS (API + RSS)
    Return: DataFrame gabungan
    """
    try:
        # Baca dari API path
        api_df = spark.read.option("multiLine", True).json(HDFS_API_PATH)
        logger.info(f"Loaded {api_df.count()} records from API path")
        
        # Baca dari RSS path
        rss_df = spark.read.option("multiLine", True).json(HDFS_RSS_PATH)
        logger.info(f"Loaded {rss_df.count()} records from RSS path")
        
        # Gabung kedua dataframe
        combined_df = api_df.union(rss_df)
        logger.info(f"Combined dataframe has {combined_df.count()} records")
        
        return combined_df
    
    except Exception as e:
        logger.error(f"Error reading news data: {e}")
        return None


def analysis_1_top_words(spark, df):
    """
    Analisis 1: Kata paling sering muncul di judul berita
    """
    try:
        logger.info("=== Analisis 1: Top Words dalam Judul Berita ===")
        
        # Pastikan title ada dan tidak null
        df_with_title = df.filter(col('title').isNotNull())
        
        # Split judul menjadi words
        words_df = df_with_title.select(
            split(lower(col('title')), ' ').alias('words'),
            col('source')
        )
        
        # Explode words
        exploded_df = words_df.select(
            explode(col('words')).alias('word'),
            col('source')
        )
        
        # Clean words: remove special chars, filter stopwords, filter short words
        cleaned_df = exploded_df.filter(
            (length(col('word')) > 3) &
            (~col('word').isin(list(STOPWORDS)))
        ).filter(
            col('word').rlike('^[a-z0-9]+$')
        )
        
        # Count dan sort
        top_words = cleaned_df.groupBy('word').count() \\
            .orderBy(desc('count')) \\
            .limit(20)
        
        result_df = top_words.toPandas()
        
        logger.info("Top 20 Words:")
        for idx, row in result_df.iterrows():
            logger.info(f"  {idx+1}. '{row['word']}' : {row['count']} occurrences")
        
        return result_df.to_dict('records')
    
    except Exception as e:
        logger.error(f"Error in analysis 1: {e}")
        return []


def analysis_2_distribution_by_source(spark, df):
    """
    Analisis 2: Distribusi berita per sumber
    """
    try:
        logger.info("=== Analisis 2: Distribusi Berita Per Sumber ===")
        
        # Pastikan source ada dan tidak null
        df_with_source = df.filter(col('source').isNotNull())
        
        # Count per source
        source_dist = df_with_source.groupBy('source').count() \\
            .orderBy(desc('count')) \\
            .withColumnRenamed('count', 'jumlah_artikel')
        
        result_df = source_dist.toPandas()
        
        # Calculate percentage
        total = result_df['jumlah_artikel'].sum()
        result_df['persentase'] = (result_df['jumlah_artikel'] / total * 100).round(2)
        
        logger.info("Distribution by Source:")
        for idx, row in result_df.iterrows():
            logger.info(f"  {row['source']}: {row['jumlah_artikel']} articles ({row['persentase']}%)")
        
        return result_df.to_dict('records')
    
    except Exception as e:
        logger.error(f"Error in analysis 2: {e}")
        return []


def analysis_3_volume_by_hour(spark, df):
    """
    Analisis 3: Volume publikasi per jam
    """
    try:
        logger.info("=== Analisis 3: Volume Publikasi Per Jam ===")
        
        # Gunakan timestamp untuk extract jam
        df_with_time = df.filter(
            (col('timestamp').isNotNull()) | (col('published_at').isNotNull())
        )
        
        # Ambil timestamp sebagai timestamp column
        df_time_col = df_with_time.select(
            col('timestamp').alias('time_str'),
            col('title')
        ).filter(col('time_str').isNotNull())
        
        # Extract hour
        try:
            df_with_hour = df_time_col.select(
                hour(to_timestamp(col('time_str'))).alias('jam'),
                col('title')
            ).filter(col('jam').isNotNull())
            
            # Count per jam
            volume_by_hour = df_with_hour.groupBy('jam').count() \\
                .orderBy('jam') \\
                .withColumnRenamed('count', 'jumlah_artikel')
            
            result_df = volume_by_hour.toPandas()
            
            logger.info("Volume per Jam (0-23):")
            for idx, row in result_df.iterrows():
                jam = int(row['jam'])
                jumlah = row['jumlah_artikel']
                bar_length = int(jumlah / 2)  # Scale untuk display
                bar = '█' * bar_length
                logger.info(f"  {jam:02d}:00 - {jam+1:02d}:00 : {jumlah:3d} | {bar}")
            
            return result_df.to_dict('records')
        
        except Exception as e:
            logger.warning(f"Error extracting hour: {e}. Using fallback...")
            # Fallback: just count records
            count_df = df_with_time.select(count('*').alias('total')).toPandas()
            return [{'info': 'fallback', 'total_articles': int(count_df.iloc[0]['total'])}]
    
    except Exception as e:
        logger.error(f"Error in analysis 3: {e}")
        return []


def save_results(results):
    """
    Simpan hasil analisis ke JSON lokal dan HDFS
    """
    try:
        output = {
            'timestamp': datetime.utcnow().isoformat(),
            'topic': 'NewsPulse - Analisis Tren Berita Nasional',
            'analyses': {
                'top_words': results.get('top_words', []),
                'source_distribution': results.get('source_distribution', []),
                'volume_by_hour': results.get('volume_by_hour', [])
            }
        }
        
        # Simpan ke local dashboard
        with open(LOCAL_OUTPUT_PATH, 'w', encoding='utf-8') as f:
            json.dump(output, f, ensure_ascii=False, indent=2, default=str)
        
        logger.info(f"Results saved to: {LOCAL_OUTPUT_PATH}")
        
        # Optional: simpan ke HDFS juga
        try:
            import subprocess
            hdfs_file_path = f'{HDFS_OUTPUT_PATH}/analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
            subprocess.run(
                f'hdfs dfs -put {LOCAL_OUTPUT_PATH} {hdfs_file_path}',
                shell=True,
                timeout=10
            )
            logger.info(f"Results also saved to HDFS: {hdfs_file_path}")
        except Exception as e:
            logger.warning(f"Could not save to HDFS: {e}")
    
    except Exception as e:
        logger.error(f"Error saving results: {e}")


def main():
    logger.info("=== NewsPulse Spark Analysis ===")
    
    # Initialize Spark
    spark = init_spark()
    if spark is None:
        logger.error("Failed to initialize Spark")
        return
    
    try:
        # Read data
        logger.info("Reading news data from HDFS...")
        df = read_news_data(spark)
        
        if df is None or df.count() == 0:
            logger.warning("No data found in HDFS paths")
            logger.info("Make sure producer dan consumer sudah menjalankan dan data sudah tersimpan di HDFS")
            return
        
        logger.info(f"Total records to analyze: {df.count()}")
        df.show()
        
        # Run analyses
        results = {}
        
        results['top_words'] = analysis_1_top_words(spark, df)
        results['source_distribution'] = analysis_2_distribution_by_source(spark, df)
        results['volume_by_hour'] = analysis_3_volume_by_hour(spark, df)
        
        # Save results
        save_results(results)
        
        logger.info("\\n=== Analisis Selesai ===")
        logger.info(f"Hasil tersimpan di: {LOCAL_OUTPUT_PATH}")
    
    except Exception as e:
        logger.error(f"Error in main: {e}")
    
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == '__main__':
    main()
