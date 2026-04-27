#!/usr/bin/env python3
"""
Monitoring & Health Check Script
Untuk memonitor status semua komponen sistem
"""

import subprocess
import json
import os
import sys
from datetime import datetime

def run_command(cmd, timeout=10):
    """Run shell command dan return output"""
    try:
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result.returncode == 0, result.stdout.strip()
    except subprocess.TimeoutExpired:
        return False, "Timeout"
    except Exception as e:
        return False, str(e)

def check_docker_containers():
    """Check Docker containers status"""
    print("\n📦 DOCKER CONTAINERS")
    print("-" * 50)
    
    containers = {
        'kafka-broker': 'Kafka Broker',
        'zookeeper': 'Zookeeper',
        'namenode': 'Hadoop Namenode',
        'datanode1': 'Hadoop Datanode 1',
        'datanode2': 'Hadoop Datanode 2'
    }
    
    for container, name in containers.items():
        success, output = run_command(f"docker ps | grep {container}")
        if success:
            print(f"✓ {name:25} - Running")
        else:
            print(f"✗ {name:25} - Not running")

def check_kafka():
    """Check Kafka topics and messages"""
    print("\n📨 KAFKA STATUS")
    print("-" * 50)
    
    # Check topics
    success, output = run_command(
        "docker exec kafka-broker kafka-topics.sh --list --bootstrap-server localhost:9092"
    )
    
    if success:
        topics = output.split('\n')
        print(f"✓ Topics found: {len(topics)}")
        for topic in topics:
            if topic.strip():
                print(f"  - {topic}")
    else:
        print("✗ Could not list topics")
    
    # Check consumer group
    success, output = run_command(
        "docker exec kafka-broker kafka-consumer-groups.sh "
        "--bootstrap-server localhost:9092 --list"
    )
    
    if success:
        print(f"✓ Consumer groups: {output.split()[0] if output else 'None'}")
    else:
        print("✗ Could not list consumer groups")

def check_hdfs():
    """Check HDFS status"""
    print("\n🗄️  HDFS STATUS")
    print("-" * 50)
    
    # Check namenode status
    success, output = run_command(
        "docker exec namenode hdfs dfsadmin -report | head -5"
    )
    
    if success:
        print("✓ Namenode responsive")
    else:
        print("✗ Namenode not responding")
    
    # Check directories
    paths = ['/data/news/api', '/data/news/rss', '/data/news/hasil']
    for path in paths:
        success, output = run_command(
            f"docker exec namenode hdfs dfs -ls {path} 2>/dev/null | wc -l"
        )
        if success and output:
            count = int(output.strip())
            print(f"✓ {path:25} - {count} files")
        else:
            print(f"✗ {path:25} - Not accessible")

def check_dashboard():
    """Check Dashboard data files"""
    print("\n📊 DASHBOARD DATA")
    print("-" * 50)
    
    files = {
        'dashboard/data/spark_results.json': 'Spark Results',
        'dashboard/data/live_api.json': 'Live API Data',
        'dashboard/data/live_rss.json': 'Live RSS Data'
    }
    
    for filepath, name in files.items():
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            print(f"✓ {name:25} - {size:,} bytes")
        else:
            print(f"✗ {name:25} - File not found")

def check_services_urls():
    """Check if services are responding"""
    print("\n🔗 SERVICE ENDPOINTS")
    print("-" * 50)
    
    services = {
        'http://localhost:5000': 'Dashboard',
        'http://localhost:9870': 'HDFS Web UI',
        'http://localhost:9092': 'Kafka Broker',
    }
    
    for url, name in services.items():
        success, _ = run_command(f"curl -s {url} >/dev/null 2>&1", timeout=5)
        if success:
            print(f"✓ {name:25} - {url}")
        else:
            print(f"✗ {name:25} - {url} (Not responding)")

def main():
    print("\n" + "="*50)
    print("🔍 NewsPulse System Health Check")
    print(f"   Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*50)
    
    check_docker_containers()
    check_kafka()
    check_hdfs()
    check_dashboard()
    check_services_urls()
    
    print("\n" + "="*50)
    print("✓ Health check completed")
    print("="*50 + "\n")

if __name__ == '__main__':
    main()
