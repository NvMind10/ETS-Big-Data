#!/bin/bash
# startup.sh - Quick startup script untuk development

set -e

echo "🚀 NewsPulse Big Data Pipeline - Startup Script"
echo "=================================================="

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check Docker
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker found${NC}"

# Check Docker Compose
if ! command -v docker-compose &> /dev/null; then
    echo -e "${RED}✗ Docker Compose not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker Compose found${NC}"

# Start Kafka
echo ""
echo -e "${YELLOW}Starting Kafka...${NC}"
docker-compose -f docker-compose-kafka.yml up -d
sleep 5

# Create topics
echo -e "${YELLOW}Creating Kafka topics...${NC}"
docker exec -it kafka-broker kafka-topics.sh \
  --create --topic news-api \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic news-api already exists"

docker exec -it kafka-broker kafka-topics.sh \
  --create --topic news-rss \
  --bootstrap-server localhost:9092 \
  --partitions 3 --replication-factor 1 2>/dev/null || echo "Topic news-rss already exists"

echo -e "${GREEN}✓ Kafka started successfully${NC}"

# Start Hadoop
echo ""
echo -e "${YELLOW}Starting Hadoop HDFS...${NC}"
docker-compose -f docker-compose-hadoop.yml up -d
sleep 15  # Wait for HDFS to initialize

# Create HDFS directories
echo -e "${YELLOW}Creating HDFS directories...${NC}"
docker exec -it namenode hdfs dfs -mkdir -p /data/news/api 2>/dev/null || true
docker exec -it namenode hdfs dfs -mkdir -p /data/news/rss 2>/dev/null || true
docker exec -it namenode hdfs dfs -mkdir -p /data/news/hasil 2>/dev/null || true

echo -e "${GREEN}✓ Hadoop HDFS started successfully${NC}"

# Summary
echo ""
echo -e "${GREEN}✓ All services started!${NC}"
echo ""
echo "📋 Service URLs:"
echo "  - Kafka Broker: localhost:9092"
echo "  - HDFS Namenode Web UI: http://localhost:9870"
echo ""
echo "📝 Next steps:"
echo "  1. Set API keys in kafka/producer_api.py"
echo "  2. Terminal 1: cd kafka && python producer_api.py"
echo "  3. Terminal 2: cd kafka && python producer_rss.py"
echo "  4. Terminal 3: cd kafka && python consumer_to_hdfs.py"
echo "  5. Wait 5-10 minutes for data to accumulate"
echo "  6. Terminal 4: cd spark && python analysis.py"
echo "  7. Terminal 5: cd dashboard && python app.py"
echo "  8. Browser: http://localhost:5000"
echo ""
