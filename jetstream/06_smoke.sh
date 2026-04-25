#!/bin/bash
# Run on driver01.
# 90-second smoke test against the actual standalone cluster.

set -uo pipefail

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077
DURATION=90

cd ~/distributed-crypto-lob

# Wipe any prior topic state so per-run sequence numbers are clean
echo "==> resetting topic (delete + recreate with 8 partitions)"
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP:-10.4.36.193}:9092 \
    --delete --topic lob-events 2>/dev/null || true
sleep 2
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP:-10.4.36.193}:9092 \
    --create --if-not-exists --topic lob-events --partitions 8 --replication-factor 1 \
    2>&1 | tail -2

# Wipe prior parquet output too
sudo rm -rf /data/lob_snapshots/* 2>/dev/null || true

# Build a custom Spark image once, with requests + the app code baked in.
# Faster than pip-installing inside an ephemeral container on every run, and
# avoids the /home/spark permission problem.
if ! docker image inspect lob-spark:latest >/dev/null 2>&1; then
    echo "==> building lob-spark image"
    docker build -t lob-spark:latest -f Dockerfile.spark . 2>&1 | tail -5
else
    echo "==> lob-spark image already built"
fi

# kafka client install (host-side for producers)
pip3 uninstall --break-system-packages -y kafka-python kafka-python-ng >/dev/null 2>&1 || true
pip3 install --break-system-packages --quiet --force-reinstall \
    'kafka-python-ng==2.2.3' websocket-client requests
python3 -c "from kafka import KafkaProducer; print('host kafka client ok')"

export KAFKA_BOOTSTRAP=${KAFKA_IP}:9092
export KAFKA_TOPIC=lob-events
export EXCHANGES=coinbase,binance,kraken
export COINBASE_PRODUCTS=BTC-USD,ETH-USD,SOL-USD,LTC-USD
export BINANCE_PRODUCTS=BTC-USDT,ETH-USDT,SOL-USDT,LTC-USDT
export KRAKEN_PRODUCTS=BTC-USD,ETH-USD,SOL-USD,LTC-USD
export KAFKA_PARTITIONS=8
export STARTING_OFFSETS=earliest
export SNAPSHOT_SINK_DIR=/data/lob_snapshots
export ALL_BOOK_KEYS="coinbase:BTC-USD,coinbase:ETH-USD,coinbase:SOL-USD,coinbase:LTC-USD,binance:BTC-USDT,binance:ETH-USDT,binance:SOL-USDT,binance:LTC-USDT,kraken:BTC-USD,kraken:ETH-USD,kraken:SOL-USD,kraken:LTC-USD"

mkdir -p logs

echo "==> launching 3 producers"
nohup python3 -u producer_stream.py  > logs/producer_coinbase.log 2>&1 &
echo $! > logs/coinbase.pid
nohup python3 -u producer_binance.py > logs/producer_binance.log 2>&1 &
echo $! > logs/binance.pid
nohup python3 -u producer_kraken.py  > logs/producer_kraken.log  2>&1 &
echo $! > logs/kraken.pid

sleep 5
echo "==> producer health check"
for f in logs/producer_*.log; do
    echo "--- $f ---"
    head -3 "$f" 2>&1
done

echo "==> submitting spark job"
docker rm -f spark-app 2>/dev/null || true
docker run -d --name spark-app --network host \
    -v /data:/data \
    -e KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP} \
    -e KAFKA_TOPIC=${KAFKA_TOPIC} \
    -e STARTING_OFFSETS=${STARTING_OFFSETS} \
    -e SNAPSHOT_SINK_DIR=${SNAPSHOT_SINK_DIR} \
    --entrypoint /opt/spark/bin/spark-submit \
    lob-spark:latest \
        --master ${SPARK_MASTER} \
        --deploy-mode client \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.executor.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.driver.host=10.4.36.243 \
        /app/spark_order_book.py

echo "==> running for ${DURATION}s"
sleep ${DURATION}

echo "==> stopping producers"
for pidf in logs/coinbase.pid logs/binance.pid logs/kraken.pid; do
    pid=$(cat $pidf 2>/dev/null) || continue
    kill $pid 2>/dev/null || true
done
sleep 2
docker stop spark-app >/dev/null 2>&1 || true

echo
echo "==> producer summary"
for f in logs/producer_*.log; do
    echo "--- $f (last 5) ---"
    tail -5 "$f"
done

echo
echo "==> spark app: BATCH/metrics/errors lines"
docker logs spark-app 2>&1 | grep -E '===== BATCH|latency_p|total_records=|gap_count=|Loading snapshot|ERROR|Traceback' | tail -40

echo
echo "==> parquet output dirs (showing batch_id=*)"
ls /data/lob_snapshots/ 2>/dev/null | head -10
