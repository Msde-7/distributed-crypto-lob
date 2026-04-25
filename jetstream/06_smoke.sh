#!/bin/bash
# Run on driver01.
# 90-second smoke test against the actual standalone cluster.
# - launches 3 producers (Coinbase, Binance, Kraken) on driver01 in background
# - submits the Spark app to spark://10.4.36.243:7077 (uses both workers)
# - waits, then stops everything and dumps the metrics

set -uo pipefail

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077
DURATION=90

cd ~/distributed-crypto-lob

export KAFKA_BOOTSTRAP=${KAFKA_IP}:9092
export KAFKA_TOPIC=lob-events
export EXCHANGES=coinbase,binance,kraken
export COINBASE_PRODUCTS=BTC-USD,ETH-USD,SOL-USD,LTC-USD
export BINANCE_PRODUCTS=BTC-USDT,ETH-USDT,SOL-USDT,LTC-USDT
export KRAKEN_PRODUCTS=BTC-USD,ETH-USD,SOL-USD,LTC-USD
export KAFKA_PARTITIONS=8
export STARTING_OFFSETS=earliest
export SNAPSHOT_SINK_DIR=/data/lob_snapshots
# Build ALL_BOOK_KEYS so the partitioner is consistent across producers
export ALL_BOOK_KEYS="coinbase:BTC-USD,coinbase:ETH-USD,coinbase:SOL-USD,coinbase:LTC-USD,binance:BTC-USDT,binance:ETH-USDT,binance:SOL-USDT,binance:LTC-USDT,kraken:BTC-USD,kraken:ETH-USD,kraken:SOL-USD,kraken:LTC-USD"

mkdir -p logs

echo "==> launching 3 producers in background"
nohup python3 -u producer_stream.py  > logs/producer_coinbase.log 2>&1 &
echo $! > logs/coinbase.pid
nohup python3 -u producer_binance.py > logs/producer_binance.log 2>&1 &
echo $! > logs/binance.pid
nohup python3 -u producer_kraken.py  > logs/producer_kraken.log  2>&1 &
echo $! > logs/kraken.pid

sleep 3
echo "==> producer pids: coinbase=$(cat logs/coinbase.pid) binance=$(cat logs/binance.pid) kraken=$(cat logs/kraken.pid)"

echo "==> submitting spark job to ${SPARK_MASTER}"
docker rm -f spark-app 2>/dev/null || true
docker run -d --name spark-app --network host \
    -v ~/distributed-crypto-lob:/app \
    -v /data:/data \
    -e KAFKA_BOOTSTRAP=${KAFKA_BOOTSTRAP} \
    -e KAFKA_TOPIC=${KAFKA_TOPIC} \
    -e STARTING_OFFSETS=${STARTING_OFFSETS} \
    -e SNAPSHOT_SINK_DIR=${SNAPSHOT_SINK_DIR} \
    apache/spark:3.5.3-python3 \
    /opt/spark/bin/spark-submit \
        --master ${SPARK_MASTER} \
        --deploy-mode client \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
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

echo "==> stopping spark app"
docker stop spark-app >/dev/null 2>&1 || true

echo "==> producer summary"
for f in logs/producer_*.log; do
    echo "--- $f ---"
    tail -5 "$f"
done

echo "==> spark app summary (last 40 lines)"
docker logs --tail 40 spark-app 2>&1 | head -40

echo "==> parquet output"
ls -la /data/lob_snapshots/ 2>/dev/null | head -10
