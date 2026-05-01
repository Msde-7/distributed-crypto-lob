#!/bin/bash
# synthetic-load smoke. spins up SYNTH_PROCS synth producer processes that
# together push SYNTH_RATE events/sec into kafka. spark consumes as usual.
# books are partitioned across processes round-robin so each owns its own
# subset and per-book sequence counters dont collide.
#
# usage:
#   SYNTH_RATE=50000 SYNTH_PROCS=4 DURATION=120 bash jetstream/13_synth_smoke.sh
#   REPLICATION_FACTOR=3 SYNTH_RATE=80000 bash jetstream/13_synth_smoke.sh
#
# run on driver01.

set -uo pipefail

KAFKA_IP="${KAFKA_IP:-10.4.36.193}"
SPARK_MASTER="${SPARK_MASTER:-spark://10.4.36.243:7077}"
DURATION="${DURATION:-120}"
SYNTH_RATE="${SYNTH_RATE:-30000}"
SYNTH_PROCS="${SYNTH_PROCS:-4}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
PER_PROC_RATE=$(( SYNTH_RATE / SYNTH_PROCS ))

cd ~/distributed-crypto-lob

# blow away the topic so synth seq counters start clean (otherwise spark
# would see old live-feed sequence numbers and fire false gaps)
echo "==> resetting topic (rf=${REPLICATION_FACTOR})"
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --delete --topic lob-events 2>/dev/null || true
sleep 2
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --create --if-not-exists --topic lob-events --partitions 8 \
    --replication-factor ${REPLICATION_FACTOR} 2>&1 | tail -2

sudo rm -rf /data/lob_snapshots/* 2>/dev/null || true

# build the spark image once (caches across runs)
if ! docker image inspect lob-spark:latest >/dev/null 2>&1; then
    echo "==> building lob-spark image"
    docker build -t lob-spark:latest -f Dockerfile.spark . 2>&1 | tail -5
else
    echo "==> lob-spark image already built"
fi

# kafka-python deps (idempotent, fast on second run)
pip3 uninstall --break-system-packages -y kafka-python kafka-python-ng >/dev/null 2>&1 || true
pip3 install --break-system-packages --quiet --force-reinstall \
    'kafka-python-ng==2.2.3' websocket-client requests
python3 -c "from kafka import KafkaProducer; print('host kafka client ok')"

export KAFKA_BOOTSTRAP=${KAFKA_IP}:9092
export KAFKA_TOPIC=lob-events
export STARTING_OFFSETS=earliest
export SNAPSHOT_SINK_DIR=/data/lob_snapshots
export KAFKA_PARTITIONS=8
# every producer process needs the same key set so partition_for is stable
export ALL_BOOK_KEYS="coinbase:BTC-USD,coinbase:ETH-USD,coinbase:SOL-USD,coinbase:LTC-USD,binance:BTC-USDT,binance:ETH-USDT,binance:SOL-USDT,binance:LTC-USDT,kraken:BTC-USD,kraken:ETH-USD,kraken:SOL-USD,kraken:LTC-USD"

mkdir -p logs
rm -f logs/synth_*.log logs/synth_*.pid

# round-robin the 12 books across SYNTH_PROCS processes (proc i owns books at
# indices i, i+SYNTH_PROCS, i+2*SYNTH_PROCS, ...). each book is owned by exactly
# one process so there is no contention on the per-book sequence counter.
ALL_BOOKS=(coinbase:BTC-USD coinbase:ETH-USD coinbase:SOL-USD coinbase:LTC-USD
           binance:BTC-USDT binance:ETH-USDT binance:SOL-USDT binance:LTC-USDT
           kraken:BTC-USD   kraken:ETH-USD   kraken:SOL-USD   kraken:LTC-USD)

echo "==> launching ${SYNTH_PROCS} synth procs at ${PER_PROC_RATE} ev/s each"
echo "    (target combined rate: ${SYNTH_RATE} ev/s)"
for i in $(seq 0 $((SYNTH_PROCS - 1))); do
    books=""
    for ((j=i; j<${#ALL_BOOKS[@]}; j+=SYNTH_PROCS)); do
        books+="${ALL_BOOKS[j]},"
    done
    books="${books%,}"

    SYNTH_RATE=${PER_PROC_RATE} \
    SYNTH_BOOKS="${books}" \
    SYNTH_DURATION=${DURATION} \
    SYNTH_SEED=$((42 + i)) \
    nohup python3 -u producer_synth.py > logs/synth_${i}.log 2>&1 &
    echo $! > logs/synth_${i}.pid
done

sleep 4
echo "==> synth health check"
for f in logs/synth_*.log; do
    echo "--- $f (head 3) ---"
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

echo "==> running for ${DURATION}s (synth procs will self-stop)"
sleep ${DURATION}

# synth procs should already be exiting on their own since we set SYNTH_DURATION,
# but kill any stragglers just in case
echo "==> stopping synth procs (if any still alive)"
for pidf in logs/synth_*.pid; do
    pid=$(cat $pidf 2>/dev/null) || continue
    kill $pid 2>/dev/null || true
done
sleep 2
docker stop spark-app >/dev/null 2>&1 || true

echo
echo "==> synth summary (last 2 lines per proc)"
for f in logs/synth_*.log; do
    echo "--- $f ---"
    tail -2 "$f"
done

echo
echo "==> spark app: BATCH/metrics/errors lines"
docker logs spark-app 2>&1 | grep -E '===== BATCH|latency_p|total_records=|gap_count=|Loading snapshot|ERROR|Traceback' | tail -50

echo
echo "==> parquet output dirs"
ls /data/lob_snapshots/ 2>/dev/null | head -10
echo
echo "==> total parquet batches written"
ls /data/lob_snapshots/ 2>/dev/null | wc -l
