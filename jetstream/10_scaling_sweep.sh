#!/bin/bash
# Run on driver01.
# Scaling sweep: same workload at N = 2, 4, 6, 8 Spark workers.
# Parallelism is constrained via --conf spark.cores.max so all 8 workers can
# stay registered, but Spark only schedules N*2 cores per run.

set -uo pipefail

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077
DURATION=120         # seconds of data collection per N
WARMUP=20            # seconds for producers to fill some Kafka backlog before Spark starts
WORKER_COUNTS="2 4 6 8"

cd ~/distributed-crypto-lob
mkdir -p logs/sweep

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

# kafka-python-ng must be installed (for producer scripts on the host)
pip3 install --break-system-packages --quiet 'kafka-python-ng==2.2.3' websocket-client requests >/dev/null

start_producers() {
    nohup python3 -u producer_stream.py  > logs/sweep/coinbase.log 2>&1 &
    echo $! > logs/sweep/coinbase.pid
    nohup python3 -u producer_binance.py > logs/sweep/binance.log  2>&1 &
    echo $! > logs/sweep/binance.pid
    nohup python3 -u producer_kraken.py  > logs/sweep/kraken.log   2>&1 &
    echo $! > logs/sweep/kraken.pid
}

stop_producers() {
    for pidf in logs/sweep/*.pid; do
        pid=$(cat $pidf 2>/dev/null) || continue
        kill $pid 2>/dev/null || true
    done
    rm -f logs/sweep/*.pid
}

reset_topic() {
    docker run --rm --network host apache/kafka:3.9.0 \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --delete --topic lob-events 2>/dev/null || true
    sleep 2
    docker run --rm --network host apache/kafka:3.9.0 \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --create --if-not-exists --topic lob-events --partitions 8 --replication-factor 1 \
        2>&1 | tail -1
    sudo rm -rf /data/lob_snapshots/* 2>/dev/null || true
}

run_one() {
    local N=$1
    local cores=$((N * 2))
    local outlog=logs/sweep/N${N}.log
    echo
    echo "######################################################"
    echo "### N=${N} workers (cores.max=${cores}), duration=${DURATION}s"
    echo "######################################################"

    stop_producers
    docker rm -f spark-app 2>/dev/null || true
    reset_topic

    echo "==> starting producers (warmup ${WARMUP}s)"
    start_producers
    sleep ${WARMUP}

    echo "==> submitting Spark with spark.cores.max=${cores}"
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
            --conf spark.cores.max=${cores} \
            --conf spark.driver.host=10.4.36.243 \
            /app/spark_order_book.py

    echo "==> running for ${DURATION}s"
    sleep ${DURATION}

    stop_producers
    sleep 1
    docker logs spark-app > ${outlog} 2>&1
    docker stop spark-app >/dev/null 2>&1 || true

    # quick on-the-fly summary
    echo "==> N=${N} summary"
    grep -E "total_records=|latency_p99_ms=|gap_count=|FileFormatWriter: Aborting" ${outlog} | tail -10
    echo "(full log at ${outlog})"
    sleep 5
}

for N in ${WORKER_COUNTS}; do
    run_one $N
done

echo
echo "######################################################"
echo "### sweep complete. parsing per-N results"
echo "######################################################"
for N in ${WORKER_COUNTS}; do
    echo
    echo "=== N=${N} ==="
    python3 parse_run.py logs/sweep/N${N}.log 2>&1 | head -30
done
