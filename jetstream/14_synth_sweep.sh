#!/bin/bash
# 2D sweep: synthetic load rate vs worker count. for every (N workers, R rate)
# cell we run for DURATION seconds and capture spark logs to logs/synth_sweep/.
# all 8 workers stay registered with the master; we clamp spark.cores.max to
# N*2 per cell so spark only schedules onto N executors. matches the pattern
# in 10_scaling_sweep.sh for live data.
#
# usage on driver01:
#     bash jetstream/14_synth_sweep.sh
# overrides:
#     RATES="10000 30000 60000 100000"
#     WORKER_COUNTS="2 4 6 8"
#     DURATION=90
#     SYNTH_PROCS=4
#     REPLICATION_FACTOR=1   (bump to 3 if running with 3-broker kafka)
#
# expect ~3 min per cell (warmup + run + spark startup). 4x4 grid is ~50 min.

set -uo pipefail

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077
DURATION="${DURATION:-90}"
WARMUP="${WARMUP:-10}"
RATES="${RATES:-10000 30000 60000 100000}"
WORKER_COUNTS="${WORKER_COUNTS:-2 4 6 8}"
SYNTH_PROCS="${SYNTH_PROCS:-4}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"

cd ~/distributed-crypto-lob
mkdir -p logs/synth_sweep

export KAFKA_BOOTSTRAP=${KAFKA_IP}:9092
export KAFKA_TOPIC=lob-events
export STARTING_OFFSETS=earliest
export SNAPSHOT_SINK_DIR=/data/lob_snapshots
export KAFKA_PARTITIONS=8
export ALL_BOOK_KEYS="coinbase:BTC-USD,coinbase:ETH-USD,coinbase:SOL-USD,coinbase:LTC-USD,binance:BTC-USDT,binance:ETH-USDT,binance:SOL-USDT,binance:LTC-USDT,kraken:BTC-USD,kraken:ETH-USD,kraken:SOL-USD,kraken:LTC-USD"

ALL_BOOKS=(coinbase:BTC-USD coinbase:ETH-USD coinbase:SOL-USD coinbase:LTC-USD
           binance:BTC-USDT binance:ETH-USDT binance:SOL-USDT binance:LTC-USDT
           kraken:BTC-USD   kraken:ETH-USD   kraken:SOL-USD   kraken:LTC-USD)

pip3 install --break-system-packages --quiet 'kafka-python-ng==2.2.3' websocket-client requests >/dev/null

start_synth() {
    local target=$1
    local per_proc=$(( target / SYNTH_PROCS ))
    rm -f logs/synth_sweep/synth_*.pid logs/synth_sweep/synth_*.log
    for i in $(seq 0 $((SYNTH_PROCS - 1))); do
        local books=""
        for ((j=i; j<${#ALL_BOOKS[@]}; j+=SYNTH_PROCS)); do
            books+="${ALL_BOOKS[j]},"
        done
        books="${books%,}"
        SYNTH_RATE=${per_proc} \
        SYNTH_BOOKS="${books}" \
        SYNTH_DURATION=$((DURATION + WARMUP + 30)) \
        SYNTH_SEED=$((42 + i)) \
        nohup python3 -u producer_synth.py \
            > logs/synth_sweep/synth_${i}.log 2>&1 &
        echo $! > logs/synth_sweep/synth_${i}.pid
    done
}

stop_synth() {
    for pidf in logs/synth_sweep/synth_*.pid; do
        pid=$(cat $pidf 2>/dev/null) || continue
        kill $pid 2>/dev/null || true
    done
    rm -f logs/synth_sweep/synth_*.pid
}

reset_topic() {
    docker run --rm --network host apache/kafka:3.9.0 \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --delete --topic lob-events 2>/dev/null || true
    sleep 2
    docker run --rm --network host apache/kafka:3.9.0 \
        /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
        --create --if-not-exists --topic lob-events --partitions 8 \
        --replication-factor ${REPLICATION_FACTOR} 2>&1 | tail -1
    sudo rm -rf /data/lob_snapshots/* 2>/dev/null || true
}

run_cell() {
    local N=$1
    local R=$2
    local cores=$((N * 2))
    local outlog=logs/synth_sweep/N${N}_R${R}.log
    echo
    echo "######################################################"
    echo "### N=${N} workers (cores.max=${cores})  rate=${R}/s"
    echo "######################################################"

    stop_synth
    docker rm -f spark-app 2>/dev/null || true
    reset_topic

    echo "==> starting synth at ${R}/s (warmup ${WARMUP}s)"
    start_synth ${R}
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

    stop_synth
    sleep 2
    docker logs spark-app > ${outlog} 2>&1
    docker stop spark-app >/dev/null 2>&1 || true

    echo "==> N=${N} R=${R} summary"
    grep -E "total_records=|latency_p99_ms=|gap_count=|FileFormatWriter: Aborting" ${outlog} | tail -8
    echo "(full log at ${outlog})"
    sleep 4
}

# main grid
for N in ${WORKER_COUNTS}; do
    for R in ${RATES}; do
        run_cell $N $R
    done
done

echo
echo "######################################################"
echo "### sweep complete. aggregating with parse_sweep.py"
echo "######################################################"
python3 parse_sweep.py logs/synth_sweep
