#!/bin/bash
# orchestrate a synth run with producers off-host on synthdata.
# spark + kafka stay on their usual nodes; only the synth procs move.
# this isolates whether driver01 cpu was the bottleneck (if yes,
# wall-clock rate should be much higher than the ~30k/s ceiling we hit
# with synth co-located on driver01).
#
# run from local laptop:
#     bash jetstream/15_synth_offhost.sh
# overrides:
#     SYNTH_RATE_PER_PROC=8000  per-proc rate target
#     SYNTH_PROCS=16            procs running on synthdata
#     DURATION=120              seconds spark runs
#     CORES_MAX=16              spark.cores.max
#     LOG_TAG=synth_offhost     subdir under logs/

set -uo pipefail

SYNTH_RATE_PER_PROC="${SYNTH_RATE_PER_PROC:-8000}"
SYNTH_PROCS="${SYNTH_PROCS:-16}"
DURATION="${DURATION:-120}"
WARMUP="${WARMUP:-10}"
CORES_MAX="${CORES_MAX:-16}"
REPLICATION_FACTOR="${REPLICATION_FACTOR:-1}"
LOG_TAG="${LOG_TAG:-synth_offhost}"

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077

ALL_BOOKS=(coinbase:BTC-USD coinbase:ETH-USD coinbase:SOL-USD coinbase:LTC-USD
           binance:BTC-USDT binance:ETH-USDT binance:SOL-USDT binance:LTC-USDT
           kraken:BTC-USD   kraken:ETH-USD   kraken:SOL-USD   kraken:LTC-USD)
ALL_KEYS="coinbase:BTC-USD,coinbase:ETH-USD,coinbase:SOL-USD,coinbase:LTC-USD,binance:BTC-USDT,binance:ETH-USDT,binance:SOL-USDT,binance:LTC-USDT,kraken:BTC-USD,kraken:ETH-USD,kraken:SOL-USD,kraken:LTC-USD"

mkdir -p "logs/${LOG_TAG}"

echo "==> RESET topic on kafka01 (rf=${REPLICATION_FACTOR})"
python jetstream/ssh_run.py driver01 --cmd "
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --delete --topic lob-events 2>/dev/null || true
sleep 2
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --create --if-not-exists --topic lob-events --partitions 8 \
    --replication-factor ${REPLICATION_FACTOR} 2>&1 | tail -1
sudo rm -rf /data/lob_snapshots/* 2>/dev/null || true
" 2>&1 | tail -5

echo
echo "==> LAUNCH ${SYNTH_PROCS} synth procs on synthdata at ${SYNTH_RATE_PER_PROC} ev/s each"
# round-robin books across procs
SYNTH_LAUNCH=""
for ((i=0; i<SYNTH_PROCS; i++)); do
    books=""
    for ((j=i; j<${#ALL_BOOKS[@]}; j+=SYNTH_PROCS)); do
        books+="${ALL_BOOKS[j]},"
    done
    books="${books%,}"
    SYNTH_LAUNCH+="
KAFKA_BOOTSTRAP=${KAFKA_IP}:9092 KAFKA_TOPIC=lob-events \
ALL_BOOK_KEYS='${ALL_KEYS}' KAFKA_PARTITIONS=8 \
SYNTH_RATE=${SYNTH_RATE_PER_PROC} SYNTH_BOOKS='${books}' \
SYNTH_DURATION=$((DURATION + WARMUP + 30)) SYNTH_SEED=$((42 + i)) \
nohup python3 -u producer_synth.py > /tmp/synth_${i}.log 2>&1 & echo \$! >> /tmp/synth_pids
"
done
python jetstream/ssh_run.py synthdata --cmd "
cd ~/distributed-crypto-lob
rm -f /tmp/synth_*.log /tmp/synth_pids
${SYNTH_LAUNCH}
sleep 1
echo \"started \$(wc -l < /tmp/synth_pids) procs\"
" 2>&1 | tail -3

echo
echo "==> waiting ${WARMUP}s warmup then submitting Spark"
sleep ${WARMUP}

python jetstream/ssh_run.py driver01 --cmd "
cd ~/distributed-crypto-lob
docker rm -f spark-app 2>/dev/null || true
docker run -d --name spark-app --network host \
    -v /data:/data \
    -e KAFKA_BOOTSTRAP=${KAFKA_IP}:9092 \
    -e KAFKA_TOPIC=lob-events \
    -e STARTING_OFFSETS=earliest \
    -e SNAPSHOT_SINK_DIR=/data/lob_snapshots \
    --entrypoint /opt/spark/bin/spark-submit lob-spark:latest \
        --master ${SPARK_MASTER} --deploy-mode client \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.executor.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.cores.max=${CORES_MAX} \
        --conf spark.driver.host=10.4.36.243 \
        /app/spark_order_book.py
echo spark-app started
" 2>&1 | tail -3

echo
echo "==> running for ${DURATION}s"
sleep ${DURATION}

echo
echo "==> capturing spark log + stopping everything"
python jetstream/ssh_run.py driver01 --cmd "
docker logs spark-app > /tmp/spark.log 2>&1
docker stop spark-app >/dev/null 2>&1 || true
" 2>&1 | tail -2

python jetstream/ssh_run.py synthdata --cmd "
for pid in \$(cat /tmp/synth_pids 2>/dev/null); do kill \$pid 2>/dev/null || true; done
sleep 1
echo === synth summary ===
grep DONE /tmp/synth_*.log | head -8
echo
echo === peak driver01 cpu? ===
echo synthdata cpu now:
top -bn1 | head -5
" 2>&1 | tail -20

# pull spark log down to local
python jetstream/ssh_run.py driver01 --cmd "cat /tmp/spark.log" 2>&1 | \
    grep -E '===== BATCH|latency_p|total_records=|gap_count=|ERROR Aborting' | \
    tail -30 > "logs/${LOG_TAG}/spark_extract.log"

# headline numbers
python jetstream/ssh_run.py driver01 --cmd "
grep -oE 'total_records=[0-9]+' /tmp/spark.log | tail -1
echo ---
grep -oE 'latency_p99_ms=[0-9.]+' /tmp/spark.log | tail -10
echo ---
grep -c FileFormatWriter /tmp/spark.log || echo 0
" 2>&1 | tail -20 > "logs/${LOG_TAG}/headline.log"

echo
echo "==> RESULTS"
cat "logs/${LOG_TAG}/headline.log"
echo
echo "wall-clock rate = total_records / DURATION (${DURATION}s) = roughly above"
