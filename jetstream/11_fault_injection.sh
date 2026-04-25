#!/bin/bash
# fault recovery test. starts the full pipeline on all 8 workers, runs for a
# bit, then ssh's into one exec and kills its spark-worker container. waits,
# brings the worker back, watches the master + spark-app logs to time how
# long the proccess of recovery takes.
#
# this version assumes ssh from driver01 to exec01 works with the same key
# (run the run_fault_injection.py wrapper instead if you dont have that).

set -uo pipefail

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077
TOTAL_DURATION=180
KILL_AT=60
RESTORE_AT=120
TARGET_EXEC_IP=10.4.36.131  # exec01

cd ~/distributed-crypto-lob
mkdir -p logs/fault

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

echo "==> reset topic, clear sink"
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --delete --topic lob-events 2>/dev/null || true
sleep 2
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_BOOTSTRAP} \
    --create --topic lob-events --partitions 8 --replication-factor 1 \
    2>&1 | tail -1
sudo rm -rf /data/lob_snapshots/*

echo "==> launching producers"
nohup python3 -u producer_stream.py  > logs/fault/coinbase.log 2>&1 &
echo $! > logs/fault/coinbase.pid
nohup python3 -u producer_binance.py > logs/fault/binance.log  2>&1 &
echo $! > logs/fault/binance.pid
nohup python3 -u producer_kraken.py  > logs/fault/kraken.log   2>&1 &
echo $! > logs/fault/kraken.pid
sleep 15

echo "==> submitting Spark with all 8 workers"
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
        --conf spark.cores.max=16 \
        --conf spark.driver.host=10.4.36.243 \
        /app/spark_order_book.py

echo "==> waiting ${KILL_AT}s before kill"
sleep ${KILL_AT}

KILL_TS=$(date +%s)
echo
echo "######################################################"
echo "### KILL at t=${KILL_AT}s (epoch ${KILL_TS})"
echo "######################################################"
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
    exouser@${TARGET_EXEC_IP} 'docker rm -f spark-worker' 2>&1 | tail -2
echo "==> killed spark-worker on ${TARGET_EXEC_IP}"

echo "==> waiting for spark to reschedule onto the surviving 7 workers"
WAIT_UNTIL=$((RESTORE_AT - KILL_AT))
sleep ${WAIT_UNTIL}

RESTORE_TS=$(date +%s)
echo
echo "######################################################"
echo "### RESTORE at t=${RESTORE_AT}s (epoch ${RESTORE_TS})"
echo "######################################################"
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
    exouser@${TARGET_EXEC_IP} 'bash -s' << 'REMOTE'
docker run -d --name spark-worker --restart unless-stopped \
    --network host \
    -v /data:/data \
    -e SPARK_NO_DAEMONIZE=1 \
    apache/spark:3.5.3-python3 \
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --host $(hostname -I | awk '{print $1}') \
        --webui-port 8081 --cores 2 --memory 4G \
        spark://10.4.36.243:7077
REMOTE

REMAIN=$((TOTAL_DURATION - RESTORE_AT))
echo "==> sleeping ${REMAIN}s for re-register and resume"
sleep ${REMAIN}

echo "==> tearing down"
for pidf in logs/fault/*.pid; do
    pid=$(cat $pidf 2>/dev/null) || continue
    kill $pid 2>/dev/null || true
done
docker logs spark-app > logs/fault/spark-app.log 2>&1
docker stop spark-app >/dev/null 2>&1 || true

echo
echo "######################################################"
echo "### timing"
echo "######################################################"
echo "kill   epoch: ${KILL_TS}"
echo "restore epoch: ${RESTORE_TS}"
echo
echo "==> master worker events"
docker logs spark-master 2>&1 | grep -E "Removing worker|Registering worker" | tail -10
echo
echo "==> spark losses + reschedules"
grep -E "Lost task|Lost executor|removed|reschedul" logs/fault/spark-app.log | head -20
echo
echo "==> batches around the kill"
grep -E "===== BATCH|total_records=|gap_count=" logs/fault/spark-app.log | tail -30
