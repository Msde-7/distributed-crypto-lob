"""fault injection from the local laptop. spawns the experiment on driver01,
sleeps, kills a worker via the ssh harness, sleeps, restores it, then pulls
the logs back. easier than putting another keypair on driver01 just so it
can ssh to exec01.

what we want out of this:
  - time from kill to next batch (recovery)
  - time from restore to re-registration (rejoin)
  - whether any data was lost (gap_count + total_records before/after)
"""
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, os.path.dirname(__file__))
from ssh_run import run_script, run_cmd  # noqa


KILL_AT = 60
RESTORE_AT = 120
TOTAL = 180
KILL_TARGET = "exec01"
WORKER_INTERNAL_IP = "10.4.36.131"


SETUP_SCRIPT = """
set -uo pipefail
cd ~/distributed-crypto-lob
mkdir -p logs/fault

# wipe topic + sink so we dont mix in events recieved from earlier runs
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.4.36.193:9092 \
    --delete --topic lob-events 2>/dev/null || true
sleep 2
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server 10.4.36.193:9092 \
    --create --topic lob-events --partitions 8 --replication-factor 1 2>&1 | tail -1
sudo rm -rf /data/lob_snapshots/*

export KAFKA_BOOTSTRAP=10.4.36.193:9092
export KAFKA_TOPIC=lob-events
export STARTING_OFFSETS=earliest
export SNAPSHOT_SINK_DIR=/data/lob_snapshots
export COINBASE_PRODUCTS=BTC-USD,ETH-USD,SOL-USD,LTC-USD
export BINANCE_PRODUCTS=BTC-USDT,ETH-USDT,SOL-USDT,LTC-USDT
export KRAKEN_PRODUCTS=BTC-USD,ETH-USD,SOL-USD,LTC-USD
export KAFKA_PARTITIONS=8
export ALL_BOOK_KEYS="coinbase:BTC-USD,coinbase:ETH-USD,coinbase:SOL-USD,coinbase:LTC-USD,binance:BTC-USDT,binance:ETH-USDT,binance:SOL-USDT,binance:LTC-USDT,kraken:BTC-USD,kraken:ETH-USD,kraken:SOL-USD,kraken:LTC-USD"

nohup python3 -u producer_stream.py  > logs/fault/coinbase.log 2>&1 &
echo $! > logs/fault/coinbase.pid
nohup python3 -u producer_binance.py > logs/fault/binance.log  2>&1 &
echo $! > logs/fault/binance.pid
nohup python3 -u producer_kraken.py  > logs/fault/kraken.log   2>&1 &
echo $! > logs/fault/kraken.pid
sleep 15

docker rm -f spark-app 2>/dev/null || true
docker run -d --name spark-app --network host \
    -v /data:/data \
    -e KAFKA_BOOTSTRAP=$KAFKA_BOOTSTRAP \
    -e KAFKA_TOPIC=$KAFKA_TOPIC \
    -e STARTING_OFFSETS=$STARTING_OFFSETS \
    -e SNAPSHOT_SINK_DIR=$SNAPSHOT_SINK_DIR \
    --entrypoint /opt/spark/bin/spark-submit \
    lob-spark:latest \
        --master spark://10.4.36.243:7077 \
        --deploy-mode client \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --conf spark.executor.memory=2g \
        --conf spark.executor.cores=2 \
        --conf spark.cores.max=16 \
        --conf spark.driver.host=10.4.36.243 \
        /app/spark_order_book.py
echo "spark-app started"
"""

KILL_SCRIPT = """
docker rm -f spark-worker
echo "killed at $(date +%s)"
"""

RESTORE_SCRIPT = f"""
INTERNAL_IP=$(hostname -I | awk '{{print $1}}')
docker run -d --name spark-worker --restart unless-stopped \\
    --network host \\
    -v /data:/data \\
    -e SPARK_NO_DAEMONIZE=1 \\
    apache/spark:3.5.3-python3 \\
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \\
        --host $INTERNAL_IP \\
        --webui-port 8081 --cores 2 --memory 4G \\
        spark://10.4.36.243:7077
echo "restored at $(date +%s)"
"""

TEARDOWN_SCRIPT = """
cd ~/distributed-crypto-lob
for pidf in logs/fault/*.pid; do
    pid=$(cat $pidf 2>/dev/null) || continue
    kill $pid 2>/dev/null || true
done
sleep 1
docker logs spark-app > logs/fault/spark-app.log 2>&1
docker stop spark-app >/dev/null 2>&1 || true
echo "torn down at $(date +%s)"
"""

REPORT_SCRIPT = """
cd ~/distributed-crypto-lob
echo "=== master worker events ==="
docker logs spark-master 2>&1 | grep -E "Removing worker|Registering worker" | tail -10
echo
echo "=== spark losses ==="
grep -E "Lost task|Lost executor|removed|reschedul|FetchFailed" logs/fault/spark-app.log | head -15
echo
echo "=== batches around the kill ==="
grep -E "===== BATCH|total_records=|gap_count=|Loading snapshot" logs/fault/spark-app.log | tail -40
"""


def main():
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    print("==> setting up + launching the experiment on driver01")
    run_script("driver01", SETUP_SCRIPT, label="driver01:setup")
    print(f"\n==> waiting {KILL_AT}s before injecting the fault")
    time.sleep(KILL_AT)

    t_kill = time.time()
    print(f"\n==> KILLING spark-worker on {KILL_TARGET} (t={int(t_kill)})")
    run_script(KILL_TARGET, KILL_SCRIPT, label=f"{KILL_TARGET}:kill")

    wait_until_restore = RESTORE_AT - KILL_AT
    print(f"\n==> sleeping {wait_until_restore}s while spark recovers")
    time.sleep(wait_until_restore)

    t_restore = time.time()
    print(f"\n==> RESTORING spark-worker on {KILL_TARGET} (t={int(t_restore)})")
    run_script(KILL_TARGET, RESTORE_SCRIPT, label=f"{KILL_TARGET}:restore")

    remain = TOTAL - RESTORE_AT
    print(f"\n==> sleeping {remain}s for the new worker to re-register")
    time.sleep(remain)

    print("\n==> teardown")
    run_script("driver01", TEARDOWN_SCRIPT, label="driver01:teardown")

    print("\n==> analysis")
    run_script("driver01", REPORT_SCRIPT, label="driver01:report")

    print(f"\n==> kill epoch:    {int(t_kill)}")
    print(f"==> restore epoch: {int(t_restore)}")
    print("==> diff against the master log epochs to get recovery + rejoin times")


if __name__ == "__main__":
    main()
