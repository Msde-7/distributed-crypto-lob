"""
Run the full 5-minute experiment unattended:
  1. Ensure Kafka is up (start if not).
  2. Launch producer_stream.py and spark_order_book.py.
  3. Wait DURATION seconds.
  4. Kill both job trees cleanly.
  5. Parse spark_run.log and print the numbers.

Usage:
    python run_experiment.py              # default 300s (5 min)
    python run_experiment.py 60           # 60s run
"""
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).parent.resolve()
SPARK_LOG = ROOT / "spark_run.log"
KAFKA_LOG = ROOT / "kafka.log"

# Which producers to launch. Comma-separated, any subset of:
#   coinbase, binance, kraken
# Each has its own producer script and its own log file.
EXCHANGES = [
    x.strip().lower()
    for x in os.environ.get("EXCHANGES", "coinbase").split(",")
    if x.strip()
]
PRODUCER_SCRIPTS = {
    "coinbase": "producer_stream.py",
    "binance": "producer_binance.py",
    "kraken": "producer_kraken.py",
}

JAVA_HOME = r"C:\Program Files\Microsoft\jdk-17.0.18.8-hotspot"
KAFKA_HOME = r"C:\tools\kafka_2.13-3.9.0"
SPARK_HOME = r"C:\tools\spark-3.5.8-bin-hadoop3"
HADOOP_HOME = r"C:\tools\hadoop"

DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 300


def port_open(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def kill_tree(pid):
    subprocess.run(
        ["taskkill", "/F", "/T", "/PID", str(pid)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def wait_for_port(host, port, label, max_wait=90):
    start = time.time()
    while time.time() - start < max_wait:
        if port_open(host, port):
            return True
        time.sleep(2)
    print(f"[FATAL] {label} did not open port {port} within {max_wait}s")
    return False


def start_kafka():
    if port_open("localhost", 9092):
        print("[kafka] already running on :9092")
        return None
    print("[kafka] starting...")
    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    env["PATH"] = rf"{JAVA_HOME}\bin;" + env.get("PATH", "")
    bat = rf"{KAFKA_HOME}\bin\windows\kafka-server-start.bat"
    cfg = rf"{KAFKA_HOME}\config\kraft\server.properties"
    log = open(KAFKA_LOG, "w", encoding="utf-8", errors="replace")
    proc = subprocess.Popen(
        [bat, cfg],
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )
    if not wait_for_port("localhost", 9092, "kafka"):
        kill_tree(proc.pid)
        sys.exit(1)
    print("[kafka] ready")
    return proc


def start_producer(exchange):
    script = PRODUCER_SCRIPTS.get(exchange)
    if script is None:
        print(f"[FATAL] unknown exchange '{exchange}' (known: {list(PRODUCER_SCRIPTS)})")
        sys.exit(1)
    log_path = ROOT / f"producer_{exchange}.log"
    print(f"[producer:{exchange}] starting {script} -> {log_path.name}")
    log = open(log_path, "w", encoding="utf-8", errors="replace")
    proc = subprocess.Popen(
        [sys.executable, script],
        cwd=ROOT,
        stdout=log,
        stderr=subprocess.STDOUT,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )
    return proc


def start_producers():
    return [(ex, start_producer(ex)) for ex in EXCHANGES]


def start_spark():
    print("[spark] starting (first run downloads Kafka connector ~57MB)...")
    env = os.environ.copy()
    env["JAVA_HOME"] = JAVA_HOME
    env["SPARK_HOME"] = SPARK_HOME
    env["HADOOP_HOME"] = HADOOP_HOME
    env["PATH"] = rf"{JAVA_HOME}\bin;{SPARK_HOME}\bin;{HADOOP_HOME}\bin;" + env.get("PATH", "")
    log = open(SPARK_LOG, "w", encoding="utf-8", errors="replace")
    cmd = (
        rf'"{SPARK_HOME}\bin\spark-submit.cmd" '
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 "
        "spark_order_book.py"
    )
    proc = subprocess.Popen(
        cmd,
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        shell=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
    )
    return proc


def wait_for_first_batch(timeout=180):
    start = time.time()
    while time.time() - start < timeout:
        if SPARK_LOG.exists():
            text = SPARK_LOG.read_text(encoding="utf-8", errors="replace")
            if "BATCH 0" in text or "===== BATCH" in text:
                return True
        time.sleep(3)
    return False


def parse_log():
    if not SPARK_LOG.exists():
        print("[parse] no spark log")
        return
    text = SPARK_LOG.read_text(encoding="utf-8", errors="replace")

    rps = [float(x) for x in re.findall(r"batch_records_per_sec=([\d.]+)", text)]
    secs = [float(x) for x in re.findall(r"batch_time_sec=([\d.]+)", text)]
    recs = [int(x) for x in re.findall(r"batch_records=(\d+)", text)]
    tb = re.findall(r"total_batches=(\d+)", text)
    tr = re.findall(r"total_records=(\d+)", text)
    trs = re.findall(r"total_resyncs=(\d+)", text)

    last_quotes = {}
    for key, bid, ask, spread in re.findall(
        r"\[([a-z]+:[A-Z0-9\-]+)\] best_bid=([\w.\-]+) best_ask=([\w.\-]+) spread=([\w.\-]+)",
        text,
    ):
        last_quotes[key] = (bid, ask, spread)

    def mean(xs):
        return sum(xs) / len(xs) if xs else 0.0

    mean_rps = mean([r for r in rps if r > 0])
    mean_ms = mean(secs) * 1000
    peak_rps = max(rps) if rps else 0.0
    peak_batch = max(recs) if recs else 0

    print()
    print("=" * 60)
    print(f"log file:              {SPARK_LOG.name}")
    print(f"batches parsed:        {len(secs)}")
    print(f"total_batches (last):  {tb[-1] if tb else 'n/a'}")
    print(f"total_records (last):  {tr[-1] if tr else 'n/a'}")
    print(f"total_resyncs (last):  {trs[-1] if trs else 'n/a'}")
    print("-" * 60)
    print(f"mean events/sec:       {mean_rps:.2f}")
    print(f"mean ms/batch:         {mean_ms:.2f}")
    print(f"peak events/sec:       {peak_rps:.2f}")
    print(f"peak batch size:       {peak_batch}")
    print("-" * 60)
    print("last quote per book (compare one to a REST call):")
    for key, (bid, ask, spread) in last_quotes.items():
        print(f"  {key}: best_bid={bid}  best_ask={ask}  spread={spread}")
    print("=" * 60)
    print("\nPaste-ready sentence:")
    print(
        f"Over {tb[-1] if tb else '?'} batches we processed "
        f"{tr[-1] if tr else '?'} events at a mean of "
        f"{mean_rps:.0f} events/sec and {mean_ms:.0f} ms/batch, with "
        f"{trs[-1] if trs else '?'} resyncs."
    )


def main():
    print(f"[run] exchanges={EXCHANGES} duration={DURATION}s")
    kafka_proc = start_kafka()
    producers = start_producers()
    spark = start_spark()

    print(f"[spark] waiting for first batch (up to 3 min)...")
    if not wait_for_first_batch():
        print("[FATAL] Spark never produced a batch. Check spark_run.log.")
        kill_tree(spark.pid)
        for _, p in producers:
            kill_tree(p.pid)
        sys.exit(1)
    print("[spark] first batch received, starting timer")

    print(f"[run] collecting data for {DURATION}s...")
    deadline = time.time() + DURATION
    while time.time() < deadline:
        remaining = int(deadline - time.time())
        print(f"  ...{remaining}s left", end="\r")
        time.sleep(5)
    print()

    print("[run] shutting down producers + spark")
    for _, p in producers:
        kill_tree(p.pid)
    kill_tree(spark.pid)
    time.sleep(3)

    parse_log()

    if kafka_proc is not None:
        print("\n[kafka] was started by this script; leaving running.")
        print("       stop with: taskkill /F /T /PID", kafka_proc.pid)


if __name__ == "__main__":
    main()
