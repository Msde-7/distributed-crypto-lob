import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

from resync import load_snapshot
from order_book import OrderBook

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "lob-events")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
# Directory for the Parquet snapshot sink. One partitioned file per batch so
# re-processing after a failure overwrites the same path (idempotent, which
# is what the report's exactly-once claim relies on).
SNAPSHOT_SINK_DIR = os.environ.get("SNAPSHOT_SINK_DIR", "")

spark = SparkSession.builder \
    .appName("LOBOrderBook") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("exchange", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("side", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("sequence", LongType(), True),
    StructField("first_sequence", LongType(), True),
    StructField("is_snapshot", BooleanType(), True),
    StructField("checksum", LongType(), True),
    # time.time_ns() stamped at WS on_message in the producer, None on REST
    # bootstrap snapshots (they don't represent live-feed arrivals).
    StructField("ingest_ns", LongType(), True),
])


def _percentile(sorted_vals, pct):
    if not sorted_vals:
        return None
    k = (len(sorted_vals) - 1) * pct
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)

# cap per exchange matches the depth window the feed actually streams
EXCHANGE_MAX_DEPTH = {"kraken": 10}

# coinbase/kraken re-emit a fresh ws snapshot on reconnect; binance has no
# ws snapshot path so we fetch from rest
RESYNC_STRATEGY = {
    "coinbase": "ws",
    "binance": "rest",
    "kraken": "ws",
}

# one book per (exchange, symbol) so venues never get mixed
books = {}
# per-book resync timestamps to avoid hammering REST during sustaned gaps
last_resync_at = {}
RESYNC_MIN_INTERVAL_SEC = 2.0

metrics = {
    "total_batches": 0,
    "total_records": 0,
    "total_resyncs": 0,
}


def _book_key(exchange, symbol):
    return f"{exchange}:{symbol}"


def process_batch(batch_df, batch_id):
    start_time = time.time()
    process_ns = time.time_ns()

    t_collect_start = time.time()
    rows = batch_df.collect()
    t_collect = time.time() - t_collect_start

    if not rows:
        print(f"\n===== BATCH {batch_id} | records=0 =====")
        return

    metrics["total_batches"] += 1
    metrics["total_records"] += len(rows)

    t_group_start = time.time()
    grouped = {}
    latencies_ns = []
    for row in rows:
        event = row.asDict()
        key = _book_key(event.get("exchange"), event.get("symbol"))
        grouped.setdefault(key, []).append(event)
        ing = event.get("ingest_ns")
        if ing is not None:
            latencies_ns.append(process_ns - ing)
    t_group = time.time() - t_group_start
    snapshot_rows = []

    print(f"\n===== BATCH {batch_id} | records={len(rows)} =====")

    t_apply_start = time.time()
    for key, events in grouped.items():
        exchange, symbol = key.split(":", 1)
        # snapshot events before updates within the same sequence so a
        # REST-bootstrapped snapshot applies before bufferred diffs
        events = sorted(
            events,
            key=lambda x: (
                x["sequence"] if x["sequence"] is not None else -1,
                0 if x.get("is_snapshot") else 1,
            ),
        )

        if key not in books:
            books[key] = OrderBook(symbol, max_depth=EXCHANGE_MAX_DEPTH.get(exchange))
        book = books[key]

        snapshot_events = [e for e in events if e["is_snapshot"]]
        update_events = [e for e in events if not e["is_snapshot"]]

        if snapshot_events:
            print(f"[{key}] Loading snapshot with {len(snapshot_events)} levels")
            book.load_snapshot(snapshot_events)

        for event in update_events:
            book.apply_event(event)

            if book.needs_resync:
                strategy = RESYNC_STRATEGY.get(exchange, "rest")
                if strategy == "ws":
                    # Producer will reconnect its WS; the fresh snapshot will
                    # flow through Kafka as is_snapshot=True events and
                    # load_snapshot will reset the book. Just stop applying
                    # further updates in this batch.
                    print(f"[{key}] Gap detected, waiting for producer WS-snapshot")
                    break
                now = time.time()
                last = last_resync_at.get(key, 0.0)
                if now - last < RESYNC_MIN_INTERVAL_SEC:
                    print(f"[{key}] Resync debounced (last was {now - last:.2f}s ago)")
                    break
                print(f"[{key}] Resync triggered from REST snapshot...")
                try:
                    snap = load_snapshot(exchange, symbol)
                    book.reset_from_snapshot(snap)
                    last_resync_at[key] = now
                    metrics["total_resyncs"] += 1
                    print(f"[{key}] Resync complete at sequence {book.last_sequence}")
                except Exception as e:
                    print(f"[{key}] Resync failed: {e}")
                    break

        snap = book.snapshot()

        if snap["best_bid"] and snap["best_ask"]:
            if snap["best_bid"] > snap["best_ask"]:
                print(f"[ERROR] Invalid book for {key}: bid > ask")

        print(f"[{key}] best_bid={snap['best_bid']} best_ask={snap['best_ask']} spread={snap['spread']}")
        print(f"[{key}] top_bids={snap['top_bids']}")
        print(f"[{key}] top_asks={snap['top_asks']}")
        print(
            f"[{key}] last_sequence={snap['last_sequence']} "
            f"needs_resync={snap['needs_resync']} "
            f"gap_count={snap['gap_count']} "
            f"old_event_count={snap['old_event_count']}"
        )
        snapshot_rows.append({
            "batch_id": batch_id,
            "exchange": exchange,
            "symbol": symbol,
            "best_bid": snap["best_bid"],
            "best_ask": snap["best_ask"],
            "spread": snap["spread"],
            "last_sequence": snap["last_sequence"],
            "gap_count": snap["gap_count"],
            "ts_ns": process_ns,
        })

    t_apply = time.time() - t_apply_start

    elapsed = time.time() - start_time
    records_per_sec = len(rows) / elapsed if elapsed > 0 else 0

    # overwrite + batch-scoped path: retries on the same batch_id replace
    # rather than append, so the sink stays idempotent
    t_write_start = time.time()
    if SNAPSHOT_SINK_DIR and snapshot_rows:
        try:
            out = spark.createDataFrame(snapshot_rows)
            (out.write
                .mode("overwrite")
                .parquet(f"{SNAPSHOT_SINK_DIR}/batch_id={batch_id}"))
        except Exception as e:
            print(f"[sink] parquet write failed for batch {batch_id}: {e}")
    t_write = time.time() - t_write_start

    # end-to-end latency: producer ws-arrival -> spark batch. ingest_ns is per
    # ws-frame so same-tick events share a measurment. rest bootstraps are None.
    if latencies_ns:
        latencies_ns.sort()
        p50_ms = _percentile(latencies_ns, 0.50) / 1e6
        p95_ms = _percentile(latencies_ns, 0.95) / 1e6
        p99_ms = _percentile(latencies_ns, 0.99) / 1e6
        max_ms = latencies_ns[-1] / 1e6
        lat_n = len(latencies_ns)
    else:
        p50_ms = p95_ms = p99_ms = max_ms = float("nan")
        lat_n = 0

    print("\n----- METRICS -----")
    print(f"batch_id={batch_id}")
    print(f"batch_records={len(rows)}")
    print(f"batch_time_sec={elapsed:.4f}")
    print(f"t_collect_sec={t_collect:.4f}")
    print(f"t_group_sec={t_group:.4f}")
    print(f"t_apply_sec={t_apply:.4f}")
    print(f"t_write_sec={t_write:.4f}")
    print(f"batch_records_per_sec={records_per_sec:.2f}")
    print(f"latency_p50_ms={p50_ms:.2f}")
    print(f"latency_p95_ms={p95_ms:.2f}")
    print(f"latency_p99_ms={p99_ms:.2f}")
    print(f"latency_max_ms={max_ms:.2f}")
    print(f"latency_n={lat_n}")
    print(f"total_batches={metrics['total_batches']}")
    print(f"total_records={metrics['total_records']}")
    print(f"total_resyncs={metrics['total_resyncs']}")
    print("-------------------")


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", STARTING_OFFSETS) \
    .option("maxOffsetsPerTrigger", int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "50000"))) \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING) AS value_str") \
    .select(from_json(col("value_str"), schema).alias("data")) \
    .select("data.*")

query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/lob_order_book_checkpoint") \
    .start()

query.awaitTermination()
