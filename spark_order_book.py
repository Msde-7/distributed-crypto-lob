import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

from resync import load_snapshot
from order_book import OrderBook

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
])

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

    rows = batch_df.collect()

    if not rows:
        print(f"\n===== BATCH {batch_id} | records=0 =====")
        return

    metrics["total_batches"] += 1
    metrics["total_records"] += len(rows)

    grouped = {}
    for row in rows:
        event = row.asDict()
        key = _book_key(event.get("exchange"), event.get("symbol"))
        grouped.setdefault(key, []).append(event)

    print(f"\n===== BATCH {batch_id} | records={len(rows)} =====")

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
            books[key] = OrderBook(symbol)
        book = books[key]

        snapshot_events = [e for e in events if e["is_snapshot"]]
        update_events = [e for e in events if not e["is_snapshot"]]

        if snapshot_events:
            print(f"[{key}] Loading snapshot with {len(snapshot_events)} levels")
            book.load_snapshot(snapshot_events)

        for event in update_events:
            book.apply_event(event)

            if book.needs_resync:
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
            f"old_event_count={snap['old_event_count']} "
            f"duplicate_count={snap['duplicate_count']}"
        )

    elapsed = time.time() - start_time
    records_per_sec = len(rows) / elapsed if elapsed > 0 else 0

    print("\n----- METRICS -----")
    print(f"batch_id={batch_id}")
    print(f"batch_records={len(rows)}")
    print(f"batch_time_sec={elapsed:.4f}")
    print(f"batch_records_per_sec={records_per_sec:.2f}")
    print(f"total_batches={metrics['total_batches']}")
    print(f"total_records={metrics['total_records']}")
    print(f"total_resyncs={metrics['total_resyncs']}")
    print("-------------------")


raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "lob-events") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 50000) \
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
