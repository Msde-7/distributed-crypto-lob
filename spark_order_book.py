import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, LongType

from resync import load_coinbase_snapshot
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
    StructField("is_snapshot", BooleanType(), True)
])

books = {}
metrics = {
    "total_batches": 0,
    "total_records": 0,
    "total_resyncs": 0
}

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
        symbol = event["symbol"]
        grouped.setdefault(symbol, []).append(event)

    print(f"\n===== BATCH {batch_id} | records={len(rows)} =====")

    for symbol, events in grouped.items():
        events = sorted(
            events,
            key=lambda x: (x["sequence"] if x["sequence"] is not None else -1)
        )

        if symbol not in books:
            books[symbol] = OrderBook(symbol)

        book = books[symbol]

        snapshot_events = [e for e in events if e["is_snapshot"]]
        update_events = [e for e in events if not e["is_snapshot"]]

        if snapshot_events:
            print(f"[{symbol}] Loading snapshot with {len(snapshot_events)} levels")
            book.load_snapshot(snapshot_events)

        for event in update_events:
            book.apply_event(event)

            if book.needs_resync:
                print(f"[{symbol}] Resync triggered from REST snapshot...")
                try:
                    snapshot = load_coinbase_snapshot(symbol)
                    book.reset_from_snapshot(snapshot)
                    metrics["total_resyncs"] += 1
                    print(f"[{symbol}] Resync complete at sequence {book.last_sequence}")
                except Exception as e:
                    print(f"[{symbol}] Resync failed: {e}")
                    break

        snap = book.snapshot()

        if snap["best_bid"] and snap["best_ask"]:
            if snap["best_bid"] > snap["best_ask"]:
                print("[ERROR] Invalid book: bid > ask")

        print(f"[{symbol}] best_bid={snap['best_bid']} best_ask={snap['best_ask']} spread={snap['spread']}")
        print(f"[{symbol}] top_bids={snap['top_bids']}")
        print(f"[{symbol}] top_asks={snap['top_asks']}")
        print(
            f"[{symbol}] last_sequence={snap['last_sequence']} "
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