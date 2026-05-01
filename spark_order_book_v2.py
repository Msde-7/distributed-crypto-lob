"""Stateful-streaming version of spark_order_book.py.

Profiling showed ~75% of each batch was spent in `collect()` shipping rows
from executors to the driver. This version keeps OrderBook state on the
executors via Spark's `applyInPandasWithState`. Per-book apply happens
locally on the executor that owns that key; only the per-batch snapshot
rows (12 of them) reach the driver.

Drop-in replacement: same Kafka topic, same schema, same parquet sink path
shape (`/data/lob_snapshots/batch_id=N/`). Driver-side foreachBatch is kept
for the small per-batch sink write only (12 rows, trivial).

Run: same docker invocation as spark_order_book.py but with /app/spark_order_book_v2.py
"""
import os
import pickle
import time
from typing import Iterator, Tuple

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit
from pyspark.sql.streaming.state import GroupState, GroupStateTimeout
from pyspark.sql.types import (
    BinaryType, BooleanType, DoubleType, IntegerType, LongType,
    StringType, StructField, StructType,
)

from order_book import OrderBook

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "lob-events")
STARTING_OFFSETS = os.environ.get("STARTING_OFFSETS", "earliest")
SNAPSHOT_SINK_DIR = os.environ.get("SNAPSHOT_SINK_DIR", "/data/lob_snapshots")
MAX_OFFSETS = int(os.environ.get("MAX_OFFSETS_PER_TRIGGER", "200000"))
CHECKPOINT_DIR = os.environ.get("CHECKPOINT_DIR", "/tmp/lob_v2_checkpoint")

EXCHANGE_MAX_DEPTH = {"kraken": 10}


spark = (SparkSession.builder
    .appName("LobOrderBookStateful")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# raw event schema (matches what producers emit through normalizer.py and
# producer_synth.py)
INPUT_SCHEMA = StructType([
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
    StructField("ingest_ns", LongType(), True),
])

# per-batch per-book output. one row per (exchange, symbol) per batch.
OUTPUT_SCHEMA = StructType([
    StructField("exchange", StringType(), False),
    StructField("symbol", StringType(), False),
    StructField("best_bid", DoubleType(), True),
    StructField("best_ask", DoubleType(), True),
    StructField("spread", DoubleType(), True),
    StructField("last_sequence", LongType(), True),
    StructField("gap_count", IntegerType(), True),
    StructField("needs_resync", BooleanType(), True),
    StructField("event_count", IntegerType(), False),
    StructField("max_latency_ms", DoubleType(), True),
    StructField("ts_ns", LongType(), False),
])

# state held on executor per (exchange, symbol). pickled OrderBook + last
# emit timestamp. spark requires this be a struct of primitives or binary.
STATE_SCHEMA = StructType([
    StructField("book_pickle", BinaryType(), True),
])


def apply_book(key: Tuple[str, str],
               pdfs: Iterator[pd.DataFrame],
               state: GroupState) -> Iterator[pd.DataFrame]:
    """Apply incoming events for one book on the executor, emit a snapshot row.

    state.book_pickle holds the pickled OrderBook between batches.
    """
    exchange, symbol = key

    if state.exists:
        (book_bytes,) = state.get
        book = pickle.loads(book_bytes)
    else:
        book = OrderBook(symbol, max_depth=EXCHANGE_MAX_DEPTH.get(exchange))

    n_events = 0
    max_latency_ns = 0
    process_ns = time.time_ns()

    for pdf in pdfs:
        if pdf.empty:
            continue

        # sort by sequence; snapshots before updates at the same seq so a
        # mid-batch reconnect-snapshot resets state before the diffs after it
        pdf = pdf.sort_values(
            by=["sequence", "is_snapshot"],
            ascending=[True, False],
            na_position="first",
        )

        snaps_mask = pdf["is_snapshot"] == True
        snaps = pdf[snaps_mask]
        updates = pdf[~snaps_mask]

        if not snaps.empty:
            book.load_snapshot(snaps.to_dict("records"))

        for ev in updates.to_dict("records"):
            book.apply_event(ev)
            n_events += 1
            ing = ev.get("ingest_ns")
            if ing is not None:
                lat = process_ns - int(ing)
                if lat > max_latency_ns:
                    max_latency_ns = lat
            if book.needs_resync:
                # in stateful streaming we cant cleanly call out to REST mid-task.
                # the producer-side ws-reconnect path will deliver a fresh
                # is_snapshot=true event next batch, which will hit load_snapshot
                # above and clear needs_resync. drop remaining events here.
                break

    # persist updated book back into spark state
    state.update((pickle.dumps(book),))

    snap = book.snapshot()
    yield pd.DataFrame([{
        "exchange":       exchange,
        "symbol":         symbol,
        "best_bid":       snap["best_bid"],
        "best_ask":       snap["best_ask"],
        "spread":         snap["spread"],
        "last_sequence":  snap["last_sequence"],
        "gap_count":      int(snap["gap_count"]),
        "needs_resync":   bool(snap["needs_resync"]),
        "event_count":    int(n_events),
        "max_latency_ms": (max_latency_ns / 1e6) if max_latency_ns > 0 else None,
        "ts_ns":          int(process_ns),
    }])


def write_batch(batch_df, batch_id: int):
    """Write the per-batch 12-row snapshot dataframe to parquet + log metrics.

    The dataframe arriving here is small (one row per book that had events
    this batch), so collect()ing for logging is cheap. parquet write is also
    cheap because rows are few; spark distributes the write back out to
    executors but each task only handles 0 or 1 rows.
    """
    t_total_start = time.time()
    rows = batch_df.collect()
    t_collect = time.time() - t_total_start
    n_groups = len(rows)
    total_events = sum(r["event_count"] for r in rows)
    max_latency = max((r["max_latency_ms"] or 0.0) for r in rows) if rows else 0.0
    gap_lines = "\n".join(
        f"[{r['exchange']}:{r['symbol']}] best_bid={r['best_bid']} "
        f"best_ask={r['best_ask']} spread={r['spread']} "
        f"last_sequence={r['last_sequence']} "
        f"needs_resync={r['needs_resync']} gap_count={r['gap_count']} "
        f"events={r['event_count']}"
        for r in rows
    )

    if rows and SNAPSHOT_SINK_DIR:
        try:
            t_w = time.time()
            (batch_df.withColumn("batch_id", lit(batch_id))
                .write.mode("overwrite")
                .parquet(f"{SNAPSHOT_SINK_DIR}/batch_id={batch_id}"))
            t_write = time.time() - t_w
        except Exception as e:
            t_write = -1.0
            print(f"[sink] parquet write failed for batch {batch_id}: {e}")
    else:
        t_write = 0.0

    elapsed = time.time() - t_total_start

    print(f"\n===== BATCH {batch_id} | groups={n_groups} | events={total_events} =====")
    if gap_lines:
        print(gap_lines)
    print("\n----- METRICS -----")
    print(f"batch_id={batch_id}")
    print(f"batch_records={total_events}")
    print(f"batch_groups={n_groups}")
    print(f"batch_time_sec={elapsed:.4f}")
    print(f"t_collect_sec={t_collect:.4f}")
    print(f"t_write_sec={t_write:.4f}")
    print(f"max_latency_ms={max_latency:.2f}")
    if elapsed > 0 and total_events > 0:
        print(f"batch_records_per_sec={total_events/elapsed:.2f}")
    print(f"total_records=accumulated_externally")
    print("-------------------")


# ---------------------------------------------------------------------------
# build the streaming query
# ---------------------------------------------------------------------------
raw_df = (spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .option("maxOffsetsPerTrigger", MAX_OFFSETS)
    .load())

parsed = (raw_df
    .selectExpr("CAST(value AS STRING) AS value_str")
    .select(from_json(col("value_str"), INPUT_SCHEMA).alias("data"))
    .select("data.*")
    .filter(col("exchange").isNotNull() & col("symbol").isNotNull()))

snapshot_stream = (parsed
    .groupBy("exchange", "symbol")
    .applyInPandasWithState(
        func=apply_book,
        outputStructType=OUTPUT_SCHEMA,
        stateStructType=STATE_SCHEMA,
        outputMode="update",
        timeoutConf=GroupStateTimeout.NoTimeout,
    ))

query = (snapshot_stream.writeStream
    .foreachBatch(write_batch)
    .outputMode("update")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start())

query.awaitTermination()
