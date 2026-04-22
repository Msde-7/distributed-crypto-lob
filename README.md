# distributed-crypto-lob

Distributed crypto limit-order-book pipeline. Exchange WebSocket feeds to
Kafka to Spark Structured Streaming, with sequence-gap detection and REST
snapshot resync. Three exchanges supported accross one unified event schema, so the Spark
side is exchange-agnostic.

## Components

- `normalizer.py`: per-exchange frame normalizers. All return events in
  `{exchange, symbol, side, price, quantity, event_time, sequence,
  first_sequence, is_snapshot, checksum}`.
- `order_book.py`: in-memory L2 book. One gap rule covers Coinbase
  single-seq ticks and Binance `[U..u]` windows via `first_sequence`.
- `resync.py`: REST snapshot fetchers + `load_snapshot(exchange, symbol)`
  dispatcher. Binance host configurable (defaults to Binance.US).
- `producer_stream.py`: Coinbase Advanced Trade level2 to Kafka.
- `producer_binance.py`: Binance combined diff-depth to Kafka. Bootstraps
  each symbol with a REST snapshot, then replays buffered diffs.
- `producer_kraken.py`: Kraken v2 book channel to Kafka. CRC32 is carried
  through on the last event of each frame.
- `spark_order_book.py`: Spark Structured Streaming job. Groups by
  `(exchange, symbol)`, one `OrderBook` per pair, resync dispatched per
  exchange.
- `validate_adapters.py`: offline test of every normalizer + OrderBook.
- `probe_live_ws.py`: short live WS probe of all three feeds, no Kafka.
- `run_experiment.py`: starts Kafka + producers + Spark, times the run,
  parses the log.
- `parse_run.py`: standalone parser for `spark_run.log`.
- `start-kafka.sh`, `run-spark.sh`: convenience launchers.

## Setup

1. Install Java 17, Kafka (KRaft), Spark 3.5.x, Hadoop winutils (Windows),
   Python 3.11.
2. `pip install kafka-python websocket-client requests pyspark`
3. `cp .env.example .env` and edit paths / hosts for your machine.

## Verify before running live

```bash
python validate_adapters.py    # offline, asserts all three adapters pass
python probe_live_ws.py 8      # ~8s live per exchange, no Kafka
```

## Run

```bash
# Terminal 1
./start-kafka.sh

# Terminal 2, Coinbase only (original default)
python run_experiment.py 300

# All three exchanges concurrently
EXCHANGES=coinbase,binance,kraken python run_experiment.py 300
```

Logs (`spark_run.log`, `producer_<exchange>.log`, `kafka.log`) are gitignored.

## Security

No credentials are needed for any of the public feeds here. If you add an
authenticated channel later, put secrets in `.env` (gitignored).
