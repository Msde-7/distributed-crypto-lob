# distributed-crypto-lob

A distributed crypto limit-order-book pipeline: Coinbase level2 WebSocket → Kafka → Spark Structured Streaming, with gap detection and REST snapshot resync.

## Components

- `producer_stream.py` — consumes Coinbase `level2` WS frames and publishes normalized events to Kafka.
- `normalizer.py` — flattens Coinbase frames to `{exchange, symbol, side, price, quantity, sequence, ...}` events.
- `order_book.py` — in-memory L2 book with sequence-gap detection and resync state.
- `resync.py` — fetches a REST snapshot to rebuild the book after a gap.
- `spark_order_book.py` — Spark Structured Streaming job that consumes Kafka and maintains order books.
- `run_experiment.py` — orchestrates a timed end-to-end run (Kafka + producer + Spark), then parses the log.
- `parse_run.py` — standalone parser for `spark_run.log`.
- `start-kafka.sh`, `run-spark.sh` — convenience launchers (Windows / Git Bash).

## Setup

1. Install Java 17, Kafka (KRaft), Spark 3.5.x, Hadoop winutils (Windows), and Python 3.11.
2. `pip install kafka-python websocket-client requests pyspark`
3. `cp .env.example .env` and edit the paths for your machine. The launcher scripts currently read these as hardcoded Windows paths — update them to match your install, or set env vars before running.

## Run

```bash
# Terminal 1
./start-kafka.sh

# Terminal 2 — 5-minute end-to-end experiment
python run_experiment.py 300
```

Logs (`spark_run.log`, `producer.log`, `kafka.log`) are gitignored.

## Security

No credentials are required for the Coinbase level2 public feed. If you add an authenticated feed, put secrets in `.env` (gitignored) — never commit them.
