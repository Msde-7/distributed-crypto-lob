"""Synthetic load producer.

Pushes schema-correct events into kafka at a target rate so the scaling
sweep can go past the live-feed ceiling (~30k/s across all 12 books).
Per-book seq counters increment by 1 so the gap rule never fires.

Envs: SYNTH_RATE (10k), SYNTH_BOOKS (all 12), SYNTH_DURATION (60s, 0=forever),
SYNTH_SEED (42). KAFKA_BOOTSTRAP / KAFKA_TOPIC like the live producers.

    SYNTH_RATE=20000 SYNTH_DURATION=60 python3 producer_synth.py
"""
import json
import os
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

from partitioning import partition_for


# starting mids, roughly current as of project work in 2026
STARTING_PRICES = {
    "coinbase:BTC-USD": 76000.0,
    "binance:BTC-USDT": 76000.0,
    "kraken:BTC-USD": 76000.0,
    "coinbase:ETH-USD": 2275.0,
    "binance:ETH-USDT": 2275.0,
    "kraken:ETH-USD": 2275.0,
    "coinbase:SOL-USD": 83.5,
    "binance:SOL-USDT": 83.5,
    "kraken:SOL-USD": 83.5,
    "coinbase:LTC-USD": 55.0,
    "binance:LTC-USDT": 55.0,
    "kraken:LTC-USD": 55.0,
}


def _now_iso():
    return datetime.now(tz=timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _new_event(exchange, symbol, seq, price, side, qty, is_snapshot=False):
    # single-event "range" (first==seq), so the gap rule never fires while we
    # increment seq by 1
    return {
        "exchange": exchange,
        "symbol": symbol,
        "side": side,
        "price": round(price, 4),
        "quantity": round(qty, 6),
        "event_time": _now_iso(),
        "sequence": seq,
        "first_sequence": seq,
        "is_snapshot": is_snapshot,
        "checksum": None,
        "ingest_ns": time.time_ns(),
    }


def main():
    rate = int(os.environ.get("SYNTH_RATE", "10000"))
    duration = int(os.environ.get("SYNTH_DURATION", "60"))
    seed = int(os.environ.get("SYNTH_SEED", "42"))
    books = os.environ.get("SYNTH_BOOKS", "")
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    topic = os.environ.get("KAFKA_TOPIC", "lob-events")

    book_list = ([b.strip() for b in books.split(",") if b.strip()]
                 if books else list(STARTING_PRICES.keys()))

    rng = random.Random(seed)

    # per-book state: next sequence number + current mid price
    state = {b: {"seq": 1, "mid": STARTING_PRICES.get(b, 100.0)} for b in book_list}

    # snappy/lz4/gzip needs extra deps (python-snappy, lz4) that arent in the
    # default pip set, so off unless asked
    compression = os.environ.get("SYNTH_COMPRESSION") or None

    producer_kwargs = dict(
        bootstrap_servers=bootstrap.split(","),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else k,
        # without batching, single-event sends cap at ~5k/s on python
        linger_ms=5,
        batch_size=65536,
        acks=int(os.environ.get("SYNTH_ACKS", "1")),  # bump to "all" w/ multi-broker
    )
    if compression:
        producer_kwargs["compression_type"] = compression
    producer = KafkaProducer(**producer_kwargs)

    # bootstrap each book with one snapshot event per side, so spark has
    # somethign to load_snapshot from on its first batch
    for b in book_list:
        ex, sym = b.split(":", 1)
        s = state[b]
        for side in ("bid", "ask"):
            ev = _new_event(ex, sym, s["seq"], s["mid"], side, 1.0, is_snapshot=True)
            producer.send(topic, key=sym, value=ev, partition=partition_for(ex, sym))
            s["seq"] += 1

    print(f"[synth] target_rate={rate}/s books={len(book_list)} duration={duration or 'forever'}s",
          flush=True)
    producer.flush()

    # pace per chunk of TICK events, not per-event - per-event time.sleep
    # cant hit > ~5k/s on python
    TICK = 200
    tick_window_ns = int(1e9 * TICK / rate)
    sent = 0
    started_ns = time.time_ns()
    last_log_ns = started_ns
    last_tick_ns = started_ns

    try:
        while True:
            # wait out the tick window every TICK events
            if sent > 0 and sent % TICK == 0:
                now_ns = time.time_ns()
                elapsed = now_ns - last_tick_ns
                if elapsed < tick_window_ns:
                    time.sleep((tick_window_ns - elapsed) / 1e9)
                last_tick_ns = time.time_ns()

            # pick a book uniformly
            b = book_list[rng.randrange(len(book_list))]
            ex, sym = b.split(":", 1)
            s = state[b]

            # fixed 10-level grid per side around a constant mid, so old
            # levels dont go stale and the book never crosses. qty=0 deletes.
            level = rng.randrange(10)
            offset = 0.5 + level                 # 0.5, 1.5, ..., 9.5
            side = "bid" if rng.random() < 0.5 else "ask"
            price = s["mid"] - offset if side == "bid" else s["mid"] + offset
            # 10% chance the event is a level-removal (qty=0); the rest
            # are size updates with positive qty
            qty = 0.0 if rng.random() < 0.10 else abs(rng.gauss(1.0, 0.5))

            ev = _new_event(ex, sym, s["seq"], price, side, qty)
            producer.send(topic, key=sym, value=ev, partition=partition_for(ex, sym))
            s["seq"] += 1
            sent += 1

            # log actual sustaned rate every 5 seconds (only check at tick
            # boundaries to avoid time.time_ns() on every event)
            if sent % TICK == 0:
                now_ns = time.time_ns()
                if now_ns - last_log_ns > 5_000_000_000:
                    elapsed = (now_ns - started_ns) / 1e9
                    actual = sent / elapsed if elapsed > 0 else 0.0
                    print(f"[synth] sent={sent} elapsed={elapsed:.1f}s "
                          f"actual={actual:.0f}/s target={rate}/s", flush=True)
                    last_log_ns = now_ns

                # stop if duration elapsed (0 = forever)
                if duration > 0 and (now_ns - started_ns) / 1e9 >= duration:
                    break
    finally:
        producer.flush(timeout=10)
        producer.close()
        elapsed = (time.time_ns() - started_ns) / 1e9
        actual = sent / elapsed if elapsed > 0 else 0.0
        print(f"[synth] DONE sent={sent} target={rate}/s actual={actual:.0f}/s "
              f"elapsed={elapsed:.1f}s", flush=True)


if __name__ == "__main__":
    main()
