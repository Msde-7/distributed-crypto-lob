"""Coinbase Advanced Trade level2 WebSocket to Kafka producer.

Reconnects on socket close. Advanced Trade's sequence_num is a subscription-
wide counter shared with non-l2_data frames (subscriptions ACKs, etc.), so it
skips ahead between l2_data messages and is not usable as a per-channel gap
signal. We trust WebSocket ordering and rely on TCP close plus our ping to
detect real disruption. On any reconnect, Coinbase re-emits a fresh snapshot
that is time-aligned with subsequent updates, so load_snapshot resets the book
cleanly without the crossed-book artifact of a REST resync.
"""
import json
import os
import time

import websocket
from kafka import KafkaProducer

from normalizer import normalize_coinbase_message
from partitioning import partition_for

_NOW_NS = time.time_ns


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "lob-events")
COINBASE_WS_URL = os.environ.get(
    "COINBASE_WS_URL", "wss://advanced-trade-ws.coinbase.com"
)
PRODUCT_IDS = [s.strip() for s in os.environ.get("COINBASE_PRODUCTS", "BTC-USD").split(",") if s.strip()]


class CoinbaseProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def on_open(self, ws):
        print(f"[coinbase] connected, subscribing to {PRODUCT_IDS}", flush=True)
        ws.send(json.dumps({
            "type": "subscribe",
            "product_ids": PRODUCT_IDS,
            "channel": "level2",
        }))

    def on_message(self, ws, message):
        ingest_ns = _NOW_NS()
        try:
            events = normalize_coinbase_message(message)
            for ev in events:
                ev["ingest_ns"] = ingest_ns
                self.producer.send(
                    KAFKA_TOPIC,
                    key=ev["symbol"],
                    value=ev,
                    partition=partition_for(ev["exchange"], ev["symbol"]),
                )
            if events:
                self.producer.flush()
        except Exception as e:
            print(f"[coinbase] on_message error: {e}", flush=True)

    def on_error(self, ws, error):
        print(f"[coinbase] WS error: {error}", flush=True)

    def on_close(self, ws, code, msg):
        print(f"[coinbase] WS closed: code={code} msg={msg}", flush=True)

    def run(self):
        while True:
            ws = websocket.WebSocketApp(
                COINBASE_WS_URL,
                on_open=self.on_open,
                on_message=self.on_message,
                on_error=self.on_error,
                on_close=self.on_close,
            )
            ws.run_forever(ping_interval=30, ping_timeout=10)
            time.sleep(1)


if __name__ == "__main__":
    CoinbaseProducer().run()
