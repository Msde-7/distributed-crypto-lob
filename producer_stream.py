"""Coinbase Advanced Trade level2 ws -> kafka. reconnects on close, trusts
ws ordering instead of sequence_num (which is subscription-wide and skips
between l2_data frames). the reconnect snapshot resets the book.

First producer made (Coinbase). Updated from Aishwarya's original implementation to the more modern,
feature ritch api

api: https://docs.cdp.coinbase.com/coinbase-app/advanced-trade-apis/websocket/websocket-channels
"""
import json
import os
import time

import websocket
from kafka import KafkaProducer

from normalizer import normalize_coinbase_message
from partitioning import partition_for


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
        ingest_ns = time.time_ns()
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
