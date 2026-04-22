"""Kraken WebSocket v2 to Kafka producer.

Kraken's book channel emits a snapshot on subscribe, then incremental updates
with a CRC32 checksum. There's no native sequence number, so the normalizer
stamps a local monotonic counter for ordering.

Usage:
    python producer_kraken.py
    python producer_kraken.py BTC-USD,ETH-USD
"""
import json
import os
import sys

import websocket
from kafka import KafkaProducer

from normalizer import normalize_kraken_message, reset_kraken_state


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "lob-events")
KRAKEN_WS_URL = "wss://ws.kraken.com/v2"
DEFAULT_DEPTH = int(os.environ.get("KRAKEN_DEPTH", "10"))


def _parse_symbols(argv):
    if len(argv) > 1:
        return [s.strip().upper() for s in argv[1].split(",") if s.strip()]
    return ["BTC-USD"]


def _ws_symbols(symbols):
    return [s.replace("-", "/").upper() for s in symbols]


class KrakenProducer:
    def __init__(self, symbols):
        self.symbols = symbols
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        reset_kraken_state()

    def on_open(self, ws):
        ws_syms = _ws_symbols(self.symbols)
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": ws_syms,
                "depth": DEFAULT_DEPTH,
            },
        }
        ws.send(json.dumps(msg))
        print(f"[kraken] subscribed to book channel: {ws_syms}", flush=True)

    def on_message(self, ws, message):
        try:
            events = normalize_kraken_message(message)
            if not events:
                return
            for ev in events:
                self.producer.send(KAFKA_TOPIC, key=ev["symbol"], value=ev)
            self.producer.flush()
        except Exception as e:
            print(f"[kraken] on_message error: {e}", flush=True)

    def on_error(self, ws, error):
        print(f"[kraken] WS error: {error}", flush=True)

    def on_close(self, ws, code, msg):
        print(f"[kraken] WS closed: code={code} msg={msg}", flush=True)

    def run(self):
        print(f"[kraken] connecting to {KRAKEN_WS_URL}", flush=True)
        ws = websocket.WebSocketApp(
            KRAKEN_WS_URL,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever(ping_interval=30, ping_timeout=10)


if __name__ == "__main__":
    KrakenProducer(_parse_symbols(sys.argv)).run()
