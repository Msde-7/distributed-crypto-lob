"""Binance WebSocket to Kafka producer.

Binance diff streams never include a snapshot, so bootstrap each symbol with
a REST depth snapshot, publish it as is_snapshot=True, then drain any diffs
recieved during the fetch. The order book drops diffs with u <= lastUpdateId
and accepts the first diff with U <= L+1 <= u.

Usage:
    python producer_binance.py
    python producer_binance.py BTC-USDT,ETH-USDT
"""
import json
import os
import sys
import threading
import time

import websocket
from kafka import KafkaProducer

from normalizer import normalize_binance_message
from partitioning import partition_for
from resync import load_binance_snapshot


KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "lob-events")
# Binance geofences stream.binance.com from the US. Default to Binance.US;
# set BINANCE_WS_BASE=wss://stream.binance.com:9443/stream for non-US hosts.
BINANCE_WS_BASE = os.environ.get("BINANCE_WS_BASE", "wss://stream.binance.us:9443/stream")


def _parse_symbols(argv):
    if len(argv) > 1:
        return [s.strip().upper() for s in argv[1].split(",") if s.strip()]
    env = os.environ.get("BINANCE_PRODUCTS", "")
    if env.strip():
        return [s.strip().upper() for s in env.split(",") if s.strip()]
    return ["BTC-USDT"]


def _combined_stream_url(symbols):
    parts = [f"{s.replace('-', '').lower()}@depth@100ms" for s in symbols]
    return f"{BINANCE_WS_BASE}?streams={'/'.join(parts)}"


class BinanceProducer:
    def __init__(self, symbols):
        self.symbols = symbols
        self.symbol_map = {s.replace("-", "").upper(): s for s in symbols}
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.lock = threading.Lock()
        self.buffered = {s: [] for s in symbols}
        self.bootstrapped = {s: False for s in symbols}
        self.snapshot_thread_started = False

    def _publish(self, event):
        self.producer.send(
            KAFKA_TOPIC,
            key=event["symbol"],
            value=event,
            partition=partition_for(event["exchange"], event["symbol"]),
        )

    def _bootstrap_symbol(self, symbol):
        try:
            snap = load_binance_snapshot(symbol)
        except Exception as e:
            print(f"[binance] REST snapshot failed for {symbol}: {e}", flush=True)
            time.sleep(2)
            snap = load_binance_snapshot(symbol)

        last_update_id = snap["sequence"]
        print(f"[binance] snapshot {symbol} lastUpdateId={last_update_id} "
              f"bids={len(snap['bids'])} asks={len(snap['asks'])}", flush=True)

        for price, qty in snap["bids"]:
            self._publish({
                "exchange": "binance", "symbol": symbol, "side": "bid",
                "price": price, "quantity": qty, "event_time": None,
                "sequence": last_update_id, "first_sequence": last_update_id,
                "is_snapshot": True, "checksum": None,
            })
        for price, qty in snap["asks"]:
            self._publish({
                "exchange": "binance", "symbol": symbol, "side": "ask",
                "price": price, "quantity": qty, "event_time": None,
                "sequence": last_update_id, "first_sequence": last_update_id,
                "is_snapshot": True, "checksum": None,
            })

        with self.lock:
            drained = self.buffered[symbol]
            self.buffered[symbol] = []
            self.bootstrapped[symbol] = True

        dropped = 0
        forwarded = 0
        for ev in drained:
            if ev["sequence"] is not None and ev["sequence"] <= last_update_id:
                dropped += 1
                continue
            self._publish(ev)
            forwarded += 1
        self.producer.flush()
        print(f"[binance] {symbol} bootstrapped: forwarded={forwarded} dropped={dropped}",
              flush=True)

    def _start_bootstrap_once(self):
        if self.snapshot_thread_started:
            return
        self.snapshot_thread_started = True
        for symbol in self.symbols:
            t = threading.Thread(target=self._bootstrap_symbol, args=(symbol,),
                                 daemon=True, name=f"binance-bootstrap-{symbol}")
            t.start()

    def on_open(self, ws):
        print(f"[binance] connected; streams={self.symbols}", flush=True)
        # small delay so the buffer holds some diffs that overlap the snapshot
        threading.Timer(0.5, self._start_bootstrap_once).start()

    def on_message(self, ws, message):
        ingest_ns = time.time_ns()
        try:
            events = normalize_binance_message(message, symbol_map=self.symbol_map)
            if not events:
                return
            for ev in events:
                ev["ingest_ns"] = ingest_ns

            symbol = events[0]["symbol"]
            if symbol not in self.buffered:
                return

            with self.lock:
                if not self.bootstrapped[symbol]:
                    self.buffered[symbol].extend(events)
                    return

            for ev in events:
                self._publish(ev)
            self.producer.flush()
        except Exception as e:
            print(f"[binance] on_message error: {e}", flush=True)

    def on_error(self, ws, error):
        print(f"[binance] WS error: {error}", flush=True)

    def on_close(self, ws, code, msg):
        print(f"[binance] WS closed: code={code} msg={msg}", flush=True)

    def run(self):
        url = _combined_stream_url(self.symbols)
        print(f"[binance] connecting to {url}", flush=True)
        ws = websocket.WebSocketApp(
            url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close,
        )
        ws.run_forever(ping_interval=60, ping_timeout=10)


if __name__ == "__main__":
    BinanceProducer(_parse_symbols(sys.argv)).run()
