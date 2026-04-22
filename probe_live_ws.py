"""Short live WS probe, no Kafka neccessary. Preflight before a real run."""
import json
import os
import sys
import threading
import time

import websocket

from normalizer import (
    normalize_coinbase_message,
    normalize_binance_message,
    normalize_kraken_message,
    reset_kraken_state,
)


DURATION = int(sys.argv[1]) if len(sys.argv) > 1 else 10


def _probe(name, url, on_open, normalize):
    frames = []
    events = []
    errors = []
    stop_evt = threading.Event()

    def on_message(ws, message):
        try:
            frames.append(message)
            evs = normalize(message)
            events.extend(evs)
        except Exception as e:
            errors.append(str(e))

    def on_error(ws, error):
        errors.append(str(error))

    def on_close(ws, code, msg):
        stop_evt.set()

    ws = websocket.WebSocketApp(
        url,
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close,
    )
    t = threading.Thread(target=lambda: ws.run_forever(ping_interval=30, ping_timeout=10),
                         daemon=True)
    t.start()
    time.sleep(DURATION)
    ws.close()
    stop_evt.wait(timeout=3)

    print(f"[{name}] frames={len(frames)} events={len(events)} errors={len(errors)}")
    if errors:
        print(f"[{name}]   first_error: {errors[0][:200]}")
    if events:
        sample = events[0]
        print(f"[{name}]   sample: exchange={sample['exchange']} symbol={sample['symbol']} "
              f"side={sample['side']} price={sample['price']} qty={sample['quantity']} "
              f"seq={sample['sequence']} first_seq={sample['first_sequence']} "
              f"is_snapshot={sample['is_snapshot']}")
        symbols = {e["symbol"] for e in events}
        print(f"[{name}]   symbols seen: {symbols}")
    return len(events) > 0 and not errors


def probe_coinbase():
    def on_open(ws):
        ws.send(json.dumps({
            "type": "subscribe", "product_ids": ["BTC-USD"], "channel": "level2"
        }))
    return _probe("coinbase", "wss://advanced-trade-ws.coinbase.com",
                  on_open, normalize_coinbase_message)


def probe_binance():
    ws_base = os.environ.get("BINANCE_WS_BASE", "wss://stream.binance.us:9443/stream")
    url = f"{ws_base}?streams=btcusdt@depth@100ms"

    def on_open(ws):
        pass  # Combined-stream URL already declares the subscription.

    def norm(msg):
        return normalize_binance_message(msg, symbol_map={"BTCUSDT": "BTC-USDT"})

    return _probe("binance", url, on_open, norm)


def probe_kraken():
    reset_kraken_state()

    def on_open(ws):
        ws.send(json.dumps({
            "method": "subscribe",
            "params": {"channel": "book", "symbol": ["BTC/USD"], "depth": 10},
        }))
    return _probe("kraken", "wss://ws.kraken.com/v2", on_open, normalize_kraken_message)


def main():
    print(f"Probing each exchange for {DURATION}s...\n")
    results = {
        "coinbase": probe_coinbase(),
        "binance": probe_binance(),
        "kraken": probe_kraken(),
    }
    print()
    for name, ok in results.items():
        print(f"  {name}: {'OK' if ok else 'FAIL'}")
    sys.exit(0 if all(results.values()) else 1)


if __name__ == "__main__":
    main()
