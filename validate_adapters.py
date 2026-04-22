"""Offline validation for the three exchange adapters.

Feeds canned frames through each normalizer and OrderBook and asserts the
resulting state. No network, runs anywhere.
"""
import json
import sys

from normalizer import (
    normalize_coinbase_message,
    normalize_binance_message,
    normalize_kraken_message,
    reset_kraken_state,
)
from order_book import OrderBook


def _check(cond, label):
    if not cond:
        print(f"  FAIL: {label}")
        return False
    print(f"  ok  : {label}")
    return True


def validate_coinbase():
    print("\n[coinbase]")
    results = []
    book = OrderBook("BTC-USD")

    snap_frame = json.dumps({
        "channel": "l2_data", "sequence_num": 0,
        "events": [{
            "type": "snapshot", "product_id": "BTC-USD", "updates": [
                {"side": "bid", "event_time": "t", "price_level": "100", "new_quantity": "1"},
                {"side": "bid", "event_time": "t", "price_level": "99",  "new_quantity": "2"},
                {"side": "offer", "event_time": "t", "price_level": "101", "new_quantity": "3"},
                {"side": "offer", "event_time": "t", "price_level": "102", "new_quantity": "4"},
            ]
        }]
    })
    snap_events = normalize_coinbase_message(snap_frame)
    results.append(_check(len(snap_events) == 4, "snapshot produces 4 events"))
    results.append(_check(all(e["is_snapshot"] for e in snap_events), "snapshot flags are True"))
    book.load_snapshot(snap_events)
    results.append(_check(book.best_bid() == 100.0 and book.best_ask() == 101.0,
                          "book top-of-book after snapshot = 100/101"))
    results.append(_check(book.last_sequence == 0, "last_sequence = 0 after snapshot"))

    # Normal update at sequence 1 (flip best bid higher, delete an ask level).
    up = json.dumps({
        "channel": "l2_data", "sequence_num": 1,
        "events": [{
            "type": "update", "product_id": "BTC-USD", "updates": [
                {"side": "bid", "event_time": "t", "price_level": "100.5", "new_quantity": "5"},
                {"side": "offer", "event_time": "t", "price_level": "101", "new_quantity": "0"},
            ]
        }]
    })
    for e in normalize_coinbase_message(up):
        book.apply_event(e)
    results.append(_check(book.best_bid() == 100.5 and book.best_ask() == 102.0,
                          "after update: best_bid=100.5, best_ask=102 (101 deleted)"))
    results.append(_check(book.last_sequence == 1 and not book.needs_resync,
                          "last_sequence advanced to 1, no resync"))

    # Stale event at sequence 0 -> old_event_count increments, no state change.
    stale = json.dumps({
        "channel": "l2_data", "sequence_num": 0,
        "events": [{
            "type": "update", "product_id": "BTC-USD", "updates": [
                {"side": "bid", "event_time": "t", "price_level": "50", "new_quantity": "99"},
            ]
        }]
    })
    for e in normalize_coinbase_message(stale):
        book.apply_event(e)
    results.append(_check(book.old_event_count == 1 and 50.0 not in book.bids,
                          "stale event rejected, old_event_count = 1"))

    # Gap: jump from 1 -> 5.
    gap = json.dumps({
        "channel": "l2_data", "sequence_num": 5,
        "events": [{
            "type": "update", "product_id": "BTC-USD", "updates": [
                {"side": "bid", "event_time": "t", "price_level": "100.5", "new_quantity": "99"},
            ]
        }]
    })
    for e in normalize_coinbase_message(gap):
        book.apply_event(e)
    results.append(_check(book.needs_resync and book.gap_count == 1,
                          "gap (1 -> 5) detected, needs_resync flagged"))

    return all(results)


def validate_binance():
    print("\n[binance]")
    results = []
    book = OrderBook("BTC-USDT")

    # Simulate a REST snapshot at lastUpdateId=100 via the order book's own
    # reset_from_snapshot path (this is what Spark does after a gap).
    book.reset_from_snapshot({
        "exchange": "binance",
        "bids": [(50000.0, 1.0), (49999.0, 2.0)],
        "asks": [(50001.0, 1.5), (50002.0, 3.0)],
        "sequence": 100,
    })
    results.append(_check(book.last_sequence == 100,
                          "Binance REST snapshot anchors last_sequence = lastUpdateId"))

    # Straddling first diff: U=98, u=102 -> accepted (first diff after snapshot).
    straddle = json.dumps({
        "e": "depthUpdate", "E": 1, "s": "BTCUSDT",
        "U": 98, "u": 102,
        "b": [["50000", "0"], ["49998", "5"]],
        "a": [["50001", "0"]],
    })
    events = normalize_binance_message(straddle, symbol_map={"BTCUSDT": "BTC-USDT"})
    results.append(_check(len(events) == 3, "Binance diff produces 3 level events"))
    results.append(_check(all(e["first_sequence"] == 98 and e["sequence"] == 102 for e in events),
                          "first_sequence=U=98, sequence=u=102 on all events"))
    for e in events:
        book.apply_event(e)
    results.append(_check(not book.needs_resync and book.last_sequence == 102,
                          "straddle diff accepted, last_sequence=102"))
    results.append(_check(50000.0 not in book.bids,
                          "bid level 50000 deleted (qty=0)"))
    results.append(_check(book.bids.get(49998.0) == 5.0,
                          "new bid level 49998 = 5.0"))
    results.append(_check(book.best_ask() == 50002.0,
                          "best_ask now 50002 after 50001 deleted"))

    # Stale diff: u=102 (equal) -> same-tick; still accepted, no state damage.
    same_tick = json.dumps({
        "e": "depthUpdate", "E": 2, "s": "BTCUSDT",
        "U": 98, "u": 102,
        "b": [["49997", "10"]], "a": [],
    })
    for e in normalize_binance_message(same_tick, symbol_map={"BTCUSDT": "BTC-USDT"}):
        book.apply_event(e)
    results.append(_check(book.bids.get(49997.0) == 10.0 and book.last_sequence == 102,
                          "same-tick replay applied, last_sequence unchanged"))

    # Fully stale diff: u=90 < last_sequence=102 -> old event.
    old = json.dumps({
        "e": "depthUpdate", "E": 3, "s": "BTCUSDT",
        "U": 88, "u": 90,
        "b": [["40000", "99"]], "a": [],
    })
    before_old = book.old_event_count
    for e in normalize_binance_message(old, symbol_map={"BTCUSDT": "BTC-USDT"}):
        book.apply_event(e)
    results.append(_check(book.old_event_count > before_old and 40000.0 not in book.bids,
                          "stale Binance diff (u=90 < 102) rejected"))

    # Normal progression: U=103, u=105 -> accepted, last_sequence=105.
    normal = json.dumps({
        "e": "depthUpdate", "E": 4, "s": "BTCUSDT",
        "U": 103, "u": 105,
        "b": [["49999", "0"]], "a": [["50003", "7"]],
    })
    for e in normalize_binance_message(normal, symbol_map={"BTCUSDT": "BTC-USDT"}):
        book.apply_event(e)
    results.append(_check(book.last_sequence == 105 and 49999.0 not in book.bids,
                          "normal diff applied, 49999 deleted"))

    # Gap: U=200, u=205 with last_sequence=105 -> needs_resync.
    gap = json.dumps({
        "e": "depthUpdate", "E": 5, "s": "BTCUSDT",
        "U": 200, "u": 205,
        "b": [["49990", "1"]], "a": [],
    })
    for e in normalize_binance_message(gap, symbol_map={"BTCUSDT": "BTC-USDT"}):
        book.apply_event(e)
    results.append(_check(book.needs_resync and book.gap_count == 1,
                          "gap (U=200 > last+1=106) detected"))

    # Combined-stream wrapper is unwrapped correctly.
    wrapped = json.dumps({
        "stream": "btcusdt@depth@100ms",
        "data": {"e": "depthUpdate", "E": 6, "s": "BTCUSDT",
                 "U": 300, "u": 305,
                 "b": [["50000", "1"]], "a": []},
    })
    unwrapped = normalize_binance_message(wrapped, symbol_map={"BTCUSDT": "BTC-USDT"})
    results.append(_check(len(unwrapped) == 1 and unwrapped[0]["first_sequence"] == 300,
                          "combined-stream envelope unwrapped"))

    return all(results)


def validate_kraken():
    print("\n[kraken]")
    results = []
    reset_kraken_state()
    book = OrderBook("BTC-USD")

    snap = json.dumps({
        "channel": "book", "type": "snapshot", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": 50000, "qty": 1.0}, {"price": 49999, "qty": 2.0}],
            "asks": [{"price": 50001, "qty": 1.5}, {"price": 50002, "qty": 3.0}],
            "checksum": 123456789,
            "timestamp": "2026-04-22T00:00:00.000000Z",
        }]
    })
    events = normalize_kraken_message(snap)
    results.append(_check(len(events) == 4, "snapshot expands to 4 level events"))
    results.append(_check(all(e["is_snapshot"] for e in events), "is_snapshot=True on all events"))
    results.append(_check(events[-1]["checksum"] == 123456789 and events[0]["checksum"] is None,
                          "checksum attached to last event only"))
    results.append(_check(events[0]["symbol"] == "BTC-USD", "symbol normalized BTC/USD -> BTC-USD"))
    book.load_snapshot(events)
    results.append(_check(book.best_bid() == 50000 and book.best_ask() == 50001,
                          "snapshot loaded correctly"))
    results.append(_check(book.last_sequence == 0, "snapshot assigns counter=0"))

    up = json.dumps({
        "channel": "book", "type": "update", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": 50000, "qty": 2.5}],
            "asks": [{"price": 50001, "qty": 0}],  # delete
            "checksum": 222222,
            "timestamp": "2026-04-22T00:00:01.000000Z",
        }]
    })
    for e in normalize_kraken_message(up):
        book.apply_event(e)
    results.append(_check(book.bids[50000.0] == 2.5 and 50001.0 not in book.asks,
                          "update applied: bid quantity changed, ask level deleted"))
    results.append(_check(book.last_sequence == 1 and not book.needs_resync,
                          "counter advanced to 1, no resync"))

    # Second update; counter should advance to 2 and be strictly monotonic.
    up2 = json.dumps({
        "channel": "book", "type": "update", "data": [{
            "symbol": "BTC/USD",
            "bids": [{"price": 49998, "qty": 0.25}],
            "asks": [],
            "checksum": 333333,
            "timestamp": "2026-04-22T00:00:02.000000Z",
        }]
    })
    for e in normalize_kraken_message(up2):
        book.apply_event(e)
    results.append(_check(book.last_sequence == 2 and book.bids[49998.0] == 0.25,
                          "second update: counter=2, new bid present"))

    # Unknown channel frames are ignored (no crash).
    noise = json.dumps({"channel": "heartbeat"})
    results.append(_check(normalize_kraken_message(noise) == [],
                          "non-book channel returns no events"))

    return all(results)


def main():
    ok = all([validate_coinbase(), validate_binance(), validate_kraken()])
    print("\n" + ("ALL ADAPTERS OK" if ok else "FAILURES DETECTED"))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
