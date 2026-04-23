"""Per-exchange frame normalizers.

All normalizers return dicts with:
    exchange, symbol, side, price, quantity, event_time,
    sequence, first_sequence, is_snapshot, checksum

first_sequence lets one gap rule cover Coinbase single-seq ticks and
Binance [U..u] windows.
"""
import json


def normalize_coinbase_message(message):
    data = json.loads(message) if isinstance(message, str) else message
    if data.get("channel") != "l2_data":
        return []

    sequence = data.get("sequence_num")
    out = []
    for evt in data.get("events", []):
        is_snapshot = evt.get("type") == "snapshot"
        product_id = evt.get("product_id")
        for upd in evt.get("updates", []):
            raw_side = upd.get("side", "").lower()
            side = "bid" if raw_side == "bid" else "ask"
            out.append({
                "exchange": "coinbase",
                "symbol": product_id,
                "side": side,
                "price": float(upd["price_level"]),
                "quantity": float(upd["new_quantity"]),
                "event_time": upd.get("event_time"),
                "sequence": sequence,
                # first_sequence=None disables gap detection for Coinbase. The
                # sequence_num is subscription-wide, not per-channel, so it
                # skips non-l2_data frames and looks gappy even when nothing
                # is actually missing. We trust WS ordering + reconnect here.
                "first_sequence": None,
                "is_snapshot": is_snapshot,
                "checksum": None,
            })
    return out


def _binance_symbol(stream_symbol, symbol_map):
    if not stream_symbol:
        return None
    key = stream_symbol.upper()
    if symbol_map and key in symbol_map:
        return symbol_map[key]
    # fallback split by common quote currencys
    for quote in ("USDT", "BUSD", "USDC", "USD", "BTC", "ETH", "EUR"):
        if key.endswith(quote) and len(key) > len(quote):
            return f"{key[:-len(quote)]}-{quote}"
    return key


def normalize_binance_message(message, symbol_map=None):
    data = json.loads(message) if isinstance(message, str) else message
    if isinstance(data, dict) and "data" in data and "stream" in data:
        data = data["data"]
    if not isinstance(data, dict) or data.get("e") != "depthUpdate":
        return []

    symbol = _binance_symbol(data.get("s"), symbol_map)
    first_id = data.get("U")
    last_id = data.get("u")
    event_time = data.get("E")

    out = []
    for price, qty in data.get("b", []):
        out.append({
            "exchange": "binance",
            "symbol": symbol,
            "side": "bid",
            "price": float(price),
            "quantity": float(qty),
            "event_time": event_time,
            "sequence": last_id,
            "first_sequence": first_id,
            "is_snapshot": False,
            "checksum": None,
        })
    for price, qty in data.get("a", []):
        out.append({
            "exchange": "binance",
            "symbol": symbol,
            "side": "ask",
            "price": float(price),
            "quantity": float(qty),
            "event_time": event_time,
            "sequence": last_id,
            "first_sequence": first_id,
            "is_snapshot": False,
            "checksum": None,
        })
    return out


# Kraken v2 has no sequence numbers, so keep a local counter per symbol.
_kraken_counters = {}


def _kraken_symbol(ws_symbol):
    if not ws_symbol:
        return None
    return ws_symbol.replace("/", "-").upper()


def normalize_kraken_message(message):
    data = json.loads(message) if isinstance(message, str) else message
    if not isinstance(data, dict) or data.get("channel") != "book":
        return []
    frame_type = data.get("type")
    if frame_type not in ("snapshot", "update"):
        return []

    out = []
    for entry in data.get("data", []):
        symbol = _kraken_symbol(entry.get("symbol"))
        timestamp = entry.get("timestamp")
        checksum = entry.get("checksum")
        is_snapshot = frame_type == "snapshot"

        groups = [("bid", entry.get("bids", [])), ("ask", entry.get("asks", []))]
        flat = [(side, row) for side, rows in groups for row in rows]
        if not flat:
            continue

        # bump counter only when we actually emit events, otherwise an empty
        # Kraken heartbeat-style frame would leave a phantom sequence hole
        counter = _kraken_counters.get(symbol, -1) + 1
        _kraken_counters[symbol] = counter

        for idx, (side, row) in enumerate(flat):
            price = float(row["price"])
            qty = float(row["qty"])
            is_last = idx == len(flat) - 1
            out.append({
                "exchange": "kraken",
                "symbol": symbol,
                "side": side,
                "price": price,
                "quantity": qty,
                "event_time": timestamp,
                "sequence": counter,
                # first_sequence=None: Kraken has no native sequence, our local
                # counter is not a reliable gap signal, so trust ordering and
                # rely on CRC32 (future work) for integrity checking
                "first_sequence": None,
                "is_snapshot": is_snapshot,
                "checksum": checksum if is_last else None,
            })
    return out


def reset_kraken_state(symbol=None):
    if symbol is None:
        _kraken_counters.clear()
    else:
        _kraken_counters.pop(symbol, None)
