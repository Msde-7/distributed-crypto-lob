"""REST snapshot fetchers. each returns {exchange, symbol, bids, asks, sequence}.

sequence is per-exchange and not cross-comparable: coinbase = global Exchange
sequence (NOT the ws level2 sequence_num namespace), binance = lastUpdateId
(same namespace as ws diff u), kraken = None (CRC is the real integrity signal,
not implemented in time for the project report).
"""
import os

import requests


# Binance geofences api.binance.com from the US (HTTP 451). Default to the US. Less liquid of an exchange, so might have some issues
# host override for non-US deployments.
BINANCE_REST_HOST = os.environ.get("BINANCE_REST_HOST", "https://api.binance.us")


def load_coinbase_snapshot(symbol):
    url = f"https://api.exchange.coinbase.com/products/{symbol}/book?level=2"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    bids = [(float(price), float(qty)) for price, qty, *_ in data.get("bids", [])]
    asks = [(float(price), float(qty)) for price, qty, *_ in data.get("asks", [])]

    return {
        "exchange": "coinbase",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "sequence": data.get("sequence"),
    }


def _binance_rest_symbol(symbol):
    return symbol.replace("-", "").upper()


def load_binance_snapshot(symbol, limit=1000):
    rest_symbol = _binance_rest_symbol(symbol)
    url = f"{BINANCE_REST_HOST}/api/v3/depth?symbol={rest_symbol}&limit={limit}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    bids = [(float(price), float(qty)) for price, qty in data.get("bids", [])]
    asks = [(float(price), float(qty)) for price, qty in data.get("asks", [])]

    return {
        "exchange": "binance",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "sequence": data.get("lastUpdateId"),
    }


def _kraken_rest_pair(symbol):
    base, quote = symbol.split("-")
    if base == "BTC":
        base = "XBT"
    return f"{base}{quote}"


def load_kraken_snapshot(symbol, count=500):
    pair = _kraken_rest_pair(symbol)
    url = f"https://api.kraken.com/0/public/Depth?pair={pair}&count={count}"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    payload = response.json()
    if payload.get("error"):
        raise RuntimeError(f"Kraken REST error: {payload['error']}")

    # Kraken returns result keyed by its canonical pair name (XXBTZUSD).
    result = payload.get("result", {})
    if not result:
        raise RuntimeError("Kraken REST returned empty result")
    _canonical, book = next(iter(result.items()))

    bids = [(float(price), float(qty)) for price, qty, *_ in book.get("bids", [])]
    asks = [(float(price), float(qty)) for price, qty, *_ in book.get("asks", [])]

    return {
        "exchange": "kraken",
        "symbol": symbol,
        "bids": bids,
        "asks": asks,
        "sequence": None,
    }


def load_snapshot(exchange, symbol):
    if exchange == "coinbase":
        return load_coinbase_snapshot(symbol)
    if exchange == "binance":
        return load_binance_snapshot(symbol)
    if exchange == "kraken":
        return load_kraken_snapshot(symbol)
    raise ValueError(f"Unknown exchange: {exchange}")
