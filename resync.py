"""REST snapshot fetchers per exchange.

Each returns: {exchange, symbol, bids, asks, sequence}.
sequence meaning:
  coinbase: global Exchange sequence, NOT comparable to WS level2 sequence_num
  binance : lastUpdateId, same namespace as WS diff u
  kraken  : None (no sequence in REST v2 Depth; CRC is the integrity signal)
"""
import os

import requests


# Binance geofences api.binance.com from the US (HTTP 451). Default to the US
# host; override for non-US deployments.
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


_KRAKEN_REST_MAP = {
    "BTC-USD": "XBTUSD",
    "BTC-USDT": "XBTUSDT",
    "ETH-USD": "ETHUSD",
    "ETH-USDT": "ETHUSDT",
    "SOL-USD": "SOLUSD",
}


def _kraken_rest_pair(symbol):
    if symbol in _KRAKEN_REST_MAP:
        return _KRAKEN_REST_MAP[symbol]
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

    # Kraken returns result keyed by its canonical pair name (e.g. XXBTZUSD).
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
