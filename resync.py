import requests

def load_coinbase_snapshot(symbol):
    """
    Fetch a fresh order book snapshot from Coinbase REST API.
    Returns a dictionary with bids, asks, and sequence.
    """
    url = f"https://api.exchange.coinbase.com/products/{symbol}/book?level=2"

    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()

    bids = [(float(price), float(qty)) for price, qty, *_ in data.get("bids", [])]
    asks = [(float(price), float(qty)) for price, qty, *_ in data.get("asks", [])]

    return {
        "bids": bids,
        "asks": asks,
        "sequence": data.get("sequence")
    }