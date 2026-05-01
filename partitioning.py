"""Deterministic round-robin partitioner for exchange:symbol keys.

murmur2 on 12 keys across 8 partitions leaves 2-4 partitions empty, which
flattens the scaling sweep. we use partition = sorted_index % num_partitions
instead. partition_for returns None if ALL_BOOK_KEYS isnt set, which falls
through to the default murmur2.
"""
import os


def _parse_keys(raw):
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


def _keys_from_product_envs():
    """Fallback derivation when ALL_BOOK_KEYS is not set explicitly.

    Useful under docker-compose where each producer container has only its
    own exchange's product list in env. We include all three, not just the
    one this process runs, so the partition assignment is consistant across
    every producer process.
    """
    exchanges = {
        "coinbase": os.environ.get("COINBASE_PRODUCTS", ""),
        "binance": os.environ.get("BINANCE_PRODUCTS", ""),
        "kraken": os.environ.get("KRAKEN_PRODUCTS", ""),
    }
    keys = []
    for ex, raw in exchanges.items():
        for sym in raw.split(","):
            sym = sym.strip().upper()
            if sym:
                keys.append(f"{ex}:{sym}")
    return keys


def _build_map():
    num_partitions = int(os.environ.get("KAFKA_PARTITIONS", "8"))
    raw_keys = os.environ.get("ALL_BOOK_KEYS", "")
    keys = _parse_keys(raw_keys)
    if not keys:
        keys = _keys_from_product_envs()
    keys = sorted(keys)
    if not keys:
        return {}
    return {k: i % num_partitions for i, k in enumerate(keys)}


_MAP = _build_map()


def partition_for(exchange, symbol):
    """Return a fixed partition for (exchange, symbol), or None for default."""
    key = f"{exchange}:{symbol}"
    return _MAP.get(key)
