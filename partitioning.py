"""Deterministic round-robin Kafka partitioner across exchange:symbol pairs.

Default murmur2 on the key field distributes unevenly for small key sets
(12 keys in 8 partitions routinely leaves 2-4 empty), which flattens the
scaling sweep. This module spreads keys across partitions by assigning
partition = index_in_sorted(ALL_BOOK_KEYS) % NUM_PARTITIONS.

Usage in a producer:

    from partitioning import partition_for

    producer.send(
        topic,
        key=event["symbol"],
        value=event,
        partition=partition_for(event["exchange"], event["symbol"]),
    )

If ALL_BOOK_KEYS is not set, partition_for returns None and Kafka falls back
to its default murmur2 partitioner.
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
    one this process runs, so the partition assignment is consistent across
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
        return {}, num_partitions
    return {k: i % num_partitions for i, k in enumerate(keys)}, num_partitions


_MAP, _NUM_PARTITIONS = _build_map()


def partition_for(exchange, symbol):
    """Return a fixed partition for (exchange, symbol), or None for default."""
    key = f"{exchange}:{symbol}"
    return _MAP.get(key)


def describe():
    return dict(_MAP), _NUM_PARTITIONS
