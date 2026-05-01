"""one-shot demo: latest parquet snapshot vs live REST.

run on driver01:
    cd ~/distributed-crypto-lob
    python3 jetstream/demo.py

prints a side-by-side comparison of what our pipeline reconstructed (from
the parquet sink) and what the exchanges show right now (REST). drift should
be small and proportional (only a couple secobds in between max), in the direction of recent market motion.
"""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from resync import load_coinbase_snapshot, load_binance_snapshot, load_kraken_snapshot

SINK = "/data/lob_snapshots"


def latest_parquet_batch():
    import pyarrow.dataset as ds
    d = ds.dataset(SINK, format="parquet")
    df = d.to_table().to_pandas()
    if df.empty:
        return df
    last = df["batch_id"].max()
    return df[df["batch_id"] == last].sort_values(["exchange", "symbol"])


REST_FN = {
    "coinbase": load_coinbase_snapshot,
    "binance":  load_binance_snapshot,
    "kraken":   load_kraken_snapshot,
}

# all 12 books we run in the pipeline
ALL_BOOKS = [
    ("coinbase", "BTC-USD"),  ("coinbase", "ETH-USD"),
    ("coinbase", "SOL-USD"),  ("coinbase", "LTC-USD"),
    ("binance",  "BTC-USDT"), ("binance",  "ETH-USDT"),
    ("binance",  "SOL-USDT"), ("binance",  "LTC-USDT"),
    ("kraken",   "BTC-USD"),  ("kraken",   "ETH-USD"),
    ("kraken",   "SOL-USD"),  ("kraken",   "LTC-USD"),
]


def rest_top_of_book():
    rows = []
    for ex, sym in ALL_BOOKS:
        try:
            s = REST_FN[ex](sym)
            bb = max(p for p, _ in s["bids"])
            ba = min(p for p, _ in s["asks"])
            rows.append((ex, sym, bb, ba))
        except Exception as e:
            rows.append((ex, sym, None, None))
            print(f"  REST failed for {ex}:{sym}: {e}", file=sys.stderr)
    return rows


def main():
    print("=" * 72)
    print(" STREAMED  (latest parquet snapshot from /data/lob_snapshots)")
    print("=" * 72)
    pq = latest_parquet_batch()
    if pq.empty:
        print("  (no parquet output found - is the pipeline running?)")
    else:
        for _, r in pq.iterrows():
            print(f"  {r['exchange']:<9} {r['symbol']:<10} "
                  f"bid={r['best_bid']:>12.4f}  ask={r['best_ask']:>12.4f}  "
                  f"spread={r['spread']:>7.4f}")

    print()
    print("=" * 72)
    print(" LIVE REST  (right now, fresh from the exchange APIs)")
    print("=" * 72)
    rest = rest_top_of_book()
    for ex, sym, bb, ba in rest:
        if bb is None:
            print(f"  {ex:<9} {sym:<10}  (REST fetch failed)")
        else:
            print(f"  {ex:<9} {sym:<10} bid={bb:>12.4f}  ask={ba:>12.4f}  "
                  f"spread={(ba-bb):>7.4f}")

    if not pq.empty:
        print()
        print("=" * 72)
        print(" DRIFT  (streamed - rest, in dollars)")
        print("=" * 72)
        rest_d = {(ex, sym): (bb, ba) for ex, sym, bb, ba in rest if bb is not None}
        for _, r in pq.iterrows():
            key = (r["exchange"], r["symbol"])
            if key not in rest_d:
                continue
            rb, ra = rest_d[key]
            print(f"  {r['exchange']:<9} {r['symbol']:<10} "
                  f"bid drift={r['best_bid']-rb:+8.2f}   "
                  f"ask drift={r['best_ask']-ra:+8.2f}")


if __name__ == "__main__":
    main()
