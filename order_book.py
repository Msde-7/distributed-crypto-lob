class OrderBook:
    def __init__(self, symbol):
        self.symbol = symbol
        self.bids = {}
        self.asks = {}
        self.last_sequence = None
        self.needs_resync = False
        self.gap_count = 0
        self.old_event_count = 0
        self.duplicate_count = 0

    def apply_event(self, event):
        sequence = event.get("sequence")

        # If book is invalid, do not apply more updates
        if self.needs_resync:
            print(f"[RESYNC NEEDED] Skipping update for {self.symbol}")
            return

        # Sequence checks
        if self.last_sequence is not None and sequence is not None:
            if sequence < self.last_sequence:
                self.old_event_count += 1
                print(f"[OLD EVENT] {self.symbol}: got {sequence}, last is {self.last_sequence}")
                return

            if sequence > self.last_sequence + 1:
                self.gap_count += 1
                self.needs_resync = True
                print(
                    f"[GAP DETECTED] {self.symbol}: expected {self.last_sequence + 1}, got {sequence}"
                )
                return

        side = event["side"]
        price = float(event["price"])
        quantity = float(event["quantity"])

        if side == "bid":
            if quantity == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = quantity

        elif side == "ask":
            if quantity == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = quantity

        self.last_sequence = sequence

    def load_snapshot(self, events):
        self.bids = {}
        self.asks = {}

        for event in events:
            side = event["side"]
            price = float(event["price"])
            quantity = float(event["quantity"])

            if side == "bid":
                self.bids[price] = quantity
            elif side == "ask":
                self.asks[price] = quantity

            if event.get("sequence") is not None:
                self.last_sequence = event["sequence"]

        self.needs_resync = False

    def reset_from_snapshot(self, snapshot):
        self.bids = {price: qty for price, qty in snapshot["bids"]}
        self.asks = {price: qty for price, qty in snapshot["asks"]}
        # REST snapshot sequence is Exchange's global (huge) namespace,
        # not the WS level2 sequence_num. Reset to None so the next WS
        # event re-establishes the baseline.
        self.last_sequence = None
        self.needs_resync = False

    def best_bid(self):
        return max(self.bids.keys()) if self.bids else None

    def best_ask(self):
        return min(self.asks.keys()) if self.asks else None

    def spread(self):
        bid = self.best_bid()
        ask = self.best_ask()
        if bid is not None and ask is not None:
            return ask - bid
        return None

    def snapshot(self, depth=5):
        top_bids = sorted(self.bids.items(), key=lambda x: x[0], reverse=True)[:depth]
        top_asks = sorted(self.asks.items(), key=lambda x: x[0])[:depth]

        return {
            "symbol": self.symbol,
            "best_bid": self.best_bid(),
            "best_ask": self.best_ask(),
            "spread": self.spread(),
            "top_bids": top_bids,
            "top_asks": top_asks,
            "last_sequence": self.last_sequence,
            "needs_resync": self.needs_resync,
            "gap_count": self.gap_count,
            "old_event_count": self.old_event_count,
            "duplicate_count": self.duplicate_count
        }