class OrderBook:
    """Per-symbol L2 book with sequence-based gap detection.

    Gap fires when a batch starts strictly after last_sequence + 1.
    A batch straddling last_sequence (first_sequence <= last_sequence + 1 <= sequence)
    is treated as the first event after a REST snapshot and accepted.
    """

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
        first_sequence = event.get("first_sequence", sequence)

        if self.needs_resync:
            print(f"[RESYNC NEEDED] Skipping update for {self.symbol}")
            return

        if self.last_sequence is not None and sequence is not None:
            if sequence < self.last_sequence:
                self.old_event_count += 1
                print(f"[OLD EVENT] {self.symbol}: got {sequence}, last is {self.last_sequence}")
                return

            if sequence == self.last_sequence:
                # same-tick continuation, fall through and apply
                pass
            else:
                effective_first = first_sequence if first_sequence is not None else sequence
                if effective_first > self.last_sequence + 1:
                    self.gap_count += 1
                    self.needs_resync = True
                    print(
                        f"[GAP DETECTED] {self.symbol}: expected {self.last_sequence + 1}, "
                        f"got batch [{effective_first}..{sequence}]"
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

        if sequence is not None:
            if self.last_sequence is None or sequence > self.last_sequence:
                self.last_sequence = sequence

    def load_snapshot(self, events):
        self.bids = {}
        self.asks = {}

        for event in events:
            side = event["side"]
            price = float(event["price"])
            quantity = float(event["quantity"])

            if side == "bid":
                if quantity > 0:
                    self.bids[price] = quantity
            elif side == "ask":
                if quantity > 0:
                    self.asks[price] = quantity

            seq = event.get("sequence")
            if seq is not None:
                if self.last_sequence is None or seq > self.last_sequence:
                    self.last_sequence = seq

        self.needs_resync = False

    def reset_from_snapshot(self, snapshot):
        # Coinbase REST sequence is a seperate namespace from WS sequence_num,
        # so null it out and let the next live event re-anchor. Binance REST
        # lastUpdateId lives in the same space as WS diff u's, so keep it.
        self.bids = {price: qty for price, qty in snapshot["bids"] if qty > 0}
        self.asks = {price: qty for price, qty in snapshot["asks"] if qty > 0}
        exchange = snapshot.get("exchange", "coinbase")
        if exchange == "binance":
            self.last_sequence = snapshot.get("sequence")
        else:
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
            "duplicate_count": self.duplicate_count,
        }
