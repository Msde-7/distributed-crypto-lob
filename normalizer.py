import json

def normalize_coinbase_message(message: str):
    """Normalize a Coinbase Advanced Trade 'level2' WebSocket frame.

    Frame shape:
        { "channel": "l2_data", "timestamp": "...", "sequence_num": N,
          "events": [ { "type": "snapshot" | "update",
                        "product_id": "BTC-USD",
                        "updates": [ { "side": "bid" | "offer",
                                       "event_time": "...",
                                       "price_level": "...",
                                       "new_quantity": "..." } ] } ] }
    """
    data = json.loads(message)

    if data.get("channel") != "l2_data":
        return []

    sequence = data.get("sequence_num")
    normalized_events = []

    for evt in data.get("events", []):
        evt_type = evt.get("type")
        is_snapshot = evt_type == "snapshot"
        product_id = evt.get("product_id")

        for upd in evt.get("updates", []):
            raw_side = upd.get("side", "").lower()
            side = "bid" if raw_side == "bid" else "ask"

            normalized_events.append({
                "exchange": "coinbase",
                "symbol": product_id,
                "side": side,
                "price": float(upd["price_level"]),
                "quantity": float(upd["new_quantity"]),
                "event_time": upd.get("event_time"),
                "sequence": sequence,
                "is_snapshot": is_snapshot,
            })

    return normalized_events
