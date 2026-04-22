from kafka import KafkaProducer
import websocket
import json

from normalizer import normalize_coinbase_message

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def on_message(ws, message):
    try:
        events = normalize_coinbase_message(message)

        for event in events:
            producer.send(
                "lob-events",
                key=event["symbol"],
                value=event
            )

        if events:
            producer.flush()
            print(f"Sent {len(events)} normalized event(s)")

    except Exception as e:
        print("Producer error:", e)

def on_open(ws):
    print("Connected to Coinbase")

    subscribe_message = {
        "type": "subscribe",
        "product_ids": ["BTC-USD"],
        "channel": "level2"
    }

    ws.send(json.dumps(subscribe_message))

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("Connection closed")

socket = "wss://advanced-trade-ws.coinbase.com"

ws = websocket.WebSocketApp(
    socket,
    on_message=on_message,
    on_open=on_open,
    on_error=on_error,
    on_close=on_close
)

ws.run_forever()