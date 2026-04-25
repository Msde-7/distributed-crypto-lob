# distributed-crypto-lob

Crypto limit order book pipeline. Coinbase + Binance + Kraken websockets to
Kafka to Spark, books reconstructed accross a unified schema.

## Run

```bash
./start-kafka.sh
EXCHANGES=coinbase,binance,kraken python run_experiment.py 300
```

Offline check first if you want: `python validate_adapters.py`.

## On Jetstream2

`jetstream/` has the bootstrap, smoke, sweep, and fault-injection scripts.
SSH passphrase goes in `.env` (gitignored).

## Security

Public feeds, no creds needed. Auth keys go in `.env` if you add a private feed.
