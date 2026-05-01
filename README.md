# distributed-crypto-lob

Crypto limit order book pipeline. Coinbase + Binance + Kraken websockets to
Kafka to Spark, books reconstructed accross a unified schema.

## Run

Offline check (cross-platform): `python validate_adapters.py`.

Local end-to-end run is **Windows-only** right now (`run_experiment.py`
hardcodes `C:\tools\...` paths and uses `taskkill`):

```bash
./start-kafka.sh
EXCHANGES=coinbase,binance,kraken python run_experiment.py 300
```

On Linux/Mac use the jetstream scripts below instead.

## On Jetstream2

`jetstream/` has the bootstrap, smoke, sweep, and fault-injection scripts.
SSH passphrase goes in `.env` (gitignored).

Live-traffic smoke (90s default, override with `DURATION=`):
```bash
bash jetstream/06_smoke.sh
DURATION=1800 bash jetstream/06_smoke.sh
```

Live-traffic scaling sweep (N=2,4,6,8 workers): `bash jetstream/10_scaling_sweep.sh`.

Worker-kill fault injection: `bash jetstream/11_fault_injection.sh`.

## Multi-broker Kafka

Run `jetstream/12_kafka_multi.sh` on each kafka VM with `NODE_ID` set to its
position in the quorum. Defaults to a 3-broker layout, configurable via the
`KAFKA_NODES` env (`"1@ip,2@ip,3@ip"`). Default broker-level RF and ISR are
derived from the cluster size.

```bash
# on kafka01
NODE_ID=1 bash jetstream/12_kafka_multi.sh
# on kafka02
NODE_ID=2 bash jetstream/12_kafka_multi.sh
# on kafka03
NODE_ID=3 bash jetstream/12_kafka_multi.sh
```

When running smoke tests against a multi-broker cluster, set the topic RF:
```bash
REPLICATION_FACTOR=3 bash jetstream/06_smoke.sh
```

## Synthetic load

The live exchange feeds top out around 30k events/sec across all 12 books,
which is enough work for ~4 worker cores. To exercise the cluster past that
ceiling, `producer_synth.py` generates schema-correct events at a configurable
rate using per-book sequence counters that never gap.

Single-rate smoke:
```bash
SYNTH_RATE=50000 SYNTH_PROCS=4 DURATION=120 bash jetstream/13_synth_smoke.sh
```

Full 2D sweep across rates × worker counts:
```bash
bash jetstream/14_synth_sweep.sh
# results in logs/synth_sweep/, aggregated CSV at logs/synth_sweep/sweep_grid.csv
```

Override the grid with `RATES`, `WORKER_COUNTS`, `DURATION`, `SYNTH_PROCS`:
```bash
RATES="20000 50000 100000" WORKER_COUNTS="4 8" \
    bash jetstream/14_synth_sweep.sh
```

## Security

Public feeds, no creds needed. Auth keys go in `.env` if you add a private feed.
