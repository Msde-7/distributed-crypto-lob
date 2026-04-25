#!/bin/bash
# Run on driver01.
# Clone the repo, install Python deps for the producers, run a 60s smoke test.

set -euo pipefail

REPO=https://github.com/Msde-7/distributed-crypto-lob.git
KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077

if [ ! -d ~/distributed-crypto-lob ]; then
    git clone "$REPO" ~/distributed-crypto-lob
fi
cd ~/distributed-crypto-lob
git pull --ff-only || true

if ! command -v pip3 >/dev/null; then
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install python3-pip
fi
pip3 install --break-system-packages --quiet \
    kafka-python==2.0.2 websocket-client==1.8.0 requests==2.32.3

echo "==> verifying adapter tests pass"
python3 validate_adapters.py | tail -5

echo "==> creating topic with 8 partitions"
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --create --if-not-exists --topic lob-events --partitions 8 --replication-factor 1 \
    2>&1 | tail -3

docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --describe --topic lob-events 2>&1 | head -2

echo "==> environment ready. spark master: ${SPARK_MASTER}"
echo "==> launch a smoke run separately with: python3 run_experiment.py 60"
