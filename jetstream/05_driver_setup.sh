#!/bin/bash
# Run on driver01.
# Repo files are SFTP'd in by jetstream/scp_repo.py. This script just installs
# Python deps, validates the adapters, and creates the Kafka topic.

set -euo pipefail

KAFKA_IP=10.4.36.193
SPARK_MASTER=spark://10.4.36.243:7077

cd ~/distributed-crypto-lob

if ! command -v pip3 >/dev/null; then
    sudo DEBIAN_FRONTEND=noninteractive apt-get -y -qq install python3-pip
fi
# kafka-python 2.0.2 is broken on python 3.12 (Ubuntu 24.04 default); use the
# kafka-python-ng fork. uninstall first to clear any pre-existing 2.0.2.
pip3 uninstall --break-system-packages -y kafka-python kafka-python-ng >/dev/null 2>&1 || true
pip3 install --break-system-packages --quiet \
    'kafka-python-ng==2.2.3' websocket-client==1.8.0 requests==2.32.3

echo "==> verifying adapters offline"
python3 validate_adapters.py | tail -5

echo "==> creating topic with 8 partitions on ${KAFKA_IP}:9092"
docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --create --if-not-exists --topic lob-events --partitions 8 --replication-factor 1 \
    2>&1 | tail -3

docker run --rm --network host apache/kafka:3.9.0 \
    /opt/kafka/bin/kafka-topics.sh --bootstrap-server ${KAFKA_IP}:9092 \
    --describe --topic lob-events 2>&1 | head -2

echo "==> spark cluster status"
docker run --rm --network host apache/spark:3.5.3-python3 bash -c "
echo 'master: ${SPARK_MASTER}'
"

echo "==> done. environment ready on $(hostname)"
