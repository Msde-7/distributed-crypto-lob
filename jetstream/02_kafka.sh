#!/bin/bash
# Run on kafka01.
# Single-broker KRaft Kafka using the official apache/kafka image.
# - 1-hour log retention so the 50 GB disk never fills during experiments
# - 8 default partitions matches our scaling sweep target
# - data persisted on the attached 50 GB volume

set -euo pipefail

VOL_MOUNT=/media/volume/CourseProject-kafka01-data
INTERNAL_IP=$(hostname -I | awk '{print $1}')
CLUSTER_ID="${CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"  # any 22-char base64 string works for a single-broker dev cluster

if [ ! -L /data ]; then
    sudo ln -sfn "$VOL_MOUNT" /data
fi
sudo mkdir -p /data/kafka
# apache/kafka container runs as uid 1000 (appuser)
sudo chown -R 1000:1000 /data/kafka

echo "==> internal IP for advertised listener: $INTERNAL_IP"
echo "==> /data: $(readlink -f /data)"
df -h /data | tail -1

docker rm -f kafka 2>/dev/null || true

docker run -d --name kafka --restart unless-stopped \
    -p 9092:9092 \
    -e KAFKA_NODE_ID=1 \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${INTERNAL_IP}:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_NUM_PARTITIONS=8 \
    -e KAFKA_LOG_RETENTION_HOURS=1 \
    -e CLUSTER_ID=${CLUSTER_ID} \
    -v /data/kafka:/var/lib/kafka/data \
    apache/kafka:3.9.0

echo "==> waiting for kafka to be ready on :9092"
for i in $(seq 1 45); do
    if (echo > /dev/tcp/127.0.0.1/9092) >/dev/null 2>&1; then
        echo "==> kafka up after ${i}x2s"
        docker logs --tail 5 kafka 2>&1 | tail -5
        exit 0
    fi
    sleep 2
done
echo "==> FAIL: kafka did not open :9092 in 90s"
docker logs --tail 60 kafka
exit 1
