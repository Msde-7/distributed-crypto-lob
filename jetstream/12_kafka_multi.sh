#!/bin/bash
# multi-broker KRaft kafka. run on each kafka node.
# expects NODE_ID env var (1 = kafka01, 2 = kafka02, 3 = kafka03).
# picks up its own internal IP for advertised listeners.
#
# the controller quorum is built from KAFKA_NODES, a comma-separated list of
# node_id@ip pairs. defaults to a 3-broker setup if not provided. you can pass
# fewer nodes if you only have 2 brokers up.
#
# wipes existing /data/kafka (so cluster can re-form with new cluster id).
# topic replication factor is set separately via kafka-topics --create, but
# the broker-level defaults (KAFKA_DEFAULT_REPLICATION_FACTOR etc.) are derived
# from the number of nodes in the quorum.

set -euo pipefail

NODE_ID="${NODE_ID:?must set NODE_ID=1, 2, or 3}"
CLUSTER_ID="${CLUSTER_ID:-MkU3OEVBNTcwNTJENDM2Qk}"
INTERNAL_IP=$(hostname -I | awk '{print $1}')

# default to 3-broker layout. override via KAFKA_NODES env if needed.
# format: "id@ip,id@ip,id@ip"
KAFKA_NODES="${KAFKA_NODES:-1@10.4.36.193,2@10.4.36.77,3@10.4.36.50}"

# build controller quorum string: "1@ip:9093,2@ip:9093,3@ip:9093"
QUORUM=""
NUM_NODES=0
IFS=',' read -ra NODES <<< "$KAFKA_NODES"
for n in "${NODES[@]}"; do
    if [ -z "$QUORUM" ]; then
        QUORUM="${n}:9093"
    else
        QUORUM="${QUORUM},${n}:9093"
    fi
    NUM_NODES=$((NUM_NODES + 1))
done

# pick replication / ISR sizes based on cluster size
# RF can't exceed broker count. ISR is RF-1 to tolerate one failure.
if [ "$NUM_NODES" -ge 3 ]; then
    RF=3; MIN_ISR=2
elif [ "$NUM_NODES" -eq 2 ]; then
    RF=2; MIN_ISR=1
else
    RF=1; MIN_ISR=1
fi

echo "==> NODE_ID=${NODE_ID} INTERNAL_IP=${INTERNAL_IP}"
echo "==> cluster size: ${NUM_NODES} broker(s), default RF=${RF}, min_isr=${MIN_ISR}"
echo "==> controller quorum: ${QUORUM}"

# point /data at the attached volume if there is one
if [ -d /media/volume ]; then
    VOL=$(ls /media/volume 2>/dev/null | head -1 || true)
    if [ -n "$VOL" ]; then
        sudo ln -sfn "/media/volume/$VOL" /data
    fi
fi
[ -L /data ] || sudo mkdir -p /data

sudo mkdir -p /data/kafka
# wipe so the new cluster id can format cleanly
docker rm -f kafka 2>/dev/null || true
sudo rm -rf /data/kafka/*
sudo chown -R 1000:1000 /data/kafka

# ufw: allow kafka client + controller ports from project subnet (idempotent)
sudo ufw allow from 10.4.36.0/24 to any port 9092 proto tcp >/dev/null
sudo ufw allow from 10.4.36.0/24 to any port 9093 proto tcp >/dev/null

docker run -d --name kafka --restart unless-stopped \
    --network host \
    -e KAFKA_NODE_ID=${NODE_ID} \
    -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://${INTERNAL_IP}:9092 \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e KAFKA_CONTROLLER_QUORUM_VOTERS="${QUORUM}" \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
    -e KAFKA_NUM_PARTITIONS=8 \
    -e KAFKA_DEFAULT_REPLICATION_FACTOR=${RF} \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=${RF} \
    -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=${RF} \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=${MIN_ISR} \
    -e KAFKA_MIN_INSYNC_REPLICAS=${MIN_ISR} \
    -e KAFKA_LOG_RETENTION_HOURS=1 \
    -e CLUSTER_ID=${CLUSTER_ID} \
    -v /data/kafka:/var/lib/kafka/data \
    apache/kafka:3.9.0

echo "==> waiting for kafka :9092 to open (up to 90s)"
for i in $(seq 1 45); do
    if (echo > /dev/tcp/127.0.0.1/9092) 2>/dev/null; then
        echo "==> kafka up after ${i}x2s"
        docker logs --tail 4 kafka 2>&1 | tail -4
        exit 0
    fi
    sleep 2
done
echo "==> FAIL: :9092 didnt open in 90s"
docker logs --tail 80 kafka
exit 1
