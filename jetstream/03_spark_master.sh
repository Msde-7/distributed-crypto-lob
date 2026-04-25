#!/bin/bash
# Run on driver01.
# Spark Standalone master, listening on the host's internal IP.
# - --network host so workers / submitters reach 7077 directly without docker NAT
# - SPARK_NO_DAEMONIZE=1 keeps it in the foreground for the container

set -euo pipefail

VOL_MOUNT=/media/volume/CourseProject-driver01-data
INTERNAL_IP=$(hostname -I | awk '{print $1}')

if [ ! -L /data ]; then
    sudo ln -sfn "$VOL_MOUNT" /data
fi
sudo mkdir -p /data/lob_snapshots /data/spark_checkpoint
sudo chown -R exouser:exouser /data

echo "==> driver01 internal IP: $INTERNAL_IP"
echo "==> /data mounted at: $(readlink -f /data)"
df -h /data | tail -1

docker rm -f spark-master 2>/dev/null || true

docker run -d --name spark-master --restart unless-stopped \
    --network host \
    -e SPARK_NO_DAEMONIZE=1 \
    apache/spark:3.5.3-python3 \
    /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master \
        --host ${INTERNAL_IP} --port 7077 --webui-port 8080

echo "==> waiting for spark master on :7077"
for i in $(seq 1 30); do
    if (echo > /dev/tcp/127.0.0.1/7077) >/dev/null 2>&1; then
        echo "==> spark master up after ${i}x2s"
        echo "    web UI: http://${INTERNAL_IP}:8080  (only reachable inside the project network)"
        exit 0
    fi
    sleep 2
done
echo "==> FAIL: spark master did not open :7077 in 60s"
docker logs --tail 60 spark-master
exit 1
