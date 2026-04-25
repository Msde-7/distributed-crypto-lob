#!/bin/bash
# spawns a spark standalone worker on this box. host networking so the worker
# registers its real ip not the docker bridge one. /data is the NFS mount and
# has to be visible inside the container, otherwise parquet writes fail (this
# occured to me a few times before adding the bind mount).

set -euo pipefail

MASTER_URL="${MASTER_URL:-spark://10.4.36.243:7077}"
INTERNAL_IP=$(hostname -I | awk '{print $1}')
WORKER_CORES="${WORKER_CORES:-2}"
WORKER_MEM="${WORKER_MEM:-4G}"

echo "==> joining master at ${MASTER_URL} as $(hostname) (${INTERNAL_IP})"

docker rm -f spark-worker 2>/dev/null || true

docker run -d --name spark-worker --restart unless-stopped \
    --network host \
    -v /data:/data \
    -e SPARK_NO_DAEMONIZE=1 \
    apache/spark:3.5.3-python3 \
    /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker \
        --host ${INTERNAL_IP} \
        --webui-port 8081 \
        --cores ${WORKER_CORES} \
        --memory ${WORKER_MEM} \
        ${MASTER_URL}

echo "==> waiting for worker to register (60s)"
for i in $(seq 1 30); do
    if docker logs spark-worker 2>&1 | grep -q "Successfully registered with master"; then
        echo "==> worker registered with master"
        docker logs --tail 5 spark-worker 2>&1 | tail -5
        exit 0
    fi
    sleep 2
done
echo "==> FAIL: worker did not register within 60s"
docker logs --tail 60 spark-worker
exit 1
