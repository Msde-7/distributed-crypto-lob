#!/bin/bash
# spawns a spark standalone worker on this box. host networking so the worker
# registers its real ip not the docker bridge one. /data is the NFS mount and
# has to be visible inside the container, otherwise parquet writes fail.
#
# NFS gotcha: docker bind mounts default to rprivate, which captures the host
# directory state at container-start time. if NFS isnt mounted yet when the
# container starts (e.g. fresh VM unshelve), the container sees an empty /data
# and stays stuck on it even after NFS comes up on the host. fix: bind with
# rslave propagation so the container inherits host-side mount events. as a
# belt-and-suspenders, we also wait for the host NFS mount to be ready before
# starting the container.

set -euo pipefail

MASTER_URL="${MASTER_URL:-spark://10.4.36.243:7077}"
INTERNAL_IP=$(hostname -I | awk '{print $1}')
WORKER_CORES="${WORKER_CORES:-2}"
WORKER_MEM="${WORKER_MEM:-4G}"

echo "==> joining master at ${MASTER_URL} as $(hostname) (${INTERNAL_IP})"

# wait up to 30s for NFS /data mount to come up before starting the worker.
# if /data isnt mounted yet, the container would bind to the empty local dir
# and writes would silently fail.
for i in $(seq 1 15); do
    if mount | grep -q ":/data on /data"; then
        echo "==> NFS mount on /data: ready"
        break
    fi
    if [ $i -eq 15 ]; then
        echo "==> WARN: NFS mount on /data not detected after 30s, starting anyway"
        echo "    (parquet writes may fail; re-run this script after fixing NFS)"
    fi
    sleep 2
done

docker rm -f spark-worker 2>/dev/null || true

# rslave bind propagation: container inherits host-side mount changes.
# with the default rprivate, an NFS mount that comes up after the container
# starts is invisible inside the container.
docker run -d --name spark-worker --restart unless-stopped \
    --network host \
    --mount type=bind,source=/data,target=/data,bind-propagation=rslave \
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
